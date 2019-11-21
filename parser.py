import requests
import copy
from time import sleep


def load_data(data_folder):
    url = 'http://mychem.info/v1/query?q=*:*&fetch_all=True&dotfield=True&fields=chembl.inchi,chembl.inchi_key,aeolus.drug_rxcui,chembl.smiles,pubchem.cid,drugcentral.xrefs.chebi,chembl.molecule_chembl_id,drugbank.id,chembl.pref_name,unii.unii,drugcentral.xrefs.umlscui,ginas.xrefs.MESH'
    fields = ['inchi', 'inchikey', 'chembl', 'pubchem', 'rxcui', 'smiles', 'mesh', 'umls', 'chebi', 'drugbank', 'name', 'unii']
    cnt = 0
    total = 1
    while cnt < total:
        doc = requests.get(url).json()
        if 'total' not in doc:
            i = 0
            while i < 3:
                doc = requests.get(url).json()
                if 'total' in doc:
                    break
                i += 1
                sleep(10)
        total = doc['total']
        print('total', total)
        cnt += len(doc['hits'])
        print('current cnt', cnt)
        url = 'http://mychem.info/v1/query?scroll_id=' + doc['_scroll_id']
        for _doc in doc['hits']:
            _doc = restructure_output(_doc)
            primary_id = get_primary_id(_doc)
            if primary_id:
                keys = copy.copy(list(_doc.keys()))
                for k in keys:
                    if k not in fields:
                        _doc.pop(k)
                _doc['_id'] = primary_id
                _doc['type'] = 'ChemicalSubstance'
                yield _doc


def restructure_output(_doc):
    """Restructure the API output"""
    field_mapping = {'chembl.inchi': 'inchi',
                     'chembl.inchi_key': 'inchikey',
                     'aeolus.drug_rxcui': 'rxcui',
                     'chembl.smiles': 'smiles',
                     'pubchem.cid': 'pubchem',
                     'drugcentral.xrefs.chebi': 'chebi',
                     'chembl.molecule_chembl_id': 'chembl',
                     'drugbank.id': 'drugbank',
                     'chembl.pref_name': 'name',
                     'unii.unii': 'unii',
                     'drugcentral.xrefs.umlscui': 'umls',
                     'ginas.xrefs.MESH': 'mesh'}
    # loop through mapping, change the field name one by one
    for k, v in field_mapping.items():
        if _doc.get(k):
            _doc[v] = _doc.pop(k)
    return _doc

def get_primary_id(_doc):
    """get the primary id for each doc"""
    ID_RANKS = ['chembl', 'drugbank', 'umls', 'pubchem', 'chebi', 'mesh', 'inchikey', 'inchi', 'rxcui', 'smiles']
    for _id in ID_RANKS:
        if _doc.get(_id):
            return (_id + ':' + str(_doc.get(_id)))