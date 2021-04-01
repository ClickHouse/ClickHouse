import os
import sys
from pandas import DataFrame

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CATBOOST_ROOT = os.path.dirname(SCRIPT_DIR)
CATBOOST_PYTHON_DIR = os.path.join(CATBOOST_ROOT, 'data', 'python-package')

if CATBOOST_PYTHON_DIR not in sys.path:
    sys.path.append(CATBOOST_PYTHON_DIR)


import catboost
from catboost import CatBoostClassifier


def train_catboost_model(df, target, cat_features, params, verbose=True):

    if not isinstance(df, DataFrame):
        raise Exception('DataFrame object expected, but got ' + repr(df))

    print('features:', df.columns.tolist())

    cat_features_index = list(df.columns.get_loc(feature) for feature in cat_features)
    print('cat features:', cat_features_index)
    model = CatBoostClassifier(**params)
    model.fit(df, target, cat_features=cat_features_index, verbose=verbose)
    return model
