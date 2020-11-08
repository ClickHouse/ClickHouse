from helpers.server_with_models import ClickHouseServerWithCatboostModels
from helpers.generate import generate_uniform_string_column, generate_uniform_float_column, generate_uniform_int_column
from helpers.train import train_catboost_model
import os
import numpy as np
from pandas import DataFrame


PORT = int(os.environ.get('CLICKHOUSE_TESTS_PORT', '9000'))
CLICKHOUSE_TESTS_SERVER_BIN_PATH = os.environ.get('CLICKHOUSE_TESTS_SERVER_BIN_PATH', '/usr/bin/clickhouse')


def add_noise_to_target(target, seed, threshold=0.05):
    col = generate_uniform_float_column(len(target), 0., 1., seed + 1) < threshold
    return target * (1 - col) + (1 - target) * col


def check_predictions(test_name, target, pred_python, pred_ch, acc_threshold):
    ch_class = pred_ch.astype(int)
    python_class = pred_python.astype(int)
    if not np.array_equal(ch_class, python_class):
        raise Exception('Got different results:\npython:\n' + str(python_class) + '\nClickHouse:\n' + str(ch_class))

    acc = 1 - np.sum(np.abs(ch_class - np.array(target))) / (len(target) + .0)
    assert acc >= acc_threshold
    print(test_name, 'accuracy: {:.10f}'.format(acc))


def test_apply_float_features_only():

    name = 'test_apply_float_features_only'

    train_size = 10000
    test_size = 10000

    def gen_data(size, seed):
        data = {
            'a': generate_uniform_float_column(size, 0., 1., seed + 1),
            'b': generate_uniform_float_column(size, 0., 1., seed + 2),
            'c': generate_uniform_float_column(size, 0., 1., seed + 3)
        }
        return DataFrame.from_dict(data)

    def get_target(df):
        def target_filter(row):
            return 1 if (row['a'] > .3 and row['b'] > .3) or (row['c'] < .4 and row['a'] * row['b'] > 0.1) else 0
        return df.apply(target_filter, axis=1).as_matrix()

    train_df = gen_data(train_size, 42)
    test_df = gen_data(test_size, 43)

    train_target = get_target(train_df)
    test_target = get_target(test_df)

    print()
    print('train target', train_target)
    print('test target', test_target)

    params = {
        'iterations': 4,
        'depth': 2,
        'learning_rate': 1,
        'loss_function': 'Logloss'
    }

    model = train_catboost_model(train_df, train_target, [], params)
    pred_python = model.predict(test_df)

    server = ClickHouseServerWithCatboostModels(name, CLICKHOUSE_TESTS_SERVER_BIN_PATH, PORT)
    server.add_model(name, model)
    with server:
        pred_ch = (np.array(server.apply_model(name, test_df, [])) > 0).astype(int)

    print('python predictions', pred_python)
    print('clickhouse predictions', pred_ch)

    check_predictions(name, test_target, pred_python, pred_ch, 0.9)


def test_apply_float_features_with_string_cat_features():

    name = 'test_apply_float_features_with_string_cat_features'

    train_size = 10000
    test_size = 10000

    def gen_data(size, seed):
        data = {
            'a': generate_uniform_float_column(size, 0., 1., seed + 1),
            'b': generate_uniform_float_column(size, 0., 1., seed + 2),
            'c': generate_uniform_string_column(size, ['a', 'b', 'c'], seed + 3),
            'd': generate_uniform_string_column(size, ['e', 'f', 'g'], seed + 4)
        }
        return DataFrame.from_dict(data)

    def get_target(df):
        def target_filter(row):
            return 1 if (row['a'] > .3 and row['b'] > .3 and row['c'] != 'a') \
                        or (row['a'] * row['b'] > 0.1 and row['c'] != 'b' and row['d'] != 'e') else 0
        return df.apply(target_filter, axis=1).as_matrix()

    train_df = gen_data(train_size, 42)
    test_df = gen_data(test_size, 43)

    train_target = get_target(train_df)
    test_target = get_target(test_df)

    print()
    print('train target', train_target)
    print('test target', test_target)

    params = {
        'iterations': 6,
        'depth': 2,
        'learning_rate': 1,
        'loss_function': 'Logloss'
    }

    model = train_catboost_model(train_df, train_target, ['c', 'd'], params)
    pred_python = model.predict(test_df)

    server = ClickHouseServerWithCatboostModels(name, CLICKHOUSE_TESTS_SERVER_BIN_PATH, PORT)
    server.add_model(name, model)
    with server:
        pred_ch = (np.array(server.apply_model(name, test_df, [])) > 0).astype(int)

    print('python predictions', pred_python)
    print('clickhouse predictions', pred_ch)

    check_predictions(name, test_target, pred_python, pred_ch, 0.9)


def test_apply_float_features_with_int_cat_features():

    name = 'test_apply_float_features_with_int_cat_features'

    train_size = 10000
    test_size = 10000

    def gen_data(size, seed):
        data = {
            'a': generate_uniform_float_column(size, 0., 1., seed + 1),
            'b': generate_uniform_float_column(size, 0., 1., seed + 2),
            'c': generate_uniform_int_column(size, 1, 4, seed + 3),
            'd': generate_uniform_int_column(size, 1, 4, seed + 4)
        }
        return DataFrame.from_dict(data)

    def get_target(df):
        def target_filter(row):
            return 1 if (row['a'] > .3 and row['b'] > .3 and row['c'] != 1) \
                        or (row['a'] * row['b'] > 0.1 and row['c'] != 2 and row['d'] != 3) else 0
        return df.apply(target_filter, axis=1).as_matrix()

    train_df = gen_data(train_size, 42)
    test_df = gen_data(test_size, 43)

    train_target = get_target(train_df)
    test_target = get_target(test_df)

    print()
    print('train target', train_target)
    print('test target', test_target)

    params = {
        'iterations': 6,
        'depth': 4,
        'learning_rate': 1,
        'loss_function': 'Logloss'
    }

    model = train_catboost_model(train_df, train_target, ['c', 'd'], params)
    pred_python = model.predict(test_df)

    server = ClickHouseServerWithCatboostModels(name, CLICKHOUSE_TESTS_SERVER_BIN_PATH, PORT)
    server.add_model(name, model)
    with server:
        pred_ch = (np.array(server.apply_model(name, test_df, [])) > 0).astype(int)

    print('python predictions', pred_python)
    print('clickhouse predictions', pred_ch)

    check_predictions(name, test_target, pred_python, pred_ch, 0.9)


def test_apply_float_features_with_mixed_cat_features():

    name = 'test_apply_float_features_with_mixed_cat_features'

    train_size = 10000
    test_size = 10000

    def gen_data(size, seed):
        data = {
            'a': generate_uniform_float_column(size, 0., 1., seed + 1),
            'b': generate_uniform_float_column(size, 0., 1., seed + 2),
            'c': generate_uniform_string_column(size, ['a', 'b', 'c'], seed + 3),
            'd': generate_uniform_int_column(size, 1, 4, seed + 4)
        }
        return DataFrame.from_dict(data)

    def get_target(df):
        def target_filter(row):
            return 1 if (row['a'] > .3 and row['b'] > .3 and row['c'] != 'a') \
                        or (row['a'] * row['b'] > 0.1 and row['c'] != 'b' and row['d'] != 2) else 0
        return df.apply(target_filter, axis=1).as_matrix()

    train_df = gen_data(train_size, 42)
    test_df = gen_data(test_size, 43)

    train_target = get_target(train_df)
    test_target = get_target(test_df)

    print()
    print('train target', train_target)
    print('test target', test_target)

    params = {
        'iterations': 6,
        'depth': 4,
        'learning_rate': 1,
        'loss_function': 'Logloss'
    }

    model = train_catboost_model(train_df, train_target, ['c', 'd'], params)
    pred_python = model.predict(test_df)

    server = ClickHouseServerWithCatboostModels(name, CLICKHOUSE_TESTS_SERVER_BIN_PATH, PORT)
    server.add_model(name, model)
    with server:
        pred_ch = (np.array(server.apply_model(name, test_df, [])) > 0).astype(int)

    print('python predictions', pred_python)
    print('clickhouse predictions', pred_ch)

    check_predictions(name, test_target, pred_python, pred_ch, 0.9)


def test_apply_multiclass():

    name = 'test_apply_float_features_with_mixed_cat_features'

    train_size = 10000
    test_size = 10000

    def gen_data(size, seed):
        data = {
            'a': generate_uniform_float_column(size, 0., 1., seed + 1),
            'b': generate_uniform_float_column(size, 0., 1., seed + 2),
            'c': generate_uniform_string_column(size, ['a', 'b', 'c'], seed + 3),
            'd': generate_uniform_int_column(size, 1, 4, seed + 4)
        }
        return DataFrame.from_dict(data)

    def get_target(df):
        def target_filter(row):
            if row['a'] > .3 and row['b'] > .3 and row['c'] != 'a':
                return 0
            elif row['a'] * row['b'] > 0.1 and row['c'] != 'b' and row['d'] != 2:
                return 1
            else:
                return 2

        return df.apply(target_filter, axis=1).as_matrix()

    train_df = gen_data(train_size, 42)
    test_df = gen_data(test_size, 43)

    train_target = get_target(train_df)
    test_target = get_target(test_df)

    print()
    print('train target', train_target)
    print('test target', test_target)

    params = {
        'iterations': 10,
        'depth': 4,
        'learning_rate': 1,
        'loss_function': 'MultiClass'
    }

    model = train_catboost_model(train_df, train_target, ['c', 'd'], params)
    pred_python = model.predict(test_df)[:,0].astype(int)

    server = ClickHouseServerWithCatboostModels(name, CLICKHOUSE_TESTS_SERVER_BIN_PATH, PORT)
    server.add_model(name, model)
    with server:
        pred_ch = np.argmax(np.array(server.apply_model(name, test_df, [])), axis=1)

    print('python predictions', pred_python)
    print('clickhouse predictions', pred_ch)

    check_predictions(name, test_target, pred_python, pred_ch, 0.9)
