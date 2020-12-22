import os

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.dictionary import Field, Row, Dictionary, DictionaryStructure, Layout
from helpers.external_sources import SourceRedis

cluster = None
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
dict_configs_path = os.path.join(SCRIPT_DIR, 'configs/dictionaries')
node = None

KEY_FIELDS = {
    "simple": [
        Field("KeyField", 'UInt64', is_key=True, default_value_for_get=9999999)
    ],
    "complex": [
        Field("KeyField1", 'UInt64', is_key=True, default_value_for_get=9999999),
        Field("KeyField2", 'String', is_key=True, default_value_for_get='xxxxxxxxx'),
    ]
}

KEY_VALUES = {
    "simple": [
        [1], [2]
    ],
    "complex": [
        [1, 'world'], [2, 'qwerty2']
    ]
}

FIELDS = [
    Field("UInt8_", 'UInt8', default_value_for_get=55),
    Field("UInt16_", 'UInt16', default_value_for_get=66),
    Field("UInt32_", 'UInt32', default_value_for_get=77),
    Field("UInt64_", 'UInt64', default_value_for_get=88),
    Field("Int8_", 'Int8', default_value_for_get=-55),
    Field("Int16_", 'Int16', default_value_for_get=-66),
    Field("Int32_", 'Int32', default_value_for_get=-77),
    Field("Int64_", 'Int64', default_value_for_get=-88),
    Field("UUID_", 'UUID', default_value_for_get='550e8400-0000-0000-0000-000000000000'),
    Field("Date_", 'Date', default_value_for_get='2018-12-30'),
    Field("DateTime_", 'DateTime', default_value_for_get='2018-12-30 00:00:00'),
    Field("String_", 'String', default_value_for_get='hi'),
    Field("Float32_", 'Float32', default_value_for_get=555.11),
    Field("Float64_", 'Float64', default_value_for_get=777.11),
]

VALUES = [
    [22, 3],
    [333, 4],
    [4444, 5],
    [55555, 6],
    [-6, -7],
    [-77, -8],
    [-888, -9],
    [-999, -10],
    ['550e8400-e29b-41d4-a716-446655440003', '550e8400-e29b-41d4-a716-446655440002'],
    ['1973-06-28', '1978-06-28'],
    ['1985-02-28 23:43:25', '1986-02-28 23:42:25'],
    ['hello', 'hello'],
    [22.543, 21.543],
    [3332154213.4, 3222154213.4],
]

LAYOUTS = [
    Layout("flat"),
    Layout("hashed"),
    Layout("cache"),
    Layout("complex_key_hashed"),
    Layout("complex_key_cache"),
    Layout("direct"),
    Layout("complex_key_direct")
]

DICTIONARIES = []


def get_dict(source, layout, fields, suffix_name=''):
    global dict_configs_path

    structure = DictionaryStructure(layout, fields)
    dict_name = source.name + "_" + layout.name + '_' + suffix_name
    dict_path = os.path.join(dict_configs_path, dict_name + '.xml')
    dictionary = Dictionary(dict_name, structure, source, dict_path, "table_" + dict_name, fields)
    dictionary.generate_config()
    return dictionary


def setup_module(module):
    global DICTIONARIES
    global cluster
    global node
    global dict_configs_path

    for f in os.listdir(dict_configs_path):
        os.remove(os.path.join(dict_configs_path, f))

    for i, field in enumerate(FIELDS):
        DICTIONARIES.append([])
        sources = []
        sources.append(SourceRedis("RedisSimple", "localhost", "6380", "redis1", "6379", "", "clickhouse", i * 2,
                                   storage_type="simple"))
        sources.append(SourceRedis("RedisHash", "localhost", "6380", "redis1", "6379", "", "clickhouse", i * 2 + 1,
                                   storage_type="hash_map"))
        for source in sources:
            for layout in LAYOUTS:
                if not source.compatible_with_layout(layout):
                    print("Source", source.name, "incompatible with layout", layout.name)
                    continue

                fields = KEY_FIELDS[layout.layout_type] + [field]
                DICTIONARIES[i].append(get_dict(source, layout, fields, field.name))

    main_configs = []
    dictionaries = []
    for fname in os.listdir(dict_configs_path):
        dictionaries.append(os.path.join(dict_configs_path, fname))

    cluster = ClickHouseCluster(__file__)
    node = cluster.add_instance('node', main_configs=main_configs, dictionaries=dictionaries, with_redis=True)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        assert len(FIELDS) == len(VALUES)
        for dicts in DICTIONARIES:
            for dictionary in dicts:
                print("Preparing", dictionary.name)
                dictionary.prepare_source(cluster)
                print("Prepared")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("id", list(range(len(FIELDS))))
def test_redis_dictionaries(started_cluster, id):
    print('id:', id)

    dicts = DICTIONARIES[id]
    values = VALUES[id]
    field = FIELDS[id]

    node.query("system reload dictionaries")

    for dct in dicts:
        data = []
        dict_type = dct.structure.layout.layout_type
        key_fields = KEY_FIELDS[dict_type]
        key_values = KEY_VALUES[dict_type]

        for key_value, value in zip(key_values, values):
            data.append(Row(key_fields + [field], key_value + [value]))

        dct.load_data(data)

        queries_with_answers = []
        for row in data:
            for query in dct.get_select_get_queries(field, row):
                queries_with_answers.append((query, row.get_value_by_name(field.name)))

            for query in dct.get_select_has_queries(field, row):
                queries_with_answers.append((query, 1))

            for query in dct.get_select_get_or_default_queries(field, row):
                queries_with_answers.append((query, field.default_value_for_get))

        node.query("system reload dictionary {}".format(dct.name))

        for query, answer in queries_with_answers:
            print(query)
            assert node.query(query) == str(answer) + '\n'

    # Checks, that dictionaries can be reloaded.
    node.query("system reload dictionaries")
