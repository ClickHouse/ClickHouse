import pytest
import os
import time

from helpers.cluster import ClickHouseCluster
from dictionary import Field, Row, Dictionary, DictionaryStructure, Layout
from external_sources import SourceMySQL, SourceMongo, SourceClickHouse, SourceFile, SourceExecutableCache, SourceExecutableHashed

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

FIELDS = {
    "simple": [
        Field("KeyField", 'UInt64', is_key=True),
        Field("UInt8_", 'UInt8'),
        Field("UInt16_", 'UInt16'),
        Field("UInt32_", 'UInt32'),
        Field("UInt64_", 'UInt64'),
        Field("Int8_", 'Int8'),
        Field("Int16_", 'Int16'),
        Field("Int32_", 'Int32'),
        Field("Int64_", 'Int64'),
        Field("UUID_", 'UUID'),
        Field("Date_", 'Date'),
        Field("DateTime_", 'DateTime'),
        Field("String_", 'String'),
        Field("Float32_", 'Float32'),
        Field("Float64_", 'Float64'),
    ],
    "complex": [
        Field("KeyField1", 'UInt64', is_key=True),
        Field("KeyField2", 'String', is_key=True),
        Field("UInt8_", 'UInt8'),
        Field("UInt16_", 'UInt16'),
        Field("UInt32_", 'UInt32'),
        Field("UInt64_", 'UInt64'),
        Field("Int8_", 'Int8'),
        Field("Int16_", 'Int16'),
        Field("Int32_", 'Int32'),
        Field("Int64_", 'Int64'),
        Field("UUID_", 'UUID'),
        Field("Date_", 'Date'),
        Field("DateTime_", 'DateTime'),
        Field("String_", 'String'),
        Field("Float32_", 'Float32'),
        Field("Float64_", 'Float64'),
    ],
    "ranged": [
        Field("KeyField1", 'UInt64', is_key=True),
        Field("KeyField2", 'Date', is_range_key=True),
        Field("StartDate", 'Date', range_hash_type='min'),
        Field("EndDate", 'Date', range_hash_type='max'),
        Field("UInt8_", 'UInt8'),
        Field("UInt16_", 'UInt16'),
        Field("UInt32_", 'UInt32'),
        Field("UInt64_", 'UInt64'),
        Field("Int8_", 'Int8'),
        Field("Int16_", 'Int16'),
        Field("Int32_", 'Int32'),
        Field("Int64_", 'Int64'),
        Field("UUID_", 'UUID'),
        Field("Date_", 'Date'),
        Field("DateTime_", 'DateTime'),
        Field("String_", 'String'),
        Field("Float32_", 'Float32'),
        Field("Float64_", 'Float64'),
    ]

}

LAYOUTS = [
    Layout("hashed"),
    Layout("cache"),
    Layout("flat"),
    Layout("complex_key_hashed"),
    Layout("complex_key_cache"),
    Layout("range_hashed")
]

SOURCES = [
    #SourceMongo("MongoDB", "localhost", "27018", "mongo1", "27017", "root", "clickhouse"),
    SourceMySQL("MySQL", "localhost", "3308", "mysql1", "3306", "root", "clickhouse"),
    SourceClickHouse("RemoteClickHouse", "localhost", "9000", "clickhouse1", "9000", "default", ""),
    SourceClickHouse("LocalClickHouse", "localhost", "9000", "node", "9000", "default", ""),
    SourceFile("File", "localhost", "9000", "node", "9000", "", ""),
    SourceExecutableHashed("ExecutableHashed", "localhost", "9000", "node", "9000", "", ""),
    SourceExecutableCache("ExecutableCache", "localhost", "9000", "node", "9000", "", ""),
]

DICTIONARIES = []

cluster = None
node = None

def setup_module(module):
    global DICTIONARIES
    global cluster
    global node

    dict_configs_path = os.path.join(SCRIPT_DIR, 'configs/dictionaries')
    for f in os.listdir(dict_configs_path):
        os.remove(os.path.join(dict_configs_path, f))

    for layout in LAYOUTS:
        for source in SOURCES:
            if source.compatible_with_layout(layout):
                structure = DictionaryStructure(layout, FIELDS[layout.layout_type])
                dict_name = source.name + "_" + layout.name
                dict_path = os.path.join(dict_configs_path, dict_name + '.xml')
                dictionary = Dictionary(dict_name, structure, source, dict_path, "table_" + dict_name)
                dictionary.generate_config()
                DICTIONARIES.append(dictionary)
            else:
                print "Source", source.name, "incompatible with layout", layout.name

    main_configs = []
    for fname in os.listdir(dict_configs_path):
        main_configs.append(os.path.join(dict_configs_path, fname))
    cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))
    node = cluster.add_instance('node', main_configs=main_configs, with_mysql=True, with_mongo=True)
    cluster.add_instance('clickhouse1')

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for dictionary in DICTIONARIES:
            print "Preparing", dictionary.name
            dictionary.prepare_source(cluster)
            print "Prepared"

        yield cluster

    finally:
        pass
        cluster.shutdown()


def test_simple_dictionaries(started_cluster):
    fields = FIELDS["simple"]
    data = [
        Row(fields, [1, 22, 333, 4444, 55555, -6, -77,
                     -888, -999, '550e8400-e29b-41d4-a716-446655440003',
                     '1973-06-28', '1985-02-28 23:43:25', 'hello', 22.543, 3332154213.4]),
    ]

    simple_dicts = [d for d in DICTIONARIES if d.structure.layout.layout_type == "simple"]
    for dct in simple_dicts:
        dct.load_data(data)

    node.query("system reload dictionaries")
    queries_with_answers = []
    for dct in simple_dicts:
        for row in data:
            for field in fields:
                if not field.is_key:
                    queries_with_answers.append((dct.get_select_query(field, row), row.get_value_by_name(field.name)))

    for query, answer in queries_with_answers:
        print query
        assert node.query(query) == str(answer) + '\n'

def test_complex_dictionaries(started_cluster):
    fields = FIELDS["complex"]
    data = [
        Row(fields, [1, 'world', 22, 333, 4444, 55555, -6,
                     -77, -888, -999, '550e8400-e29b-41d4-a716-446655440003',
                     '1973-06-28', '1985-02-28 23:43:25',
                     'hello', 22.543, 3332154213.4]),
    ]

    complex_dicts = [d for d in DICTIONARIES if d.structure.layout.layout_type == "complex"]
    for dct in complex_dicts:
        dct.load_data(data)

    node.query("system reload dictionaries")
    queries_with_answers = []
    for dct in complex_dicts:
        for row in data:
            for field in fields:
                if not field.is_key:
                    queries_with_answers.append((dct.get_select_query(field, row), row.get_value_by_name(field.name)))

    for query, answer in queries_with_answers:
        print query
        assert node.query(query) == str(answer) + '\n'

def test_ranged_dictionaries(started_cluster):
    fields = FIELDS["ranged"]
    data = [
        Row(fields, [1, '2019-02-10', '2019-02-01', '2019-02-28',
                     22, 333, 4444, 55555, -6, -77, -888, -999,
                     '550e8400-e29b-41d4-a716-446655440003',
                     '1973-06-28', '1985-02-28 23:43:25', 'hello',
                     22.543, 3332154213.4]),
    ]

    ranged_dicts = [d for d in DICTIONARIES if d.structure.layout.layout_type == "ranged"]
    for dct in ranged_dicts:
        dct.load_data(data)

    node.query("system reload dictionaries")

    queries_with_answers = []
    for dct in ranged_dicts:
        for row in data:
            for field in fields:
                if not field.is_key and not field.is_range:
                    queries_with_answers.append((dct.get_select_query(field, row), row.get_value_by_name(field.name)))

    for query, answer in queries_with_answers:
        print query
        assert node.query(query) == str(answer) + '\n'
