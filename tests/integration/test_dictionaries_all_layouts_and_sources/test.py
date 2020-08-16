import pytest
import os

from helpers.cluster import ClickHouseCluster
from dictionary import Field, Row, Dictionary, DictionaryStructure, Layout
from external_sources import SourceMySQL, SourceClickHouse, SourceFile, SourceExecutableCache, SourceExecutableHashed
from external_sources import SourceMongo, SourceMongoURI, SourceHTTP, SourceHTTPS, SourceRedis, SourceCassandra
import math

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
dict_configs_path = os.path.join(SCRIPT_DIR, 'configs/dictionaries')

FIELDS = {
    "simple": [
        Field("KeyField", 'UInt64', is_key=True, default_value_for_get=9999999),
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
        Field("ParentKeyField", "UInt64", default_value_for_get=444, hierarchical=True)
    ],
    "complex": [
        Field("KeyField1", 'UInt64', is_key=True, default_value_for_get=9999999),
        Field("KeyField2", 'String', is_key=True, default_value_for_get='xxxxxxxxx'),
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
    ],
    "ranged": [
        Field("KeyField1", 'UInt64', is_key=True),
        Field("KeyField2", 'Date', is_range_key=True),
        Field("StartDate", 'Date', range_hash_type='min'),
        Field("EndDate", 'Date', range_hash_type='max'),
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
}

VALUES = {
    "simple": [
        [1, 22, 333, 4444, 55555, -6, -77,
         -888, -999, '550e8400-e29b-41d4-a716-446655440003',
         '1973-06-28', '1985-02-28 23:43:25', 'hello', 22.543, 3332154213.4, 0],
        [2, 3, 4, 5, 6, -7, -8,
         -9, -10, '550e8400-e29b-41d4-a716-446655440002',
         '1978-06-28', '1986-02-28 23:42:25', 'hello', 21.543, 3222154213.4, 1]
    ],
    "complex": [
        [1, 'world', 22, 333, 4444, 55555, -6,
         -77, -888, -999, '550e8400-e29b-41d4-a716-446655440003',
         '1973-06-28', '1985-02-28 23:43:25',
         'hello', 22.543, 3332154213.4],
        [2, 'qwerty2', 52, 2345, 6544, 9191991, -2,
         -717, -81818, -92929, '550e8400-e29b-41d4-a716-446655440007',
         '1975-09-28', '2000-02-28 23:33:24',
         'my', 255.543, 3332221.44]

    ],
    "ranged": [
        [1, '2019-02-10', '2019-02-01', '2019-02-28',
         22, 333, 4444, 55555, -6, -77, -888, -999,
         '550e8400-e29b-41d4-a716-446655440003',
         '1973-06-28', '1985-02-28 23:43:25', 'hello',
         22.543, 3332154213.4],
        [2, '2019-04-10', '2019-04-01', '2019-04-28',
         11, 3223, 41444, 52515, -65, -747, -8388, -9099,
         '550e8400-e29b-41d4-a716-446655440004',
         '1973-06-29', '2002-02-28 23:23:25', '!!!!',
         32.543, 3332543.4]
    ]
}



LAYOUTS = [
    Layout("flat"),
    Layout("hashed"),
    Layout("cache"),
    Layout("complex_key_hashed"),
    Layout("complex_key_cache"),
    Layout("range_hashed"),
    Layout("direct"),
    Layout("complex_key_direct")
]

SOURCES = [
    SourceCassandra("Cassandra", "localhost", "9043", "cassandra1", "9042", "", ""),
    SourceMongo("MongoDB", "localhost", "27018", "mongo1", "27017", "root", "clickhouse"),
    SourceMongoURI("MongoDB_URI", "localhost", "27018", "mongo1", "27017", "root", "clickhouse"),
    SourceMySQL("MySQL", "localhost", "3308", "mysql1", "3306", "root", "clickhouse"),
    SourceClickHouse("RemoteClickHouse", "localhost", "9000", "clickhouse1", "9000", "default", ""),
    SourceClickHouse("LocalClickHouse", "localhost", "9000", "node", "9000", "default", ""),
    SourceFile("File", "localhost", "9000", "node", "9000", "", ""),
    SourceExecutableHashed("ExecutableHashed", "localhost", "9000", "node", "9000", "", ""),
    SourceExecutableCache("ExecutableCache", "localhost", "9000", "node", "9000", "", ""),
    SourceHTTP("SourceHTTP", "localhost", "9000", "clickhouse1", "9000", "", ""),
    SourceHTTPS("SourceHTTPS", "localhost", "9000", "clickhouse1", "9000", "", ""),
]

DICTIONARIES = []

# Key-value dictionaries with only one possible field for key
SOURCES_KV = [
    SourceRedis("RedisSimple", "localhost", "6380", "redis1", "6379", "", "", storage_type="simple"),
    SourceRedis("RedisHash", "localhost", "6380", "redis1", "6379", "", "", storage_type="hash_map"),
]

DICTIONARIES_KV = []

cluster = None
node = None

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

    for layout in LAYOUTS:
        for source in SOURCES:
            if source.compatible_with_layout(layout):
                DICTIONARIES.append(get_dict(source, layout, FIELDS[layout.layout_type]))
            else:
                print "Source", source.name, "incompatible with layout", layout.name

    for layout in LAYOUTS:
        field_keys = list(filter(lambda x: x.is_key, FIELDS[layout.layout_type]))
        for source in SOURCES_KV:
            if not source.compatible_with_layout(layout):
                print "Source", source.name, "incompatible with layout", layout.name
                continue

            for field in FIELDS[layout.layout_type]:
                if not (field.is_key or field.is_range or field.is_range_key):
                    DICTIONARIES_KV.append(get_dict(source, layout, field_keys + [field], field.name))

    main_configs = []
    for fname in os.listdir(dict_configs_path):
        main_configs.append(os.path.join(dict_configs_path, fname))
    cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))
    node = cluster.add_instance('node', main_configs=main_configs, with_mysql=True, with_mongo=True, with_redis=True, with_cassandra=True)
    cluster.add_instance('clickhouse1')


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for dictionary in DICTIONARIES + DICTIONARIES_KV:
            print "Preparing", dictionary.name
            dictionary.prepare_source(cluster)
            print "Prepared"

        yield cluster

    finally:
        cluster.shutdown()


def get_dictionaries(fold, total_folds, all_dicts):
    chunk_len = int(math.ceil(len(all_dicts) / float(total_folds)))
    if chunk_len * fold >= len(all_dicts):
        return []
    return all_dicts[fold * chunk_len : (fold + 1) * chunk_len]


def remove_mysql_dicts():
    """
    We have false-positive race condition in our openSSL version.
    MySQL dictionary use OpenSSL, so to prevent known failure we
    disable tests for these dictionaries.

    Read of size 8 at 0x7b3c00005dd0 by thread T61 (mutexes: write M1010349240585225536):
    #0 EVP_CIPHER_mode <null> (clickhouse+0x13b2223b)
    #1 do_ssl3_write <null> (clickhouse+0x13a137bc)
    #2 ssl3_write_bytes <null> (clickhouse+0x13a12387)
    #3 ssl3_write <null> (clickhouse+0x139db0e6)
    #4 ssl_write_internal <null> (clickhouse+0x139eddce)
    #5 SSL_write <null> (clickhouse+0x139edf20)
    #6 ma_tls_write <null> (clickhouse+0x139c7557)
    #7 ma_pvio_tls_write <null> (clickhouse+0x139a8f59)
    #8 ma_pvio_write <null> (clickhouse+0x139a8488)
    #9 ma_net_real_write <null> (clickhouse+0x139a4e2c)
    #10 ma_net_write_command <null> (clickhouse+0x139a546d)
    #11 mthd_my_send_cmd <null> (clickhouse+0x13992546)
    #12 mysql_close_slow_part <null> (clickhouse+0x13999afd)
    #13 mysql_close <null> (clickhouse+0x13999071)
    #14 mysqlxx::Connection::~Connection() <null> (clickhouse+0x1370f814)
    #15 mysqlxx::Pool::~Pool() <null> (clickhouse+0x13715a7b)

    TODO remove this when open ssl will be fixed or thread sanitizer will be suppressed
    """

    global DICTIONARIES
    DICTIONARIES = [d for d in DICTIONARIES if not d.name.startswith("MySQL")]


@pytest.mark.parametrize("fold", list(range(10)))
def test_simple_dictionaries(started_cluster, fold):
    if node.is_built_with_thread_sanitizer():
        remove_mysql_dicts()

    fields = FIELDS["simple"]
    values = VALUES["simple"]
    data = [Row(fields, vals) for vals in values]

    all_simple_dicts = [d for d in DICTIONARIES if d.structure.layout.layout_type == "simple"]
    simple_dicts = get_dictionaries(fold, 10, all_simple_dicts)

    print "Length of dicts:", len(simple_dicts)
    for dct in simple_dicts:
        dct.load_data(data)

    node.query("system reload dictionaries")

    queries_with_answers = []
    for dct in simple_dicts:
        for row in data:
            for field in fields:
                if not field.is_key:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append((query, row.get_value_by_name(field.name)))

                    for query in dct.get_select_has_queries(field, row):
                        queries_with_answers.append((query, 1))

                    for query in dct.get_select_get_or_default_queries(field, row):
                        queries_with_answers.append((query, field.default_value_for_get))
        for query in dct.get_hierarchical_queries(data[0]):
            queries_with_answers.append((query, [1]))

        for query in dct.get_hierarchical_queries(data[1]):
            queries_with_answers.append((query, [2, 1]))

        for query in dct.get_is_in_queries(data[0], data[1]):
            queries_with_answers.append((query, 0))

        for query in dct.get_is_in_queries(data[1], data[0]):
            queries_with_answers.append((query, 1))

    for query, answer in queries_with_answers:
        print query
        if isinstance(answer, list):
            answer = str(answer).replace(' ', '')
        assert node.query(query) == str(answer) + '\n'


@pytest.mark.parametrize("fold", list(range(10)))
def test_complex_dictionaries(started_cluster, fold):

    if node.is_built_with_thread_sanitizer():
        remove_mysql_dicts()

    fields = FIELDS["complex"]
    values = VALUES["complex"]
    data = [Row(fields, vals) for vals in values]

    all_complex_dicts = [d for d in DICTIONARIES if d.structure.layout.layout_type == "complex"]
    complex_dicts = get_dictionaries(fold, 10, all_complex_dicts)

    for dct in complex_dicts:
        dct.load_data(data)

    node.query("system reload dictionaries")

    queries_with_answers = []
    for dct in complex_dicts:
        for row in data:
            for field in fields:
                if not field.is_key:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append((query, row.get_value_by_name(field.name)))

                    for query in dct.get_select_has_queries(field, row):
                        queries_with_answers.append((query, 1))

                    for query in dct.get_select_get_or_default_queries(field, row):
                        queries_with_answers.append((query, field.default_value_for_get))

    for query, answer in queries_with_answers:
        print query
        assert node.query(query) == str(answer) + '\n'


@pytest.mark.parametrize("fold", list(range(10)))
def test_ranged_dictionaries(started_cluster, fold):
    if node.is_built_with_thread_sanitizer():
        remove_mysql_dicts()

    fields = FIELDS["ranged"]
    values = VALUES["ranged"]
    data = [Row(fields, vals) for vals in values]

    all_ranged_dicts = [d for d in DICTIONARIES if d.structure.layout.layout_type == "ranged"]
    ranged_dicts = get_dictionaries(fold, 10, all_ranged_dicts)

    for dct in ranged_dicts:
        dct.load_data(data)

    node.query("system reload dictionaries")

    queries_with_answers = []
    for dct in ranged_dicts:
        for row in data:
            for field in fields:
                if not field.is_key and not field.is_range:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append((query, row.get_value_by_name(field.name)))

    for query, answer in queries_with_answers:
        print query
        assert node.query(query) == str(answer) + '\n'


@pytest.mark.parametrize("fold", list(range(10)))
def test_key_value_simple_dictionaries(started_cluster, fold):
    fields = FIELDS["simple"]
    values = VALUES["simple"]
    data = [Row(fields, vals) for vals in values]

    all_simple_dicts = [d for d in DICTIONARIES_KV if d.structure.layout.layout_type == "simple"]
    simple_dicts = get_dictionaries(fold, 10, all_simple_dicts)

    for dct in simple_dicts:
        queries_with_answers = []
        local_data = []
        for row in data:
            local_fields = dct.get_fields()
            local_values = [row.get_value_by_name(field.name) for field in local_fields if row.has_field(field.name)]
            local_data.append(Row(local_fields, local_values))

        dct.load_data(local_data)

        node.query("system reload dictionary {}".format(dct.name))

        print 'name: ', dct.name

        for row in local_data:
            print dct.get_fields()
            for field in dct.get_fields():
                print field.name, field.is_key
                if not field.is_key:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append((query, row.get_value_by_name(field.name)))

                    for query in dct.get_select_has_queries(field, row):
                        queries_with_answers.append((query, 1))

                    for query in dct.get_select_get_or_default_queries(field, row):
                        queries_with_answers.append((query, field.default_value_for_get))

        if dct.structure.has_hierarchy:
            for query in dct.get_hierarchical_queries(data[0]):
                queries_with_answers.append((query, [1]))

            for query in dct.get_hierarchical_queries(data[1]):
                queries_with_answers.append((query, [2, 1]))

            for query in dct.get_is_in_queries(data[0], data[1]):
                queries_with_answers.append((query, 0))

            for query in dct.get_is_in_queries(data[1], data[0]):
                queries_with_answers.append((query, 1))

        for query, answer in queries_with_answers:
            print query
            if isinstance(answer, list):
                answer = str(answer).replace(' ', '')
            assert node.query(query) == str(answer) + '\n'


@pytest.mark.parametrize("fold", list(range(10)))
def test_key_value_complex_dictionaries(started_cluster, fold):
    fields = FIELDS["complex"]
    values = VALUES["complex"]
    data = [Row(fields, vals) for vals in values]

    all_complex_dicts = [d for d in DICTIONARIES_KV if d.structure.layout.layout_type == "complex"]
    complex_dicts = get_dictionaries(fold, 10, all_complex_dicts)
    for dct in complex_dicts:
        dct.load_data(data)

    node.query("system reload dictionaries")

    for dct in complex_dicts:
        queries_with_answers = []
        local_data = []
        for row in data:
            local_fields = dct.get_fields()
            local_values = [row.get_value_by_name(field.name) for field in local_fields if row.has_field(field.name)]
            local_data.append(Row(local_fields, local_values))

        dct.load_data(local_data)

        node.query("system reload dictionary {}".format(dct.name))

        for row in local_data:
            for field in dct.get_fields():
                if not field.is_key:
                    for query in dct.get_select_get_queries(field, row):
                        queries_with_answers.append((query, row.get_value_by_name(field.name)))

                    for query in dct.get_select_has_queries(field, row):
                        queries_with_answers.append((query, 1))

                    for query in dct.get_select_get_or_default_queries(field, row):
                        queries_with_answers.append((query, field.default_value_for_get))

        for query, answer in queries_with_answers:
            print query
            assert node.query(query) == str(answer) + '\n'
