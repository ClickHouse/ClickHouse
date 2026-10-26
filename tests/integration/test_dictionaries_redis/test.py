import logging
import os
import shutil

import pytest
import redis

from helpers.cluster import ClickHouseCluster
from helpers.dictionary import Dictionary, DictionaryStructure, Field, Layout, Row
from helpers.external_sources import SourceRedis

cluster = ClickHouseCluster(__file__)
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

KEY_FIELDS = {
    "simple": [Field("KeyField", "UInt64", is_key=True, default_value_for_get=9999999)],
    "complex": [
        Field("KeyField1", "UInt64", is_key=True, default_value_for_get=9999999),
        Field("KeyField2", "String", is_key=True, default_value_for_get="xxxxxxxxx"),
    ],
    "complex_single": [
        Field("KeyField", "String", is_key=True, default_value_for_get="xxxxxxxxx")
    ],
}

KEY_VALUES = {
    "simple": [[1], [2]],
    "complex": [[1, "world"], [2, "qwerty2"]],
    "complex_single": [["hello"], ["world"]],
}


def get_key_kind(source, layout):
    """Pick the key structure for a (source, layout) pair.

    'hash_map' Redis storage encodes composite (two-column) complex keys, while
    'simple' storage is a flat key-value map and only fits a single key column.
    """
    if not layout.is_complex:
        return "simple"
    if source.storage_type == "hash_map":
        return "complex"
    return "complex_single"


FIELDS = [
    Field("UInt8_", "UInt8", default_value_for_get=55),
    Field("UInt16_", "UInt16", default_value_for_get=66),
    Field("UInt32_", "UInt32", default_value_for_get=77),
    Field("UInt64_", "UInt64", default_value_for_get=88),
    Field("Int8_", "Int8", default_value_for_get=-55),
    Field("Int16_", "Int16", default_value_for_get=-66),
    Field("Int32_", "Int32", default_value_for_get=-77),
    Field("Int64_", "Int64", default_value_for_get=-88),
    Field(
        "UUID_", "UUID", default_value_for_get="550e8400-0000-0000-0000-000000000000"
    ),
    Field("Date_", "Date", default_value_for_get="2018-12-30"),
    Field("DateTime_", "DateTime", default_value_for_get="2018-12-30 00:00:00"),
    Field("String_", "String", default_value_for_get="hi"),
    Field("Float32_", "Float32", default_value_for_get=555.11),
    Field("Float64_", "Float64", default_value_for_get=777.11),
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
    ["550e8400-e29b-41d4-a716-446655440003", "550e8400-e29b-41d4-a716-446655440002"],
    ["1973-06-28", "1978-06-28"],
    ["1985-02-28 23:43:25", "1986-02-28 23:42:25"],
    ["hello", "hello"],
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
    Layout("complex_key_direct"),
]

DICTIONARIES = []


def get_dict(source, layout, fields, suffix_name, dict_configs_path):
    structure = DictionaryStructure(layout, fields)
    dict_name = source.name + "_" + layout.name + "_" + suffix_name
    dict_path = os.path.join(dict_configs_path, dict_name + ".xml")
    dictionary = Dictionary(
        dict_name, structure, source, dict_path, "table_" + dict_name, fields
    )
    dictionary.generate_config()
    return dictionary


def generate_dict_configs():
    global DICTIONARIES
    global cluster
    dict_configs_path = os.path.join(SCRIPT_DIR, "configs/dictionaries")
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "")
    if worker_id:
        dict_configs_path += f"_{worker_id}"

    if os.path.exists(dict_configs_path):
        shutil.rmtree(dict_configs_path)
    os.mkdir(dict_configs_path)

    def make_source(name, db_index, storage_type):
        return SourceRedis(
            name,
            "localhost",
            cluster.redis_port,
            cluster.redis_host,
            "6379",
            "",
            "clickhouse",
            db_index,
            storage_type=storage_type,
        )

    db_index = 0
    for i, field in enumerate(FIELDS):
        DICTIONARIES.append([])
        for storage_type in ["simple", "hash_map"]:
            for layout in LAYOUTS:
                source = make_source(f"Redis_{storage_type}", db_index, storage_type)
                if not source.compatible_with_layout(layout):
                    continue
                fields = KEY_FIELDS[get_key_kind(source, layout)] + [field]
                DICTIONARIES[i].append(get_dict(source, layout, fields, field.name, dict_configs_path))
                db_index += 1

    main_configs = []
    dictionaries = []
    for fname in os.listdir(dict_configs_path):
        path = os.path.join(dict_configs_path, fname)
        logging.debug(f"Found dictionary {path}")
        dictionaries.append(path)

    cluster.add_instance(
        "node", main_configs=main_configs, dictionaries=dictionaries, with_redis=True
    )


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        generate_dict_configs()

        cluster.start()
        assert len(FIELDS) == len(VALUES)
        for dicts in DICTIONARIES:
            for dictionary in dicts:
                logging.debug(f"Preparing {dictionary.name}")
                dictionary.prepare_source(cluster)
                logging.debug(f"Prepared {dictionary.name}")

        yield cluster

    finally:
        cluster.shutdown()


def get_entity_id(entity):
    return FIELDS[entity].name


@pytest.mark.parametrize("id", list(range(len(FIELDS))), ids=get_entity_id)
def test_redis_dictionaries(started_cluster, id):
    logging.debug(f"Run test with id: {id}")

    dicts = DICTIONARIES[id]
    values = VALUES[id]
    field = FIELDS[id]

    node = started_cluster.instances["node"]
    node.query("system reload dictionaries")

    for dct in dicts:
        data = []
        key_kind = get_key_kind(dct.source, dct.structure.layout)
        key_fields = KEY_FIELDS[key_kind]
        key_values = KEY_VALUES[key_kind]

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

        node.query(f"system reload dictionary {dct.name}")

        for query, answer in queries_with_answers:
            assert node.query(query) == str(answer) + "\n"

    # Checks that dictionaries can be reloaded.
    node.query("system reload dictionaries")


def test_redis_storage_type_key_constraints(started_cluster):
    node = started_cluster.instances["node"]
    host = started_cluster.redis_host

    DB_INDEX = sum(len(dicts) for dicts in DICTIONARIES) + 1  # Pick a DB index that is not used by any dictionary.

    redis_client = redis.Redis(
        host="localhost", port=started_cluster.redis_port, password="clickhouse", db=DB_INDEX
    )
    redis_client.flushdb()
    redis_client.set("k1", "v1")

    node.query("DROP DICTIONARY IF EXISTS test_redis_simple_single")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_simple_single (key String, value String)
        PRIMARY KEY key
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'simple'))
        LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1000))
        LIFETIME(MIN 1 MAX 1)
        """
    )
    assert node.query("SELECT dictGet('test_redis_simple_single', 'value', tuple('k1'))") == "v1\n"
    node.query("DROP DICTIONARY IF EXISTS test_redis_simple_single")

    redis_client2 = redis.Redis(
        host="localhost", port=started_cluster.redis_port, password="clickhouse", db=DB_INDEX+1
    )
    redis_client2.flushdb()
    redis_client2.set("-1", "v1")
    redis_client2.set("-2", "v1")

    node.query("DROP DICTIONARY IF EXISTS test_redis_negative_key")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_negative_key (key Int64, value String)
        PRIMARY KEY key
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX+1} STORAGE_TYPE 'simple'))
        LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1000))
        LIFETIME(MIN 1 MAX 1)
        """
    )
    assert node.query("SELECT dictGet('test_redis_negative_key', 'value', tuple(toInt64(-1)))") == "v1\n"
    node.query("DROP DICTIONARY IF EXISTS test_redis_negative_key")

    # an unsigned key above 2^63 must be serialized through `getUInt` and not `getInt`.
    # reading a UInt64 column with `getInt` reinterprets the value as a negative Int64
    # and sends the wrong Redis key, which reports a present key as missing.
    redis_client.set("13824756489203974851", "v1")

    node.query("DROP DICTIONARY IF EXISTS test_redis_large_uint_key")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_large_uint_key (key UInt64, value String)
        PRIMARY KEY key
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'simple'))
        LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1000))
        LIFETIME(MIN 1 MAX 1)
        """
    )
    assert (
        node.query(
            "SELECT dictGet('test_redis_large_uint_key', 'value', tuple(toUInt64(13824756489203974851)))"
        )
        == "v1\n"
    )
    node.query("DROP DICTIONARY IF EXISTS test_redis_large_uint_key")

    # the same signed-key serialization is used for the nested 'hash_map' path, which
    # builds one array per row for HMGET rather than a flat key list for MGET.
    redis_client.hset("-1", "field1", "v1")

    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_negative_key")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_hash_negative_key (key1 Int64, key2 String, value String)
        PRIMARY KEY key1, key2
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'hash_map'))
        LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1000))
        LIFETIME(MIN 1 MAX 1)
        """
    )
    assert (
        node.query(
            "SELECT dictGet('test_redis_hash_negative_key', 'value', tuple(toInt64(-1), 'field1'))"
        )
        == "v1\n"
    )
    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_negative_key")

    node.query("DROP DICTIONARY IF EXISTS test_redis_simple_composite")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_simple_composite (key1 UInt64, key2 String, value String)
        PRIMARY KEY key1, key2
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'simple'))
        LAYOUT(COMPLEX_KEY_HASHED())
        LIFETIME(MIN 1 MAX 1)
        """
    )
    error = node.query_and_get_error(
        "SELECT dictGet('test_redis_simple_composite', 'value', tuple(toUInt64(1), 'a'))"
    )
    assert "single key column" in error, error
    assert "hash_map" in error, error
    node.query("DROP DICTIONARY IF EXISTS test_redis_simple_composite")

    # Unsupported: 'hash_map' storage requires exactly two key columns (one given).
    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_single")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_hash_single (key String, value String)
        PRIMARY KEY key
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'hash_map'))
        LAYOUT(COMPLEX_KEY_HASHED())
        LIFETIME(MIN 1 MAX 1)
        """
    )
    error = node.query_and_get_error(
        "SELECT dictGet('test_redis_hash_single', 'value', tuple('k1'))"
    )
    assert "requires 2 keys" in error, error
    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_single")

    # Unsupported: 'hash_map' storage requires exactly two key columns (three given).
    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_triple")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_hash_triple (key1 UInt64, key2 String, key3 UInt64, value String)
        PRIMARY KEY key1, key2, key3
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'hash_map'))
        LAYOUT(COMPLEX_KEY_HASHED())
        LIFETIME(MIN 1 MAX 1)
        """
    )
    error = node.query_and_get_error(
        "SELECT dictGet('test_redis_hash_triple', 'value', tuple(toUInt64(1), 'a', toUInt64(2)))"
    )
    assert "requires 2 keys" in error, error
    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_triple")

    # Unsupported: the Redis source only serializes native integers and strings as keys, so a
    # wide integer key column is refused when the source is constructed.
    #
    # `Int256` used to be admitted here and then silently miss every lookup: `ExternalResultDescription`
    # has a value type for it, so the source constructed, but the key was serialized through
    # `FieldVisitorToString`, which wraps big integers in quote characters, and `MGET` was sent
    # the six-character key `'123456'` instead of `123456`.
    # https://github.com/ClickHouse/ClickHouse/issues/117705
    redis_client.set(str(2**256 - 1), "v1")
    redis_client.set("123456", "v1")

    for key_type, key_expr in [
        ("Int256", "toInt256(123456)"),
        ("UInt256", f"toUInt256('{2**256 - 1}')"),
        ("Int128", "toInt128(123456)"),
        ("UInt128", "toUInt128(123456)"),
    ]:
        node.query("DROP DICTIONARY IF EXISTS test_redis_wide_int_key")
        node.query(
            f"""
            CREATE DICTIONARY test_redis_wide_int_key (key {key_type}, value String)
            PRIMARY KEY key
            SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'simple'))
            LAYOUT(COMPLEX_KEY_DIRECT())
            """
        )
        error = node.query_and_get_error(
            f"SELECT dictGet('test_redis_wide_int_key', 'value', tuple({key_expr}))"
        )
        assert "supports only native integer or string key" in error, error
        assert key_type in error, error
        node.query("DROP DICTIONARY IF EXISTS test_redis_wide_int_key")

    # 'hash_map' storage validates its key columns in a separate branch of the same screen,
    # so it gets its own case: a composite key with a wide integer column must be refused
    # there too, and the two branches must not drift apart.
    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_wide_int_key")
    node.query(
        f"""
        CREATE DICTIONARY test_redis_hash_wide_int_key (key1 Int256, key2 String, value String)
        PRIMARY KEY key1, key2
        SOURCE(REDIS(HOST '{host}' PORT 6379 PASSWORD 'clickhouse' DB_INDEX {DB_INDEX} STORAGE_TYPE 'hash_map'))
        LAYOUT(COMPLEX_KEY_HASHED())
        LIFETIME(MIN 1 MAX 1)
        """
    )
    error = node.query_and_get_error(
        "SELECT dictGet('test_redis_hash_wide_int_key', 'value', tuple(toInt256(123456), 'field1'))"
    )
    assert "supports only native integer or string key" in error, error
    assert "Int256" in error, error
    node.query("DROP DICTIONARY IF EXISTS test_redis_hash_wide_int_key")
