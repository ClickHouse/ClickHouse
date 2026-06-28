-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary source tables are not available on parallel-replica workers.

-- https://github.com/ClickHouse/ClickHouse/issues/104511
-- dictGetOrDefault should not throw CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN when
-- the default argument is a Nullable expression and short-circuit evaluation is enabled.
-- clearMaskedNullsBeforeCast handles: Nullable(T), Tuple(Nullable(T)),
-- Nullable(Tuple(T)), Nullable(Tuple(Nullable(T))).

SET short_circuit_function_evaluation = 'enable';

DROP TABLE IF EXISTS test_dict_nullable_default_src;
DROP TABLE IF EXISTS test_dict_nullable_default_keys;
DROP DICTIONARY IF EXISTS test_dict_nullable_default_single;
DROP DICTIONARY IF EXISTS test_dict_nullable_default_tuple;

CREATE TABLE test_dict_nullable_default_src (id UInt64, a String, b UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_dict_nullable_default_src VALUES (0, 'a0', 10), (1, 'a1', 11);

CREATE TABLE test_dict_nullable_default_keys (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO test_dict_nullable_default_keys VALUES (0), (1), (99), (100);

CREATE DICTIONARY test_dict_nullable_default_single
(
    `id` UInt64,
    `a`  String DEFAULT ''
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_dict_nullable_default_src'))
LAYOUT(DIRECT());

CREATE DICTIONARY test_dict_nullable_default_tuple
(
    `id` UInt64,
    `a`  String DEFAULT '',
    `b`  UInt32 DEFAULT 0
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'test_dict_nullable_default_src'))
LAYOUT(HASHED())
LIFETIME(0);

-- Nullable(String) -> String, single attribute with mixed found/not-found keys.
SELECT dictGetOrDefault('test_dict_nullable_default_single', 'a', x,
    toNullable(concat('fb_', toString(x)))) AS v
FROM test_dict_nullable_default_keys;
SELECT toTypeName(dictGetOrDefault('test_dict_nullable_default_single', 'a', toUInt64(0),
    toNullable(toString(x)))) FROM test_dict_nullable_default_keys LIMIT 1;

-- Tuple(Nullable(String), Nullable(UInt32)) -> Tuple(String, UInt32).
SELECT dictGetOrDefault('test_dict_nullable_default_tuple', ('a', 'b'), x,
    (toNullable(concat('fb_', toString(x))), toNullable(toUInt32(x + 1000)))) AS v
FROM test_dict_nullable_default_keys;
SELECT toTypeName(dictGetOrDefault('test_dict_nullable_default_tuple', ('a', 'b'), toUInt64(0),
    (toNullable(toString(x)), toNullable(toUInt32(x))))) FROM test_dict_nullable_default_keys LIMIT 1;

-- Nullable(Tuple(String, UInt32)) -> Tuple(String, UInt32).
SELECT dictGetOrDefault('test_dict_nullable_default_tuple', ('a', 'b'), x,
    toNullable((concat('fb_', toString(x)), toUInt32(x + 1000)))) AS v
FROM test_dict_nullable_default_keys;
SELECT toTypeName(dictGetOrDefault('test_dict_nullable_default_tuple', ('a', 'b'), toUInt64(0),
    toNullable((toString(x), toUInt32(x))))) FROM test_dict_nullable_default_keys LIMIT 1;

-- Nullable(Tuple(Nullable(String), Nullable(UInt32))) -> Tuple(String, UInt32).
SELECT dictGetOrDefault('test_dict_nullable_default_tuple', ('a', 'b'), x,
    toNullable((toNullable(concat('fb_', toString(x))), toNullable(toUInt32(x + 1000))))) AS v
FROM test_dict_nullable_default_keys;
SELECT toTypeName(dictGetOrDefault('test_dict_nullable_default_tuple', ('a', 'b'), toUInt64(0),
    toNullable((toNullable(toString(x)), toNullable(toUInt32(x)))))) FROM test_dict_nullable_default_keys LIMIT 1;

-- Negative: a default that genuinely evaluates to NULL on a not-found row must still fail.
SELECT dictGetOrDefault('test_dict_nullable_default_single', 'a', x,
    materialize(NULL::Nullable(String)))
FROM test_dict_nullable_default_keys; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

DROP DICTIONARY IF EXISTS test_dict_nullable_default_single;
DROP DICTIONARY IF EXISTS test_dict_nullable_default_tuple;
DROP TABLE IF EXISTS test_dict_nullable_default_keys;
DROP TABLE IF EXISTS test_dict_nullable_default_src;
