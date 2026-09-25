#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings

# Compact with_key_columns must match basic / with_buckets lookup semantics.
# SELECT m / mapKeys / mapValues are compared after mapSort because
# with_key_columns returns keys in dictionary order (documented deviation).
# Duplicate keys are checked separately.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$data_path"

if $CLICKHOUSE_CLIENT -q "SELECT 1" >/dev/null 2>&1; then
    CH="$CLICKHOUSE_CLIENT"
    HAVE_SERVER=1
else
    CH="$CLICKHOUSE_LOCAL --path $data_path"
    HAVE_SERVER=0
fi

$CH -m -q "
SET optimize_on_insert = 0;

CREATE TABLE t_basic
(
    id UInt64,
    m_str Map(String, String),
    m_nstr Map(String, Nullable(String)),
    m_u64 Map(String, UInt64),
    m_arr Map(String, Array(String)),
    m_nested Map(String, Array(Array(Nullable(UInt8)))),
    m_tup Map(String, Tuple(UInt8, String)),
    m_map Map(String, Map(String, UInt8)),
    m_lc Map(String, LowCardinality(String)),
    m_lcn Map(String, LowCardinality(Nullable(String))),
    m_u64key Map(UInt64, String),
    m_fs Map(FixedString(3), UInt64),
    m_enum Map(Enum8('red' = 1, 'blue' = 2), UInt64)
)
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

CREATE TABLE t_key_columns AS t_basic
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

CREATE TABLE t_buckets AS t_basic
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

INSERT INTO t_basic VALUES
    (1,
        {'a.b': 'dot', 'a/b': 'slash', '': 'empty_key', 'plain': 'v'},
        {'present_null': NULL, 'present_empty': '', 'present_val': 'x'},
        {'a': 1, 'b': 2, 'zero': 0},
        {'a': ['x', 'y'], 'empty': []},
        {'a': [[1], [NULL], []]},
        {'a': (1, 't'), 'b': (0, '')},
        {'a': {'inner': 7}, 'b': {}},
        {'a': 'lc', 'b': ''},
        {'a': 'lc', 'n': NULL, 'e': ''},
        {1: 'one', 2: 'two'},
        {'abc': 1, 'xyz': 2},
        {'red': 10, 'blue': 20}),
    (2,
        {},
        {},
        {},
        {},
        {},
        {},
        {},
        {},
        {},
        {},
        {},
        {}),
    (3,
        {'plain': ''},
        {'present_empty': ''},
        {'zero': 0},
        {'empty': []},
        {'a': []},
        {'b': (0, '')},
        {'b': {}},
        {'b': ''},
        {'e': ''},
        {1: ''},
        {'abc': 0},
        {'red': 0});

INSERT INTO t_key_columns SELECT * FROM t_basic;
INSERT INTO t_buckets SELECT * FROM t_basic;

SELECT 'key_columns_part_type',
    (SELECT DISTINCT part_type FROM system.parts
     WHERE database = currentDatabase() AND table = 't_key_columns' AND active);

SELECT 'states_nstr',
    (SELECT count() FROM (
        SELECT id, m_nstr['absent'], m_nstr['present_null'], m_nstr['present_empty'], m_nstr['present_val'],
               toTypeName(m_nstr['absent']), mapContains(m_nstr, 'absent'), mapContains(m_nstr, 'present_null'),
               mapContains(m_nstr, 'present_empty'), mapContains(m_nstr, 'present_val'), mapContains(m_nstr, 'never')
        FROM t_basic
        EXCEPT ALL
        SELECT id, m_nstr['absent'], m_nstr['present_null'], m_nstr['present_empty'], m_nstr['present_val'],
               toTypeName(m_nstr['absent']), mapContains(m_nstr, 'absent'), mapContains(m_nstr, 'present_null'),
               mapContains(m_nstr, 'present_empty'), mapContains(m_nstr, 'present_val'), mapContains(m_nstr, 'never')
        FROM t_key_columns
    ));
SELECT 'states_nstr_vs_buckets',
    (SELECT count() FROM (
        SELECT id, m_nstr['absent'], m_nstr['present_null'], m_nstr['present_empty'], m_nstr['present_val'],
               toTypeName(m_nstr['absent']), mapContains(m_nstr, 'absent'), mapContains(m_nstr, 'present_null'),
               mapContains(m_nstr, 'present_empty'), mapContains(m_nstr, 'present_val'), mapContains(m_nstr, 'never')
        FROM t_buckets
        EXCEPT ALL
        SELECT id, m_nstr['absent'], m_nstr['present_null'], m_nstr['present_empty'], m_nstr['present_val'],
               toTypeName(m_nstr['absent']), mapContains(m_nstr, 'absent'), mapContains(m_nstr, 'present_null'),
               mapContains(m_nstr, 'present_empty'), mapContains(m_nstr, 'present_val'), mapContains(m_nstr, 'never')
        FROM t_key_columns
    ));

SELECT 'lookup_u64',
    (SELECT count() FROM (
        SELECT id, m_u64['a'], m_u64['b'], m_u64['zero'], m_u64['missing'], toTypeName(m_u64['a']),
               mapContains(m_u64, 'a'), mapContainsKey(m_u64, 'missing'),
               length(m_u64), m_u64.size0
        FROM t_basic
        EXCEPT ALL
        SELECT id, m_u64['a'], m_u64['b'], m_u64['zero'], m_u64['missing'], toTypeName(m_u64['a']),
               mapContains(m_u64, 'a'), mapContainsKey(m_u64, 'missing'),
               length(m_u64), m_u64.size0
        FROM t_key_columns
    ));
SELECT 'lookup_u64_vs_buckets',
    (SELECT count() FROM (
        SELECT id, m_u64['a'], m_u64['b'], m_u64['zero'], m_u64['missing'], toTypeName(m_u64['a']),
               mapContains(m_u64, 'a'), mapContainsKey(m_u64, 'missing'),
               length(m_u64), m_u64.size0
        FROM t_buckets
        EXCEPT ALL
        SELECT id, m_u64['a'], m_u64['b'], m_u64['zero'], m_u64['missing'], toTypeName(m_u64['a']),
               mapContains(m_u64, 'a'), mapContainsKey(m_u64, 'missing'),
               length(m_u64), m_u64.size0
        FROM t_key_columns
    ));

SELECT 'select_m_sorted',
    (SELECT count() FROM (
        SELECT id, mapSort(m_str), mapSort(m_nstr), mapSort(m_u64), mapSort(m_arr), mapSort(m_nested),
               mapSort(m_tup), mapSort(m_map), mapSort(m_lc), mapSort(m_lcn), mapSort(m_u64key),
               mapSort(m_fs), mapSort(m_enum)
        FROM t_basic
        EXCEPT ALL
        SELECT id, mapSort(m_str), mapSort(m_nstr), mapSort(m_u64), mapSort(m_arr), mapSort(m_nested),
               mapSort(m_tup), mapSort(m_map), mapSort(m_lc), mapSort(m_lcn), mapSort(m_u64key),
               mapSort(m_fs), mapSort(m_enum)
        FROM t_key_columns
    ));
SELECT 'select_m_sorted_vs_buckets',
    (SELECT count() FROM (
        SELECT id, mapSort(m_str), mapSort(m_nstr), mapSort(m_u64), mapSort(m_arr), mapSort(m_nested),
               mapSort(m_tup), mapSort(m_map), mapSort(m_lc), mapSort(m_lcn), mapSort(m_u64key),
               mapSort(m_fs), mapSort(m_enum)
        FROM t_buckets
        EXCEPT ALL
        SELECT id, mapSort(m_str), mapSort(m_nstr), mapSort(m_u64), mapSort(m_arr), mapSort(m_nested),
               mapSort(m_tup), mapSort(m_map), mapSort(m_lc), mapSort(m_lcn), mapSort(m_u64key),
               mapSort(m_fs), mapSort(m_enum)
        FROM t_key_columns
    ));

SELECT 'keys_values',
    (SELECT count() FROM (
        SELECT id, arraySort(mapKeys(m_u64)), mapValues(mapSort(m_u64)), arraySort(mapKeys(m_str))
        FROM t_basic
        EXCEPT ALL
        SELECT id, arraySort(mapKeys(m_u64)), mapValues(mapSort(m_u64)), arraySort(mapKeys(m_str))
        FROM t_key_columns
    ));
SELECT 'keys_values_vs_buckets',
    (SELECT count() FROM (
        SELECT id, arraySort(mapKeys(m_u64)), mapValues(mapSort(m_u64)), arraySort(mapKeys(m_str))
        FROM t_buckets
        EXCEPT ALL
        SELECT id, arraySort(mapKeys(m_u64)), mapValues(mapSort(m_u64)), arraySort(mapKeys(m_str))
        FROM t_key_columns
    ));

SELECT 'map_functions',
    (SELECT count() FROM (
        SELECT id,
               mapSort(mapFilter((k, v) -> v > 0, m_u64)),
               mapSort(mapApply((k, v) -> (k, v + 1), m_u64)),
               mapSort(mapUpdate(m_u64, map('a', toUInt64(9)))),
               mapSort(mapConcat(m_u64, map('z', toUInt64(1)))),
               mapSort(m_u64),
               mapExists((k, v) -> v = 1, m_u64),
               mapAll((k, v) -> v >= 0, m_u64),
               mapSort(mapExtractKeyLike(m_str, 'a%')),
               mapContainsKeyLike(m_str, 'a%')
        FROM t_basic
        EXCEPT ALL
        SELECT id,
               mapSort(mapFilter((k, v) -> v > 0, m_u64)),
               mapSort(mapApply((k, v) -> (k, v + 1), m_u64)),
               mapSort(mapUpdate(m_u64, map('a', toUInt64(9)))),
               mapSort(mapConcat(m_u64, map('z', toUInt64(1)))),
               mapSort(m_u64),
               mapExists((k, v) -> v = 1, m_u64),
               mapAll((k, v) -> v >= 0, m_u64),
               mapSort(mapExtractKeyLike(m_str, 'a%')),
               mapContainsKeyLike(m_str, 'a%')
        FROM t_key_columns
    ));
SELECT 'map_functions_vs_buckets',
    (SELECT count() FROM (
        SELECT id,
               mapSort(mapFilter((k, v) -> v > 0, m_u64)),
               mapSort(mapApply((k, v) -> (k, v + 1), m_u64)),
               mapSort(mapUpdate(m_u64, map('a', toUInt64(9)))),
               mapSort(mapConcat(m_u64, map('z', toUInt64(1)))),
               mapSort(m_u64),
               mapExists((k, v) -> v = 1, m_u64),
               mapAll((k, v) -> v >= 0, m_u64),
               mapSort(mapExtractKeyLike(m_str, 'a%')),
               mapContainsKeyLike(m_str, 'a%')
        FROM t_buckets
        EXCEPT ALL
        SELECT id,
               mapSort(mapFilter((k, v) -> v > 0, m_u64)),
               mapSort(mapApply((k, v) -> (k, v + 1), m_u64)),
               mapSort(mapUpdate(m_u64, map('a', toUInt64(9)))),
               mapSort(mapConcat(m_u64, map('z', toUInt64(1)))),
               mapSort(m_u64),
               mapExists((k, v) -> v = 1, m_u64),
               mapAll((k, v) -> v >= 0, m_u64),
               mapSort(mapExtractKeyLike(m_str, 'a%')),
               mapContainsKeyLike(m_str, 'a%')
        FROM t_key_columns
    ));

SELECT 'escaped_and_typed_keys',
    (SELECT count() FROM (
        SELECT id, m_str['a.b'], m_str['a/b'], m_str[''], m_str['plain'],
               m_u64key[1], m_u64key[9], m_fs['abc'], m_enum['red'], m_enum[2],
               toTypeName(m_arr['a']), toTypeName(m_tup['a']), toTypeName(m_map['a']),
               toTypeName(m_lc['a']), toTypeName(m_lcn['n']), toTypeName(m_nested['a'])
        FROM t_basic
        EXCEPT ALL
        SELECT id, m_str['a.b'], m_str['a/b'], m_str[''], m_str['plain'],
               m_u64key[1], m_u64key[9], m_fs['abc'], m_enum['red'], m_enum[2],
               toTypeName(m_arr['a']), toTypeName(m_tup['a']), toTypeName(m_map['a']),
               toTypeName(m_lc['a']), toTypeName(m_lcn['n']), toTypeName(m_nested['a'])
        FROM t_key_columns
    ));
SELECT 'escaped_and_typed_keys_vs_buckets',
    (SELECT count() FROM (
        SELECT id, m_str['a.b'], m_str['a/b'], m_str[''], m_str['plain'],
               m_u64key[1], m_u64key[9], m_fs['abc'], m_enum['red'], m_enum[2],
               toTypeName(m_arr['a']), toTypeName(m_tup['a']), toTypeName(m_map['a']),
               toTypeName(m_lc['a']), toTypeName(m_lcn['n']), toTypeName(m_nested['a'])
        FROM t_buckets
        EXCEPT ALL
        SELECT id, m_str['a.b'], m_str['a/b'], m_str[''], m_str['plain'],
               m_u64key[1], m_u64key[9], m_fs['abc'], m_enum['red'], m_enum[2],
               toTypeName(m_arr['a']), toTypeName(m_tup['a']), toTypeName(m_map['a']),
               toTypeName(m_lc['a']), toTypeName(m_lcn['n']), toTypeName(m_nested['a'])
        FROM t_key_columns
    ));

SELECT 'prewhere_where',
    (SELECT count() FROM (
        SELECT id FROM t_basic PREWHERE mapContains(m_u64, 'a') WHERE m_u64['a'] = 1
        EXCEPT ALL
        SELECT id FROM t_key_columns PREWHERE mapContains(m_u64, 'a') WHERE m_u64['a'] = 1
    ));
SELECT 'prewhere_where_vs_buckets',
    (SELECT count() FROM (
        SELECT id FROM t_buckets PREWHERE mapContains(m_u64, 'a') WHERE m_u64['a'] = 1
        EXCEPT ALL
        SELECT id FROM t_key_columns PREWHERE mapContains(m_u64, 'a') WHERE m_u64['a'] = 1
    ));

SELECT 'group_by',
    (SELECT count() FROM (
        SELECT m_u64['a'] AS k, count() AS c FROM t_basic GROUP BY k
        EXCEPT ALL
        SELECT m_u64['a'] AS k, count() AS c FROM t_key_columns GROUP BY k
    ));
SELECT 'group_by_vs_buckets',
    (SELECT count() FROM (
        SELECT m_u64['a'] AS k, count() AS c FROM t_buckets GROUP BY k
        EXCEPT ALL
        SELECT m_u64['a'] AS k, count() AS c FROM t_key_columns GROUP BY k
    ));
"

$CH -m -q "
SET optimize_on_insert = 0;

CREATE TABLE t_dup_basic (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

CREATE TABLE t_dup_key_columns (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

CREATE TABLE t_dup_buckets (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'with_buckets',
    max_buckets_in_map = 4,
    map_buckets_strategy = 'constant',
    map_buckets_min_avg_size = 0,
    serialization_info_version = 'with_types',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;

INSERT INTO t_dup_basic VALUES (1, map('k', 1, 'k', 2, 'x', 3));
INSERT INTO t_dup_key_columns VALUES (1, map('k', 1, 'k', 2, 'x', 3));
INSERT INTO t_dup_buckets VALUES (1, map('k', 1, 'k', 2, 'x', 3));

SELECT 'dup_lookup', (SELECT m['k'] FROM t_dup_basic), (SELECT m['k'] FROM t_dup_key_columns), (SELECT m['k'] FROM t_dup_buckets);
SELECT 'dup_length_basic', length(m) FROM t_dup_basic;
SELECT 'dup_length_key_columns', length(m) FROM t_dup_key_columns;
SELECT 'dup_length_buckets', length(m) FROM t_dup_buckets;
SELECT 'dup_map_basic', m FROM t_dup_basic;
SELECT 'dup_map_key_columns', m FROM t_dup_key_columns;
SELECT 'dup_map_buckets', m FROM t_dup_buckets;
"

if [[ "$HAVE_SERVER" == "1" ]]; then
    $CH -m -q "
    SELECT 'remote',
        (SELECT count() FROM (
            SELECT id, mapSort(m_u64) FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), t_basic)
            EXCEPT ALL
            SELECT id, mapSort(m_u64) FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), t_key_columns)
        ));
    SELECT 'remote_vs_buckets',
        (SELECT count() FROM (
            SELECT id, mapSort(m_u64) FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), t_buckets)
            EXCEPT ALL
            SELECT id, mapSort(m_u64) FROM remote('127.0.0.1:${CLICKHOUSE_PORT_TCP}', currentDatabase(), t_key_columns)
        ));
    "
else
    $CH -q "SELECT 'remote', 0"
    $CH -q "SELECT 'remote_vs_buckets', 0"
fi

$CH -q "DROP TABLE t_basic; DROP TABLE t_key_columns; DROP TABLE t_buckets; DROP TABLE t_dup_basic; DROP TABLE t_dup_key_columns; DROP TABLE t_dup_buckets;"
rm -rf "${data_path:?}"
