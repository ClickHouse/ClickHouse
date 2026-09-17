#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The experimental `cuckoo_filter` and `binary_fuse_filter` skip indexes must not prune a granule that
# the query would match. Like `bloom_filter`, they store the hash of each value's exact bytes, while the
# runtime functions compare differently in several string-like cases:
#   * `=` compares a `String` with a `FixedString` constant zero-padded;
#   * `has`, `hasAny`, `hasAll`, `mapContainsKey` first coerce a `FixedString` constant to the element type
#     of a `LowCardinality` / `FixedString` array (strip the padding, then re-pad to the element width);
#   * `IN` with a set holding an empty array: the empty array has no element hash that could stand for it.
# Every count is compared with the same query under `use_skip_indexes = 0`; both must agree.

for index_type in cuckoo_filter binary_fuse_filter
do
    echo "--- ${index_type}"
    ${CLICKHOUSE_CLIENT} --allow_experimental_cuckoo_filter_index=1 --allow_experimental_binary_fuse_filter_index=1 -n --query "
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_fs3;
DROP TABLE IF EXISTS t_arr_lc;
DROP TABLE IF EXISTS t_arr_fs;
DROP TABLE IF EXISTS t_map_lc;
DROP TABLE IF EXISTS t_map_val;
DROP TABLE IF EXISTS t_arr_in;

CREATE TABLE t_str (id UInt64, v String, INDEX idx v TYPE ${index_type} GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_fs3 (id UInt64, v FixedString(3), INDEX idx v TYPE ${index_type} GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_arr_lc (id UInt64, a Array(LowCardinality(String)), INDEX idx a TYPE ${index_type} GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_arr_fs (id UInt64, a Array(FixedString(3)), INDEX idx a TYPE ${index_type} GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_map_lc (id UInt64, m Map(LowCardinality(String), UInt8), INDEX idx mapKeys(m) TYPE ${index_type} GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_map_val (id UInt64, m Map(String, LowCardinality(String)), INDEX idx mapValues(m) TYPE ${index_type} GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
CREATE TABLE t_arr_in (id UInt64, a Array(String), INDEX idx a TYPE ${index_type} GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_str VALUES (0, 'V0'), (1, 'V0\\0'), (2, 'V0X'), (3, 'X');
INSERT INTO t_fs3 VALUES (0, 'V0'), (1, 'V0X'), (2, 'X');
INSERT INTO t_arr_lc VALUES (0, ['a']), (1, ['a\\0']), (2, ['b']), (3, []);
INSERT INTO t_arr_fs VALUES (0, ['ab']), (1, ['abc']), (2, ['x']);
INSERT INTO t_map_lc VALUES (0, map('a', 1)), (1, map('b', 2)), (2, map());
INSERT INTO t_map_val VALUES (0, map('k', 'a')), (1, map('k', 'b')), (2, map());
INSERT INTO t_arr_in VALUES (0, []), (1, ['x']), (2, ['y']), (3, ['x', 'y']);

-- String index, FixedString constant: equality zero-pads, so the index cannot be used.
SELECT 'String = FixedString', count(), (SELECT count() FROM t_str WHERE v = toFixedString('V0', 3) SETTINGS use_skip_indexes = 0) FROM t_str WHERE v = toFixedString('V0', 3);
SELECT count() FROM t_str WHERE v = toFixedString('V0', 3) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
-- FixedString(3) index, narrower String constant: the padded form is the only value that can match.
SELECT 'FixedString(3) = String', count(), (SELECT count() FROM t_fs3 WHERE v = 'V0' SETTINGS use_skip_indexes = 0) FROM t_fs3 WHERE v = 'V0' SETTINGS force_data_skipping_indices = 'idx';
-- FixedString(3) index, wider constant: nothing can match, the index is declined.
SELECT 'FixedString(3) = wider', count(), (SELECT count() FROM t_fs3 WHERE v = 'V0XY' SETTINGS use_skip_indexes = 0) FROM t_fs3 WHERE v = 'V0XY';

-- Array(LowCardinality(String)) with a padded FixedString constant: the search functions strip the padding.
SELECT 'has LC', count(), (SELECT count() FROM t_arr_lc WHERE has(a, toFixedString('a', 2)) SETTINGS use_skip_indexes = 0) FROM t_arr_lc WHERE has(a, toFixedString('a', 2)) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'hasAny LC', count(), (SELECT count() FROM t_arr_lc WHERE hasAny(a, [toFixedString('a', 2)]) SETTINGS use_skip_indexes = 0) FROM t_arr_lc WHERE hasAny(a, [toFixedString('a', 2)]) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'hasAll LC', count(), (SELECT count() FROM t_arr_lc WHERE hasAll(a, [toFixedString('a', 2)]) SETTINGS use_skip_indexes = 0) FROM t_arr_lc WHERE hasAll(a, [toFixedString('a', 2)]) SETTINGS force_data_skipping_indices = 'idx';

-- Array(FixedString(3)) with a String constant: the constant is re-padded to the element width.
SELECT 'has FS3', count(), (SELECT count() FROM t_arr_fs WHERE has(a, 'ab') SETTINGS use_skip_indexes = 0) FROM t_arr_fs WHERE has(a, 'ab') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'hasAny FS3', count(), (SELECT count() FROM t_arr_fs WHERE hasAny(a, ['ab', 'zz']) SETTINGS use_skip_indexes = 0) FROM t_arr_fs WHERE hasAny(a, ['ab', 'zz']) SETTINGS force_data_skipping_indices = 'idx';
-- A constant wider than the element can match nothing; the index is declined instead of throwing.
SELECT 'has FS3 wider', count(), (SELECT count() FROM t_arr_fs WHERE has(a, 'abcd') SETTINGS use_skip_indexes = 0) FROM t_arr_fs WHERE has(a, 'abcd');

-- Map(LowCardinality(String), UInt8) keys with a padded FixedString constant.
SELECT 'mapContainsKey LC', count(), (SELECT count() FROM t_map_lc WHERE mapContainsKey(m, toFixedString('a', 2)) SETTINGS use_skip_indexes = 0) FROM t_map_lc WHERE mapContainsKey(m, toFixedString('a', 2)) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'mapContains LC', count(), (SELECT count() FROM t_map_lc WHERE mapContains(m, toFixedString('a', 2)) SETTINGS use_skip_indexes = 0) FROM t_map_lc WHERE mapContains(m, toFixedString('a', 2)) SETTINGS force_data_skipping_indices = 'idx';
-- Map(String, LowCardinality(String)) values with a padded FixedString constant.
SELECT 'mapContainsValue LC', count(), (SELECT count() FROM t_map_val WHERE mapContainsValue(m, toFixedString('a', 2)) SETTINGS use_skip_indexes = 0) FROM t_map_val WHERE mapContainsValue(m, toFixedString('a', 2)) SETTINGS force_data_skipping_indices = 'idx';

-- IN with a set that holds an empty array: the granule holding only [] must not be pruned.
SELECT 'IN with empty array', count(), (SELECT count() FROM t_arr_in WHERE a IN [[], ['x']] SETTINGS use_skip_indexes = 0) FROM t_arr_in WHERE a IN [[], ['x']];
SELECT count() FROM t_arr_in WHERE a IN [[], ['x']] SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT 'IN without empty array', count(), (SELECT count() FROM t_arr_in WHERE a IN [['x'], ['y']] SETTINGS use_skip_indexes = 0) FROM t_arr_in WHERE a IN [['x'], ['y']] SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE t_str;
DROP TABLE t_fs3;
DROP TABLE t_arr_lc;
DROP TABLE t_arr_fs;
DROP TABLE t_map_lc;
DROP TABLE t_map_val;
DROP TABLE t_arr_in;
"
done
