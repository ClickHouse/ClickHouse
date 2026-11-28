-- Tags: no-parallel
-- no-parallel: the query condition cache is server-wide and this test drops it.

-- The query condition cache must not share an entry between filters that differ only in
-- `cast_fixed_string_to_string_strip_trailing_zeros`, because the setting changes the value of the
-- conversion of `FixedString` to `String`: a granule verdict "no matching rows" written with one value
-- of the setting must not be served to a query with the other one.

DROP TABLE IF EXISTS t_qcc_fixed_string;
CREATE TABLE t_qcc_fixed_string (id UInt64, f FixedString(2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_qcc_fixed_string SELECT number, 'a' FROM numbers(20000);

SELECT 'toString';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_fixed_string WHERE toString(f) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_qcc_fixed_string WHERE toString(f) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

-- Three branches, so that `multiIf` is not rewritten to `if`.
SELECT 'multiIf';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_fixed_string WHERE multiIf(id = 100000, 'b', id = 100001, 'c', f) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_qcc_fixed_string WHERE multiIf(id = 100000, 'b', id = 100001, 'c', f) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT 'transform';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM t_qcc_fixed_string WHERE transform(f, ['b'], ['b'], f) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_qcc_fixed_string WHERE transform(f, ['b'], ['b'], f) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;

DROP TABLE t_qcc_fixed_string;
