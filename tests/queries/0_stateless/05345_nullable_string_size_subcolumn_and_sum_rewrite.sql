-- The optimizations that were applied only to non-Nullable columns are applied to Nullable columns as well:
-- `length`, `empty`, `notEmpty` and comparisons with an empty string of `Nullable(String)` are read from the `size` subcolumn,
-- and `sum(x + c)` is rewritten to `sum(x) + c * count(x)` for Nullable `x`.
-- The results must be the same as without the optimizations.

SET optimize_functions_to_subcolumns = 1, optimize_empty_string_comparisons = 1, optimize_arithmetic_operations_in_aggregate_functions = 1;

DROP TABLE IF EXISTS t_nullable_string;

CREATE TABLE t_nullable_string (id UInt64, s Nullable(String), x Nullable(Int32))
ENGINE = MergeTree ORDER BY id SETTINGS string_serialization_version = 'with_size_stream';

INSERT INTO t_nullable_string SELECT number, multiIf(number % 5 = 0, NULL, number % 5 = 1, '', toString(number)), if(number % 4 = 0, NULL, toInt32(number) - 50) FROM numbers(100);

SELECT 'string functions';
SELECT countIf(s = ''), countIf(s != ''), countIf(empty(s)), countIf(notEmpty(s)), sum(length(s)), countIf(length(s) IS NULL), countIf((s = '') IS NULL), countIf(empty(s) IS NULL) FROM t_nullable_string;
SELECT countIf(s = ''), countIf(s != ''), countIf(empty(s)), countIf(notEmpty(s)), sum(length(s)), countIf(length(s) IS NULL), countIf((s = '') IS NULL), countIf(empty(s) IS NULL) FROM t_nullable_string
SETTINGS optimize_functions_to_subcolumns = 0, optimize_empty_string_comparisons = 0;
SELECT id % 3 AS k, avg(length(s)), count() FROM t_nullable_string WHERE s != '' GROUP BY k ORDER BY k;
SELECT id % 3 AS k, avg(length(s)), count() FROM t_nullable_string WHERE s != '' GROUP BY k ORDER BY k
SETTINGS optimize_functions_to_subcolumns = 0, optimize_empty_string_comparisons = 0;
SELECT toTypeName(length(s)), toTypeName(empty(s)), toTypeName(s = '') FROM t_nullable_string LIMIT 1;

SELECT 'the size subcolumn is read';
SELECT replaceAll(explain, currentDatabase(), 'db') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT length(s), empty(s), notEmpty(s) FROM t_nullable_string WHERE s != '');

SELECT 'sum of an expression with a constant';
SELECT sum(x + 1), sum(x - 1), sum(1 + x), sum(10 - x), toTypeName(sum(x + 1)), toTypeName(sum(x - 1)) FROM t_nullable_string;
SELECT sum(x + 1), sum(x - 1), sum(1 + x), sum(10 - x), toTypeName(sum(x + 1)), toTypeName(sum(x - 1)) FROM t_nullable_string
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
-- All values in the group are NULL.
SELECT id % 2 AS k, sum(x + 1), sum(x) FROM t_nullable_string WHERE x IS NULL GROUP BY k ORDER BY k;
SELECT id % 2 AS k, sum(x + 1), sum(x) FROM t_nullable_string WHERE x IS NULL GROUP BY k ORDER BY k
SETTINGS optimize_arithmetic_operations_in_aggregate_functions = 0;
SELECT replaceAll(explain, currentDatabase(), 'db') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT sum(x + 1) FROM t_nullable_string);

DROP TABLE t_nullable_string;

SELECT 'empty and notEmpty of a Nullable key use the primary key';
DROP TABLE IF EXISTS t_nullable_string_key;
CREATE TABLE t_nullable_string_key (s Nullable(String)) ENGINE = MergeTree ORDER BY s SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_nullable_string_key VALUES (''), ('a'), ('b'), (NULL);
SELECT s FROM t_nullable_string_key WHERE s != '' ORDER BY s SETTINGS force_primary_key = 1;
SELECT s FROM t_nullable_string_key WHERE notEmpty(s) ORDER BY s SETTINGS force_primary_key = 1;
SELECT count() FROM t_nullable_string_key WHERE empty(s) SETTINGS force_primary_key = 1;
DROP TABLE t_nullable_string_key;

SELECT 'uniqExact over Nullable';
SELECT uniqExact(x), count(x) FROM (SELECT if(number % 7 = 0, NULL, number) AS x FROM numbers_mt(1000000)) SETTINGS max_threads = 8;

SELECT 'comparison of a Nullable(FixedString) key with an empty string uses the primary key';
DROP TABLE IF EXISTS t_nullable_fixed_string_key;
CREATE TABLE t_nullable_fixed_string_key (s Nullable(FixedString(1))) ENGINE = MergeTree ORDER BY s SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_nullable_fixed_string_key VALUES ('a'), ('b'), (NULL);
SELECT s FROM t_nullable_fixed_string_key WHERE s != '' ORDER BY s SETTINGS force_primary_key = 1;
DROP TABLE t_nullable_fixed_string_key;
