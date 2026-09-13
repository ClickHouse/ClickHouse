-- { echo }
-- A null map byte is a predicate, not a value: any non-zero byte means NULL. `if` forwards its raw
-- condition column as the null map, so `number % 3` fills it with the bytes 0, 1 and 2. Byte 0 marks
-- the 10 rows holding 'x'; the 1s and the 2s are the 20 rows that are equally NULL. `isNull` returns
-- 1 or 0, so each of those 20 rows must contribute exactly 1, on every evaluation path.
SELECT sum(isNull(e)), max(isNull(e)) FROM (SELECT if(number % 3, NULL, 'x') AS e FROM numbers(30));
-- The compiled path and the interpreted path must agree. A bare `isNull(e)` is a single node and is
-- not compiled, and `* 1` is folded away before compilation, so `+ 0` is what puts the function
-- inside a compiled expression; the argument needs a native type for the same reason.
SELECT sum(isNull(e) + 0) FROM (SELECT if(number % 3, NULL, toInt64(7)) AS e FROM numbers(30)) SETTINGS compile_expressions = 0, min_count_to_compile_expression = 0;
SELECT sum(isNull(e) + 0) FROM (SELECT if(number % 3, NULL, toInt64(7)) AS e FROM numbers(30)) SETTINGS compile_expressions = 1, min_count_to_compile_expression = 0;
-- A comparison against the result must not see a value other than 0 or 1.
SELECT arraySort(groupUniqArray(isNull(e) = 1)), sum(isNull(e) = 1) FROM (SELECT if(number % 3, NULL, 'x') AS e FROM numbers(30));
-- The `IS NULL` operator resolves to the same function.
SELECT sum(e IS NULL) FROM (SELECT if(number % 3, NULL, 'x') AS e FROM numbers(30));
-- An element of Array(Nullable(T)) is an ordinary Nullable by the time it reaches the function.
SELECT sum(isNull(x)) FROM (SELECT arrayJoin(a) AS x FROM (SELECT [if(number % 3, NULL, 'x')] AS a FROM numbers(30)));
-- Such a null map survives a write and a read back. `optimize_functions_to_subcolumns` is on by
-- default and reads the null map stream instead of calling the function, so both values of the
-- setting and both part types must answer alike.
CREATE TABLE t_isnull_compact (e Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0, min_bytes_for_wide_part = 1000000000;
CREATE TABLE t_isnull_wide (e Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0, min_bytes_for_wide_part = 0;
INSERT INTO t_isnull_compact SELECT if(number % 3, NULL, 'x') FROM numbers(30);
INSERT INTO t_isnull_wide SELECT if(number % 3, NULL, 'x') FROM numbers(30);
SELECT table, part_type, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table IN ('t_isnull_compact', 't_isnull_wide') AND active ORDER BY table;
SELECT sum(isNull(e)), max(isNull(e)) FROM t_isnull_compact SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(isNull(e)), max(isNull(e)) FROM t_isnull_compact SETTINGS optimize_functions_to_subcolumns = 1;
SELECT sum(isNull(e)), max(isNull(e)) FROM t_isnull_wide SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(isNull(e)), max(isNull(e)) FROM t_isnull_wide SETTINGS optimize_functions_to_subcolumns = 1;
-- Controls. `isNotNull`, `count` and a filter test nullness already, and the LowCardinality branch
-- of `isNull` builds its result from dictionary indexes rather than from a null map.
SELECT sum(isNotNull(e)), count(e), countIf(isNull(e)) FROM t_isnull_wide;
SELECT sum(isNull(toLowCardinality(e))), max(isNull(toLowCardinality(e))) FROM t_isnull_wide;
CREATE TABLE t_isnull_canon (e Nullable(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0;
INSERT INTO t_isnull_canon SELECT if(number % 2, NULL, 'x') FROM numbers(30);
SELECT sum(isNull(e)), max(isNull(e)), sum(isNotNull(e)), count(e) FROM t_isnull_canon SETTINGS optimize_functions_to_subcolumns = 0;
SELECT sum(isNull(e)), max(isNull(e)), sum(isNotNull(e)), count(e) FROM t_isnull_canon SETTINGS optimize_functions_to_subcolumns = 1;
-- The `null` subcolumn is a faithful view of the stored stream and keeps its bytes, so an index
-- built over it is still used and still answers on those bytes.
CREATE TABLE t_isnull_idx (e Nullable(String), INDEX idx e.null TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 1.0, index_granularity = 4;
INSERT INTO t_isnull_idx SELECT if(number % 3, NULL, 'x') FROM numbers(30);
SELECT sum(isNull(e)), max(isNull(e)) FROM t_isnull_idx;
SELECT count() FROM t_isnull_idx WHERE e.null != 0;
SELECT count() FROM t_isnull_idx WHERE e.null = 1 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx';
DROP TABLE t_isnull_compact;
DROP TABLE t_isnull_wide;
DROP TABLE t_isnull_canon;
DROP TABLE t_isnull_idx;
