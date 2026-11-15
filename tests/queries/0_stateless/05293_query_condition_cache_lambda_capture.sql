-- A lambda that captures a column must not reuse the query condition cache entry of a different lambda over the same columns.

SET use_query_condition_cache = 1;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (k UInt64, v UInt64, arr Array(UInt64), arr_next Array(UInt64)) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab SELECT number, number, [number, number + 1], [number + 1, number + 2] FROM numbers(10000);

-- Different bodies.
SELECT count() FROM tab WHERE arrayExists(x -> x < v, arr);
SELECT count() FROM tab WHERE arrayExists(x -> x > v, arr);

-- Same body, lambda arguments bound in a different order.
SELECT count() FROM tab WHERE arrayExists((y, x) -> x + v < y + v, arr, arr_next);
SELECT count() FROM tab WHERE arrayExists((x, y) -> x + v < y + v, arr, arr_next);

DROP TABLE tab;
