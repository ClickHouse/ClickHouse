-- `optimize_rewrite_array_exists_like_to_or` rewrites `arrayExists(x -> haystack LIKE x, constant_array)`,
-- which is what `haystack LIKE SOME([...])` and `haystack ILIKE SOME([...])` are parsed into,
-- to an `OR` chain of `LIKE` with constant patterns.

DROP TABLE IF EXISTS t_array_exists_like;

CREATE TABLE t_array_exists_like
(
    s String,
    ns Nullable(String),
    lcs LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY s;

INSERT INTO t_array_exists_like VALUES
    ('JFK Airport', 'JFK Airport', 'JFK Airport'),
    ('Midtown Center', NULL, 'Midtown Center'),
    ('Times Sq/Theatre District', 'Times Sq/Theatre District', 'Times Sq/Theatre District');

SELECT '-- disabled by default';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME(['%airport%', '%times sq%']);

SET optimize_rewrite_array_exists_like_to_or = 1;

SELECT '-- rewritten';
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME(['%airport%', '%times sq%']);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s LIKE SOME(['%Airport%', '%Times%']);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME(['%airport%']);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME([lower('%AIRPORT%'), '%times sq%']);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE lcs ILIKE SOME(['%airport%', '%times sq%']);
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT arrayExists(x -> s ILIKE x, ['%airport%', '%times sq%']) FROM t_array_exists_like;

SELECT '-- not rewritten';
-- `like` on a `Nullable` haystack returns `Nullable(UInt8)`, which is not the result type of `arrayExists`
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE ns ILIKE SOME(['%airport%', '%times sq%']);
-- a `NULL` pattern
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME(['%airport%', NULL]);
-- the patterns depend on the row
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME([s, '%times sq%']);
-- no patterns
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME(CAST([] AS Array(String)));
-- the lambda variable is the haystack, not the pattern
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT arrayExists(x -> x ILIKE '%a%', ['JFK Airport']);
-- the haystack uses the lambda variable
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT arrayExists(x -> concat(x, '!') ILIKE x, ['%a%']);
-- `ALL` is parsed into `arrayAll`
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t_array_exists_like WHERE s ILIKE ALL(['%a%', '%t%']);

SELECT '-- same results as the OR chain, with the rewrite enabled and disabled';
SELECT s, s ILIKE SOME(['%airport%', '%times sq%']) FROM t_array_exists_like ORDER BY s SETTINGS optimize_rewrite_array_exists_like_to_or = 0;
SELECT s, s ILIKE SOME(['%airport%', '%times sq%']) FROM t_array_exists_like ORDER BY s SETTINGS optimize_rewrite_array_exists_like_to_or = 1;
SELECT s, s ILIKE '%airport%' OR s ILIKE '%times sq%' FROM t_array_exists_like ORDER BY s;
SELECT count() FROM t_array_exists_like WHERE s LIKE SOME(['%Airport%', '%Times%']);
SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME(['%airport%']);
SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME([lower('%AIRPORT%'), '%times sq%']);
SELECT count() FROM t_array_exists_like WHERE lcs ILIKE SOME(['%airport%', '%times sq%']);
SELECT count() FROM t_array_exists_like WHERE ns ILIKE SOME(['%airport%', '%times sq%']);
SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME(['%airport%', NULL]);
SELECT count() FROM t_array_exists_like WHERE s ILIKE SOME([s, '%times sq%']);

DROP TABLE t_array_exists_like;
