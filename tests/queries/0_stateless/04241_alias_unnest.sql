-- unnest is a PostgreSQL alias of arrayJoin (single-argument).
SELECT unnest([1, 2, 3]);
SELECT UNNEST(['a', 'b']);
SELECT sum(x) FROM (SELECT unnest([10, 20, 30]) AS x);

-- The alias resolves to the canonical arrayJoin name, so it reaches the trivial-count guard:
-- the row count of the table (3) is not the answer, with the optimization on or off.
DROP TABLE IF EXISTS t_04241;
CREATE TABLE t_04241 (A Array(UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04241 VALUES ([1, 2, 3]), ([4, 5]), ([6]);
SELECT count(unnest(A)) FROM t_04241 SETTINGS optimize_trivial_count_query = 1;
SELECT count(unnest(A)) FROM t_04241 SETTINGS optimize_trivial_count_query = 0;
DROP TABLE t_04241;
