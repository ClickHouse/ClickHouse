-- Tags: distributed

-- `ORDER BY ALL` expands to the whole projection, so the constant `3` reaches the shard query as a
-- bare literal. The local replica of a `remote` read inside a view, and a `Merge` child re-analyzed
-- from its AST, must keep it a constant: the shard query is printed from a resolved query tree.

DROP TABLE IF EXISTS t_pos;
DROP TABLE IF EXISTS t_pos_m1;
DROP TABLE IF EXISTS t_pos_d;
DROP TABLE IF EXISTS t_pos_merge;
DROP VIEW IF EXISTS v_pos;

CREATE TABLE t_pos (k Int64, src Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pos SELECT number, 4 - number FROM numbers(5);

SELECT 'order';
SELECT * FROM view(SELECT 3, k, src FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL DESC)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;

SELECT 'pruned projection';
SELECT k FROM view(SELECT 3, k, src FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL DESC)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;

SELECT 'distinct';
SELECT k, src FROM view(SELECT DISTINCT 3, k, src FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL DESC)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, optimize_distinct_in_order = 1;

SELECT 'constant out of projection range';
SELECT * FROM view(SELECT 5, k FROM remote('127.0.0.{1,2}', currentDatabase(), t_pos) ORDER BY ALL)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;

-- The body of a view read through the local replica is still resolved positionally, also when it is inlined.
CREATE VIEW v_pos AS SELECT src % 2 AS s, count() AS c FROM t_pos GROUP BY 1;
SELECT 'view body';
SELECT s, c FROM view(SELECT s, c FROM remote('127.0.0.{1,2}', currentDatabase(), v_pos) ORDER BY s)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1;
SELECT 'inlined view body';
SELECT s, c FROM view(SELECT s, c FROM remote('127.0.0.{1,2}', currentDatabase(), v_pos) ORDER BY s)
SETTINGS prefer_localhost_replica = 1, enable_positional_arguments = 1, analyzer_inline_views = 1;

CREATE TABLE t_pos_m1 (k Int64, src Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pos_m1 SELECT number, 4 - number FROM numbers(5);
CREATE TABLE t_pos_d AS t_pos ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_pos);
CREATE TABLE t_pos_merge AS t_pos ENGINE = Merge(currentDatabase(), '^(t_pos_m1|t_pos_d)$');

SELECT 'merge';
SELECT * FROM (SELECT 3, k, src FROM t_pos_merge ORDER BY ALL DESC)
SETTINGS enable_positional_arguments = 1;

SELECT 'merge constant out of projection range';
SELECT 5, k FROM t_pos_merge ORDER BY ALL
SETTINGS enable_positional_arguments = 1;

DROP TABLE t_pos_merge;
DROP TABLE t_pos_d;
DROP TABLE t_pos_m1;
DROP VIEW v_pos;
DROP TABLE t_pos;
