-- ALIAS columns used after aggregation are computed from GROUP BY keys, so in the rows of WITH TOTALS, ROLLUP, CUBE
-- and GROUPING SETS, where the keys have default values, they get the same values as the same expressions of the keys.
-- https://github.com/ClickHouse/ClickHouse/issues/121400

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_alias_agg;

CREATE TABLE t_alias_agg
(
    k String,
    n UInt64,
    v UInt64,
    k_excl String ALIAS concat(k, '!'),
    n_plus UInt64 ALIAS n + 100,
    const_alias UInt8 ALIAS 42,
    shard_alias UInt32 ALIAS shardNum()
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_alias_agg VALUES ('a', 1, 10), ('b', 2, 20), ('a', 1, 40);

SELECT '-- WITH TOTALS';
SELECT k, k_excl, concat(k, '!'), sum(v) FROM t_alias_agg GROUP BY k WITH TOTALS ORDER BY k;
SELECT k, k_excl, sum(v) AS s FROM t_alias_agg GROUP BY k WITH TOTALS HAVING concat(k_excl, toString(s)) != 'b!20' ORDER BY k;

SELECT '-- HAVING with an aggregate function';
SELECT k, k_excl, sum(v) AS s FROM t_alias_agg GROUP BY k HAVING concat(k_excl, toString(s)) != 'b!20' ORDER BY k;

SELECT '-- ROLLUP, CUBE, GROUPING SETS';
SELECT k, n, k_excl, n_plus, sum(v) FROM t_alias_agg GROUP BY k, n WITH ROLLUP ORDER BY k, n;
SELECT k, n, k_excl, n_plus, sum(v) FROM t_alias_agg GROUP BY k, n WITH CUBE ORDER BY k, n;
SELECT k, n, k_excl, n_plus, sum(v) FROM t_alias_agg GROUP BY GROUPING SETS ((k), (n)) ORDER BY k, n;
SELECT k, n, k_excl, n_plus, sum(v) FROM t_alias_agg GROUP BY k, n WITH ROLLUP WITH TOTALS ORDER BY k, n;

SELECT '-- correlated subqueries';
SELECT k FROM t_alias_agg GROUP BY k HAVING EXISTS (SELECT 1 WHERE k_excl != 'b!') ORDER BY k;
SELECT k, (SELECT k_excl) FROM t_alias_agg GROUP BY k ORDER BY k;
SELECT k FROM t_alias_agg GROUP BY k HAVING EXISTS (SELECT 1 WHERE n_plus != 101); -- { serverError NOT_AN_AGGREGATE }
SELECT k FROM t_alias_agg GROUP BY k HAVING (SELECT k_excl != 'b!') ORDER BY k;
-- A constant ALIAS is computed in place where it is used directly, but correlated subqueries take it from the outer query.
SELECT k FROM t_alias_agg GROUP BY k HAVING EXISTS (SELECT 1 WHERE const_alias = 42) ORDER BY k;
SELECT k, (SELECT const_alias) FROM t_alias_agg GROUP BY k ORDER BY k;
SELECT k, (SELECT shard_alias) FROM t_alias_agg GROUP BY k; -- { serverError NOT_AN_AGGREGATE }
-- `TotalsHavingStep` evaluates HAVING itself, and its correlated subqueries are planned before it.
SELECT k FROM t_alias_agg GROUP BY k WITH TOTALS HAVING EXISTS (SELECT 1 WHERE k_excl != 'b!') ORDER BY k;
SELECT k, k_excl, concat(k, '!'), sum(v) FROM t_alias_agg GROUP BY k WITH TOTALS HAVING EXISTS (SELECT 1 WHERE k_excl != 'b!') ORDER BY k;
SELECT sum(v) FROM t_alias_agg GROUP BY k WITH TOTALS HAVING EXISTS (SELECT 1 WHERE k_excl != 'b!');

SELECT '-- remote';
SELECT k, k_excl, sum(v) FROM remote('127.0.0.{1,2}', currentDatabase(), t_alias_agg) GROUP BY k WITH TOTALS ORDER BY k;
SELECT k, n, k_excl, sum(v) FROM remote('127.0.0.{1,2}', currentDatabase(), t_alias_agg) GROUP BY k, n WITH ROLLUP ORDER BY k, n;
SELECT k, k_excl, sum(v) FROM remote('127.0.0.{1,2}', currentDatabase(), t_alias_agg) GROUP BY k ORDER BY k SETTINGS distributed_group_by_no_merge = 2;

DROP TABLE t_alias_agg;
