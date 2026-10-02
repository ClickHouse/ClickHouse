-- Tags: distributed

SET enable_analyzer = 0;

DROP ROW POLICY IF EXISTS rp ON t;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    k UInt64,
    team String,
    a UInt64 ALIAS k * 10,
    a2 String ALIAS concat(team, toString(a))
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t VALUES (1, 'ok'), (2, 'ok'), (3, 'ok'), (4, 'no');

CREATE TABLE t_dist AS t
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't', rand());

SELECT 'baseline local';
SELECT k FROM t ORDER BY k;

SELECT 'baseline distributed';
SELECT k FROM t_dist ORDER BY k SETTINGS prefer_localhost_replica = 0;

-- 1 fails the `ALIAS` condition; 3 fails the nested `ALIAS` condition;
-- 4 passes both `ALIAS` conditions but fails the physical team condition.
CREATE ROW POLICY rp ON t FOR SELECT
USING team = 'ok' AND a >= 20 AND a2 != 'ok30' TO ALL;

SELECT 'policy local';
SELECT k FROM t ORDER BY k;

SELECT 'policy distributed local replica';
SELECT k FROM t_dist ORDER BY k SETTINGS prefer_localhost_replica = 1;

SELECT 'policy distributed remote replica';
SELECT k FROM t_dist ORDER BY k SETTINGS prefer_localhost_replica = 0;

SELECT 'policy distributed with PREWHERE';
SELECT k FROM t_dist PREWHERE k >= 1 WHERE k <= 4 ORDER BY k
SETTINGS prefer_localhost_replica = 0;

SELECT 'policy local without alias optimization';
SELECT k FROM t ORDER BY k SETTINGS optimize_respect_aliases = 0;

SELECT 'policy local with aliases selected';
SELECT k, a, a2 FROM t ORDER BY k SETTINGS optimize_respect_aliases = 0;

-- `PREWHERE` uses team, while both the policy and the selected aliases need k.
-- The alias step must preserve k even when a separate `PREWHERE` step exists.
SELECT 'policy local with aliases selected and PREWHERE';
SELECT k, a, a2 FROM t PREWHERE team != 'no' ORDER BY k
SETTINGS optimize_respect_aliases = 0;

SELECT 'policy distributed with aliases selected';
SELECT k, a, a2 FROM t_dist ORDER BY k SETTINGS prefer_localhost_replica = 0;

SELECT 'policy distributed with aliases selected and PREWHERE';
SELECT k, a, a2 FROM t_dist PREWHERE team != 'no' ORDER BY k
SETTINGS prefer_localhost_replica = 0, optimize_respect_aliases = 0;

SELECT 'policy distributed new analyzer';
SELECT k FROM t_dist ORDER BY k
SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;

-- A policy whose only predicate references an `ALIAS` still needs its value
-- before the row-level filter, even when the `SELECT` only needs `count`.
CREATE ROW POLICY OR REPLACE rp ON t FOR SELECT
USING a2 = 'ok20' TO ALL;

SELECT 'policy with only an ALIAS predicate';
SELECT count(), sum(k) FROM t_dist SETTINGS prefer_localhost_replica = 0;

-- The equivalent expression is a control for the alias substitution.
CREATE ROW POLICY OR REPLACE rp ON t FOR SELECT
USING team = 'ok' AND k * 10 >= 20 AND concat(team, toString(k * 10)) != 'ok30' TO ALL;

SELECT 'policy with inline expression';
SELECT k FROM t_dist ORDER BY k SETTINGS prefer_localhost_replica = 0;

DROP ROW POLICY rp ON t;

-- Additional table filters use the same filter-action construction path.
SELECT 'additional filter on ALIAS';
SELECT k FROM t ORDER BY k
SETTINGS optimize_respect_aliases = 0, additional_table_filters = {'t': 'a >= 20'};

DROP TABLE t_dist;
DROP TABLE t;
