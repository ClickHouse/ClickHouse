-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/116839
-- `optimize_trivial_view_pushdown_to_distributed` ships the outer query of a trivial view over a
-- `Distributed` table to the shards, and suppresses itself when the outer query contains a subquery,
-- because a subquery reading an initiator-local table changes its result when re-evaluated per shard.
-- A bare `IN <table>` carries a TABLE node rather than a QUERY node and slipped through, so the two
-- spellings of the same predicate returned different rows.

SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP TABLE IF EXISTS t_view_pushdown_data;
DROP TABLE IF EXISTS t_view_pushdown_probe;
DROP TABLE IF EXISTS d_view_pushdown;
DROP VIEW IF EXISTS v_view_pushdown;

CREATE TABLE t_view_pushdown_data (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_view_pushdown_probe (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_view_pushdown_data SELECT number FROM numbers(10);
INSERT INTO t_view_pushdown_probe VALUES (1), (2);

CREATE TABLE d_view_pushdown AS t_view_pushdown_data
ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_view_pushdown_data);
CREATE VIEW v_view_pushdown AS SELECT * FROM d_view_pushdown;

-- The two spellings of the same predicate must agree, and neither may depend on the setting.
SELECT id FROM v_view_pushdown WHERE id IN t_view_pushdown_probe ORDER BY id;
SELECT id FROM v_view_pushdown WHERE id IN t_view_pushdown_probe ORDER BY id SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT id FROM v_view_pushdown WHERE id IN (SELECT id FROM t_view_pushdown_probe) ORDER BY id;

SELECT 'not in';
SELECT count() FROM v_view_pushdown WHERE id NOT IN t_view_pushdown_probe;
SELECT count() FROM v_view_pushdown WHERE id NOT IN t_view_pushdown_probe SETTINGS optimize_trivial_view_pushdown_to_distributed = 0;
SELECT count() FROM v_view_pushdown WHERE id NOT IN (SELECT id FROM t_view_pushdown_probe);

-- A predicate with no table reference is still pushed down.
SELECT 'still pushed down';
SELECT count() FROM v_view_pushdown WHERE id > 5;
SELECT count() FROM v_view_pushdown WHERE id IN (1, 2);

DROP VIEW v_view_pushdown;
DROP TABLE d_view_pushdown;
DROP TABLE t_view_pushdown_probe;
DROP TABLE t_view_pushdown_data;
