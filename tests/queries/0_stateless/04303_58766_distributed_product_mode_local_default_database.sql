-- Tags: no-parallel, no-flaky-check, distributed
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/58766
--
-- On the old analyzer, a query against two `Distributed` tables (created with an empty database
-- name, relying on per-replica `default_database` from the cluster config) with
-- `distributed_product_mode = 'local'` failed at parse time with `SYNTAX_ERROR`, e.g.
-- "failed at position 98 ('``')", because the subquery rewriter produced an unqualified table
-- reference like `` ``.`smaller_table` `` with empty backticks for the database name.
--
-- The new analyzer (`enable_analyzer = 1`) handles this correctly. This test guards against a
-- regression on the new analyzer path.

DROP TABLE IF EXISTS dist_smaller_58766;
DROP TABLE IF EXISTS dist_larger_58766;

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

DROP TABLE IF EXISTS shard_0.smaller_table_58766;
DROP TABLE IF EXISTS shard_0.larger_table_58766;
DROP TABLE IF EXISTS shard_1.smaller_table_58766;
DROP TABLE IF EXISTS shard_1.larger_table_58766;

CREATE TABLE shard_0.smaller_table_58766 (key String, data String) ENGINE = Memory;
CREATE TABLE shard_0.larger_table_58766 (key String, data String) ENGINE = Memory;
CREATE TABLE shard_1.smaller_table_58766 (key String, data String) ENGINE = Memory;
CREATE TABLE shard_1.larger_table_58766 (key String, data String) ENGINE = Memory;

-- The `Distributed` tables intentionally use an empty database argument — the per-replica
-- `default_database` in the cluster config resolves the underlying local table on each shard.
CREATE TABLE dist_smaller_58766 (key String, data String)
    ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 'smaller_table_58766');

CREATE TABLE dist_larger_58766 (key String, data String)
    ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 'larger_table_58766');

INSERT INTO shard_0.smaller_table_58766 VALUES ('aa', 'a0'), ('bb', 'b0');
INSERT INTO shard_1.smaller_table_58766 VALUES ('cc', 'c1'), ('dd', 'd1');
INSERT INTO shard_0.larger_table_58766 VALUES ('aa', 'A0'), ('bb', 'B0'), ('ee', 'E0');
INSERT INTO shard_1.larger_table_58766 VALUES ('cc', 'C1'), ('dd', 'D1'), ('ff', 'F1');

-- Subquery / `IN` variant — the original bug query from the issue body.
SELECT count()
FROM
(
    SELECT key
    FROM dist_larger_58766 AS lt
    WHERE key IN
    (
        SELECT key
        FROM dist_smaller_58766 AS rt
    )
)
SETTINGS distributed_product_mode = 'local', enable_analyzer = 1;

-- `JOIN` variant — the issue notes the same error also fires for joins on `Distributed` tables.
SELECT count()
FROM dist_larger_58766 AS lt
INNER JOIN dist_smaller_58766 AS rt ON lt.key = rt.key
SETTINGS distributed_product_mode = 'local', enable_analyzer = 1;

DROP TABLE dist_smaller_58766;
DROP TABLE dist_larger_58766;
DROP TABLE shard_0.smaller_table_58766;
DROP TABLE shard_0.larger_table_58766;
DROP TABLE shard_1.smaller_table_58766;
DROP TABLE shard_1.larger_table_58766;
