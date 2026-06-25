-- GROUP BY ALL and LIMIT BY ALL set boolean flags (group_by_all / limit_by_all) while leaving the
-- groupBy() / limitBy() expression lists empty, so they bypassed the trivial-view eligibility guard
-- that only checked the lists. Such a view is not trivial: under the pushdown its GROUP BY / LIMIT BY
-- would run per shard instead of once globally on the normal path, changing the result. The view must
-- be rejected so the query falls back to the canonical StorageView path.
--
-- Asserted via EXPLAIN (the plan must keep the "VIEW subquery" steps, i.e. the pushdown is
-- suppressed) and via result equality with the setting on and off. The absolute count is not asserted
-- because it depends on randomized settings such as optimize_distributed_group_by_sharding_key, which
-- behaves differently on test_cluster_two_shards_localhost (both shards read the same local table).
--
-- Tags: shard

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS data_04372;
DROP TABLE IF EXISTS dist_04372;
DROP VIEW IF EXISTS view_gba_04372;
DROP VIEW IF EXISTS view_lba_04372;

CREATE TABLE data_04372 (key Int, value String) ENGINE = MergeTree() ORDER BY key;
INSERT INTO data_04372 SELECT number, toString(number) FROM numbers(10);

CREATE TABLE dist_04372 AS data_04372
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), data_04372, key % 2);

CREATE VIEW view_gba_04372 AS SELECT key FROM dist_04372 GROUP BY ALL;
CREATE VIEW view_lba_04372 AS SELECT key FROM dist_04372 LIMIT 1 BY ALL;

-- GROUP BY ALL: pushdown must be suppressed (plan keeps "VIEW subquery") ...
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT count() FROM view_gba_04372
      SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);
-- ... and the result must be identical with the setting on and off.
SELECT
    (SELECT count() FROM view_gba_04372 SETTINGS optimize_trivial_view_pushdown_to_distributed = 1)
  = (SELECT count() FROM view_gba_04372 SETTINGS optimize_trivial_view_pushdown_to_distributed = 0)
  AS results_match;

-- LIMIT BY ALL: likewise suppressed and identical on and off.
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT count() FROM view_lba_04372
      SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);
SELECT
    (SELECT count() FROM view_lba_04372 SETTINGS optimize_trivial_view_pushdown_to_distributed = 1)
  = (SELECT count() FROM view_lba_04372 SETTINGS optimize_trivial_view_pushdown_to_distributed = 0)
  AS results_match;

DROP VIEW view_gba_04372;
DROP VIEW view_lba_04372;
DROP TABLE dist_04372;
DROP TABLE data_04372;
