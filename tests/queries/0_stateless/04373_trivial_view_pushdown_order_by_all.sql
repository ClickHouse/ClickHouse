-- ORDER BY ALL sets a boolean flag (order_by_all) while leaving the orderBy() expression list empty,
-- the same boolean-only representation as GROUP BY ALL / LIMIT BY ALL. It bypassed the trivial-view
-- eligibility guard that only checked orderBy(). Such a view is not trivial: the normal
-- StorageView::readImpl path applies the view body's global ordering before the outer LIMIT, but
-- under the pushdown the shard query can apply ORDER BY ALL and LIMIT per shard, so the coordinator
-- could return a shard-local first row instead of the globally first one. The view must be rejected so
-- the query falls back to the canonical StorageView path.
--
-- Asserted via EXPLAIN (the plan must keep the "VIEW subquery" steps, i.e. the pushdown is
-- suppressed) and via result equality with the setting on and off.
--
-- Tags: shard

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS data_04373;
DROP TABLE IF EXISTS dist_04373;
DROP VIEW IF EXISTS view_oba_04373;

CREATE TABLE data_04373 (key Int, value String) ENGINE = MergeTree() ORDER BY key;
INSERT INTO data_04373 SELECT number, toString(number) FROM numbers(10);

CREATE TABLE dist_04373 AS data_04373
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), data_04373, key % 2);

CREATE VIEW view_oba_04373 AS SELECT key FROM dist_04373 ORDER BY ALL;

-- ORDER BY ALL: pushdown must be suppressed (plan keeps "VIEW subquery") ...
SELECT countIf(explain LIKE '%VIEW subquery%') > 0 AS pushdown_suppressed
FROM (EXPLAIN SELECT key FROM view_oba_04373 LIMIT 1
      SETTINGS optimize_trivial_view_pushdown_to_distributed = 1);
-- ... and the globally-first row must be identical with the setting on and off.
SELECT
    (SELECT key FROM view_oba_04373 LIMIT 1 SETTINGS optimize_trivial_view_pushdown_to_distributed = 1)
  = (SELECT key FROM view_oba_04373 LIMIT 1 SETTINGS optimize_trivial_view_pushdown_to_distributed = 0)
  AS results_match;

DROP VIEW view_oba_04373;
DROP TABLE dist_04373;
DROP TABLE data_04373;
