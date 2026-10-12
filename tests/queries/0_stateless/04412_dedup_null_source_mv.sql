-- Tags: zookeeper

-- Characterization test for the deduplication-token path that the (now removed) part-writer hash /
-- redefineTokensWithDataHash machinery used to touch: a Null-engine source feeds a materialized view
-- into a Replicated target with deduplicate_blocks_in_dependent_materialized_views = 1. Single-row
-- inserts keep the order-sensitive unified data hash stable (immune to max_insert_threads/block-size
-- randomization), so a repeated identical insert is deduplicated on the MV target while a distinct
-- insert is not. Removing the dead part-writer machinery must not change this outcome.

SET deduplicate_blocks_in_dependent_materialized_views = 1;

DROP VIEW IF EXISTS mv;
DROP TABLE IF EXISTS mv_dst SYNC;
DROP TABLE IF EXISTS src_null;

CREATE TABLE src_null (k UInt64, v UInt64) ENGINE = Null;

CREATE TABLE mv_dst (k UInt64, s UInt64)
ENGINE = ReplicatedSummingMergeTree('/clickhouse/tables/{database}/mv_dst', '1')
ORDER BY k;

CREATE MATERIALIZED VIEW mv TO mv_dst AS SELECT k, v AS s FROM src_null;

INSERT INTO src_null VALUES (1, 5);
INSERT INTO src_null VALUES (1, 5);

SELECT 'after identical inserts';
SELECT k, s FROM mv_dst FINAL ORDER BY k;

INSERT INTO src_null VALUES (1, 7);

SELECT 'after distinct insert';
SELECT k, s FROM mv_dst FINAL ORDER BY k;

DROP VIEW mv;
DROP TABLE mv_dst SYNC;
DROP TABLE src_null;
