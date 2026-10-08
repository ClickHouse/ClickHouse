-- Tags: shard

-- Regression test for predicate pushdown through a VIEW over UNION ALL of Distributed
-- tables (https://github.com/ClickHouse/ClickHouse/issues/91641). The outer WHERE must
-- reach the primary key of every branch's shards, else each shard full-scans.

DROP VIEW IF EXISTS v_91641;
DROP TABLE IF EXISTS d_plain;
DROP TABLE IF EXISTS d_extra;
DROP TABLE IF EXISTS t_plain;
DROP TABLE IF EXISTS t_extra;

CREATE TABLE t_plain (code String, ts UInt64) ENGINE = MergeTree ORDER BY (code, ts);
CREATE TABLE t_extra (provider String, code String, ts UInt64) ENGINE = MergeTree ORDER BY (code, ts);
CREATE TABLE d_plain AS t_plain ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_plain);
CREATE TABLE d_extra AS t_extra ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_extra);

CREATE VIEW v_91641 AS
SELECT code, ts FROM d_plain
UNION ALL
SELECT code, ts FROM d_extra WHERE provider = 'p0';

INSERT INTO t_plain SELECT toString(number), number FROM numbers(16);
INSERT INTO t_extra SELECT 'p0', toString(number), number FROM numbers(16);

-- Every ReadFromMergeTree scan (local and remote-under-ReadFromRemote, both branches) must
-- carry the pushed `code` primary-key condition. (conditions == scans) is the regression
-- detector: pushing to only some scans leaves conditions < scans. serialize_query_plan = 0
-- is pinned because the "distributed plan" CI job enables it via a profile and a serialized
-- plan hides remote index analysis behind a placeholder; the query_plan_* / predicate pins
-- keep the result a regression signal instead of a randomized-settings artifact.
SELECT countIf(explain LIKE '%ReadFromMergeTree (%t_plain)%') >= 1
   AND countIf(explain LIKE '%ReadFromMergeTree (%t_extra)%') >= 1
   AND countIf(explain LIKE '%ReadFromRemote%') >= 2
   AND countIf(explain LIKE '%code in [''5'', ''5'']%') = countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN indexes = 1, distributed = 1 SELECT * FROM v_91641 WHERE code = '5')
SETTINGS
    enable_analyzer = 1,
    enable_parallel_replicas = 0,
    serialize_query_plan = 0,
    query_plan_enable_optimizations = 1,
    query_plan_filter_push_down = 1;

DROP VIEW v_91641;
DROP TABLE d_plain;
DROP TABLE d_extra;
DROP TABLE t_plain;
DROP TABLE t_extra;
