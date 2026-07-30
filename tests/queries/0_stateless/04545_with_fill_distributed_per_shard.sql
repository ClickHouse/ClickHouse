-- Tags: distributed

-- ORDER BY ... WITH FILL over a Distributed table must fill once on the initiator over the
-- merged shard streams, not on each shard. Before the fix each fill row was returned once per
-- shard (duplicated) and positioned per-shard rather than globally. See issue #111212.
-- The two shards below read the same table, so DATA rows are legitimately duplicated per shard,
-- but FILL rows must NOT be.
--
-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; that path is orthogonal to the step placement tested here.

DROP TABLE IF EXISTS t_with_fill_dist;
CREATE TABLE t_with_fill_dist (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_with_fill_dist VALUES (100);

-- Fill-only result (no data rows pass the filter): fill rows must appear exactly once.
SELECT 'empty analyzer=1';
SELECT k FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_with_fill_dist)
WHERE k < 0 ORDER BY k WITH FILL FROM 0 TO 5
SETTINGS enable_analyzer = 1, serialize_query_plan = 0;

SELECT 'empty analyzer=0';
SELECT k FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_with_fill_dist)
WHERE k < 0 ORDER BY k WITH FILL FROM 0 TO 5
SETTINGS enable_analyzer = 0, serialize_query_plan = 0;

-- With data rows: the k=100 row is duplicated across shards (correct), fill rows 0,1,2 appear once.
SELECT 'data analyzer=1';
SELECT k FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_with_fill_dist)
ORDER BY k WITH FILL FROM 0 TO 3
SETTINGS enable_analyzer = 1, serialize_query_plan = 0;

SELECT 'data analyzer=0';
SELECT k FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_with_fill_dist)
ORDER BY k WITH FILL FROM 0 TO 3
SETTINGS enable_analyzer = 0, serialize_query_plan = 0;

-- Also with distributed_push_down_limit=0 (shards asked to process to WithMergeableStateAfterAggregation).
SELECT 'no push down limit analyzer=1';
SELECT k FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_with_fill_dist)
WHERE k < 0 ORDER BY k WITH FILL FROM 0 TO 5
SETTINGS enable_analyzer = 1, distributed_push_down_limit = 0, serialize_query_plan = 0;

-- WITH FILL STEP.
SELECT 'step analyzer=1';
SELECT k FROM remote('127.0.0.1,127.0.0.1', currentDatabase(), t_with_fill_dist)
WHERE k < 0 ORDER BY k WITH FILL FROM 0 TO 5 STEP 2
SETTINGS enable_analyzer = 1, serialize_query_plan = 0;

DROP TABLE t_with_fill_dist;
