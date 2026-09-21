-- The runtime dataflow statistics entry that feeds the automatic parallel replicas cost model is
-- keyed per plan node, and for a read that key has to describe what the read actually touches.
-- Queries that differ only in something the key ignores end up sharing an entry, and the second one
-- is then priced on the first one's measurements.
--
-- Nothing else catches this. The drift check that guards a reused entry compares
-- `total_rows_to_read`, and the steps between the read and the node the replicas would send from are
-- transparent for the key when they hand their inputs onward unchanged - which a `Sorting` without a
-- limit and a projection that only forwards columns both do. So for the shapes below the key
-- degenerates to the read, and the read is all that can tell the queries apart.
--
-- `automatic_parallel_replicas_mode` = 1 installs the statistics collector exactly when the lookup
-- found nothing (or found something stale), so a query that recorded statistics is one that did not
-- reuse an entry. Mode 2 would collect unconditionally and assert nothing.

DROP TABLE IF EXISTS t_autopr_stats_key;
DROP TABLE IF EXISTS t_autopr_stats_key_sampled;
DROP TABLE IF EXISTS t_autopr_stats_key_split;
DROP TABLE IF EXISTS t_autopr_stats_key_policy;

CREATE TABLE t_autopr_stats_key (k UInt64, narrow UInt8, wide String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_autopr_stats_key_sampled (k UInt64, v UInt32) ENGINE = MergeTree ORDER BY k SAMPLE BY k;
-- `a` + `UInt8` and `aU` + `Int8` are the same bytes under a different split.
CREATE TABLE t_autopr_stats_key_split (a UInt8, aU Int8) ENGINE = MergeTree ORDER BY tuple();
-- `narrow` is deliberately outside the primary key: a policy over it cannot narrow index analysis,
-- so `total_rows_to_read` stays put and the drift check has nothing to catch.
CREATE TABLE t_autopr_stats_key_policy (k UInt64, narrow UInt8) ENGINE = MergeTree ORDER BY k;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3,
    cluster_for_parallel_replicas='parallel_replicas';

SET enable_analyzer=1;

-- max_block_size is set explicitly to ensure enough blocks will be fed to the statistics collector
SET max_threads=4, max_block_size=128;

-- Otherwise the cheap pre-check rejects these tables before the probe plan is built and nothing
-- collects statistics at all.
SET automatic_parallel_replicas_min_bytes_per_replica=0;

INSERT INTO t_autopr_stats_key SELECT number, number % 7, repeat('x', 200) FROM numbers(200000);
INSERT INTO t_autopr_stats_key_sampled SELECT number, number FROM numbers(200000);
INSERT INTO t_autopr_stats_key_split SELECT number % 256, number % 128 FROM numbers(200000);
INSERT INTO t_autopr_stats_key_policy SELECT number, number % 7 FROM numbers(200000);

-- A different set of columns. `wide` is a two-hundred-byte string where `narrow` is one byte, so
-- reusing query 0's entry would price query 2 at a fraction of what it reads.
SELECT k, narrow FROM t_autopr_stats_key ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_0_narrow_first';
-- The same query again. This is the half that must keep working: the point is not to key every
-- query separately.
SELECT k, narrow FROM t_autopr_stats_key ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_1_narrow_again';
SELECT k, wide FROM t_autopr_stats_key ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_2_wide';

-- A `SAMPLE` ratio. Same storage, same header, same PREWHERE, a tenth of the table.
SELECT k, v FROM t_autopr_stats_key_sampled ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_3_unsampled_first';
SELECT k, v FROM t_autopr_stats_key_sampled ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_4_unsampled_again';
SELECT k, v FROM t_autopr_stats_key_sampled SAMPLE 1/10 ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_5_sampled';

-- Two headers that are the same bytes under a different split. Hashing the name and the type one
-- after another without their lengths gives both the sequence `aUInt8`.
SELECT a FROM t_autopr_stats_key_split ORDER BY a FORMAT Null
    SETTINGS log_comment='05234_query_6_a_uint8';
SELECT aU FROM t_autopr_stats_key_split ORDER BY aU FORMAT Null
    SETTINGS log_comment='05234_query_7_au_int8';

-- A row policy. It is pushed into the read rather than becoming a step above it, so nothing else in
-- the plan distinguishes two policies: both leave the header identical, and a policy over a column
-- outside the primary key leaves index analysis - and so the drift check - unmoved. The two below
-- pass a different number of rows into the sort.
CREATE ROW POLICY r_05234_a ON t_autopr_stats_key_policy USING narrow < 3 TO ALL;
SELECT k, narrow FROM t_autopr_stats_key_policy ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_8_policy_a';
DROP ROW POLICY r_05234_a ON t_autopr_stats_key_policy;

CREATE ROW POLICY r_05234_b ON t_autopr_stats_key_policy USING narrow < 5 TO ALL;
SELECT k, narrow FROM t_autopr_stats_key_policy ORDER BY k FORMAT Null
    SETTINGS log_comment='05234_query_9_policy_b';
DROP ROW POLICY r_05234_b ON t_autopr_stats_key_policy;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Both events are incremented by the one `update` call that writes the entry, and that call is
-- reached exactly when at least one of them is non-zero - so their sum is "an entry was written".
-- Reading the input side alone would be wrong: it is dropped when the compression-ratio sample comes
-- out empty, which a one-byte column does often enough to make the check flap.
SELECT log_comment AS query,
       ProfileEvents['RuntimeDataflowStatisticsInputBytes']
           + ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0 AS collected_own_statistics
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
  AND (current_database = currentDatabase()) AND (log_comment LIKE '05234_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_stats_key;
DROP TABLE t_autopr_stats_key_sampled;
DROP TABLE t_autopr_stats_key_split;
DROP TABLE t_autopr_stats_key_policy;
