-- Tags: long
-- Behavior of the top-K heap under stress: heavy eviction across aggregation
-- methods, boundary ties (bitwise-distinct keys that compare equal), the
-- adaptive freeze, tie overflow, and aggregate-state arena-slot reuse.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_rows_to_group_by = 0;

-- Heavy eviction across aggregation methods.

SELECT 'UInt32 key (key32)';
SELECT k, count(), sum(v) FROM (SELECT toUInt32(999 - (number % 1000)) AS k, number AS v FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04501_engage_01';

SELECT 'UInt32 key, DESC';
SELECT k, count() FROM (SELECT toUInt32(number % 1000) AS k FROM numbers(2000)) GROUP BY k ORDER BY k DESC LIMIT 10 SETTINGS log_comment = '04501_engage_02';

SELECT 'UInt64 key (key64)';
SELECT k, count() FROM (SELECT toUInt64(999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04501_engage_03';

SELECT 'Int32 key with negatives';
SELECT k, count() FROM (SELECT toInt32(499 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04501_engage_04';

SELECT 'Float64 key with nan';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, nan, toFloat64(999 - (number % 1000))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04501_engage_05';

SELECT 'Float64 key with nan, DESC';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, nan, toFloat64(number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k DESC LIMIT 3 SETTINGS log_comment = '04501_engage_06';

SELECT 'Float32 key';
SELECT k, count() FROM (SELECT toFloat32(999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04501_engage_07';

SELECT 'DateTime key';
SELECT k, count() FROM (SELECT toDateTime('2020-01-01 00:00:00', 'UTC') + (999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04501_engage_08';

SELECT 'Date key (key16, no hash-table pruning)';
SELECT k, count() FROM (SELECT toDate('2020-01-01') + (999 - (number % 1000)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04501_engage_09';

SELECT 'String key';
SELECT k, count() FROM (SELECT concat('key_', leftPad(toString(999 - (number % 1000)), 4, '0')) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04501_engage_10';

SELECT 'FixedString key';
SELECT k, count() FROM (SELECT toFixedString(leftPad(toString(999 - (number % 1000)), 4, '0'), 4) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04501_engage_11';

SELECT 'LowCardinality(String) key';
SELECT k, count() FROM (SELECT toLowCardinality(concat('key_', leftPad(toString(999 - (number % 1000)), 4, '0'))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04501_engage_12';

SELECT 'Nullable(UInt32) key, NULLS LAST (null slot is evicted)';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, NULL, toNullable(toUInt32(999 - (number % 1000)))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC NULLS LAST LIMIT 5 SETTINGS log_comment = '04501_engage_13';

SELECT 'Nullable(UInt32) key, NULLS FIRST (null slot stays in the heap)';
SELECT k, count() FROM (SELECT if(number % 1000 = 500, NULL, toNullable(toUInt32(999 - (number % 1000)))) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 5 SETTINGS log_comment = '04501_engage_14';

SELECT 'Tuple key (single serialized GROUP BY column)';
SELECT k, count() FROM (SELECT (toUInt32(999 - (number % 1000)), toString(number % 2)) AS k FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5 SETTINGS log_comment = '04501_engage_15';

SELECT 'Composite fixed key (UInt32, UInt16)';
SELECT a, b, count() FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toUInt16(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10 SETTINGS log_comment = '04501_engage_16';

SELECT 'Composite serialized key (UInt32, String)';
SELECT a, b, count() FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toString(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10 SETTINGS log_comment = '04501_engage_17';

SELECT 'Composite nullable key (Nullable(UInt32), String)';
SELECT a, b, count() FROM (SELECT if(number % 1000 = 995, NULL, toNullable(toUInt32(99 - intDiv(number % 1000, 10)))) AS a, toString(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC NULLS LAST, b ASC LIMIT 10 SETTINGS log_comment = '04501_engage_18';

SELECT 'Composite LowCardinality key (LowCardinality(String), UInt32)';
SELECT a, b, count() FROM (SELECT toLowCardinality(leftPad(toString(99 - intDiv(number % 1000, 10)), 3, '0')) AS a, toUInt32(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10 SETTINGS log_comment = '04501_engage_19';

SELECT 'Prefix mode (ORDER BY is a prefix of GROUP BY, no hash-table pruning)';
SELECT * FROM (SELECT a, b, count() FROM (SELECT toUInt32(99 - intDiv(number % 1000, 10)) AS a, toUInt16(number % 10) AS b FROM numbers(2000)) GROUP BY a, b ORDER BY a ASC LIMIT 10) ORDER BY a, b SETTINGS log_comment = '04501_engage_20';

SELECT 'Stateful aggregate under eviction (uniqExact)';
SELECT k, uniqExact(v) FROM (SELECT toUInt32(999 - (number % 1000)) AS k, number % 3 AS v FROM numbers(2000)) GROUP BY k ORDER BY k ASC LIMIT 5;

SELECT 'Const-key block arriving after its key was evicted';
SELECT k, count(), sum(v) FROM (SELECT 2::UInt32 AS k, 1 AS v FROM numbers(5) UNION ALL SELECT 1::UInt32, 1 FROM numbers(5) UNION ALL SELECT 2::UInt32, 1 FROM numbers(5)) GROUP BY k ORDER BY k ASC LIMIT 1;

-- LowCardinality eviction must not erase from the hash table: the State's
-- per-dictionary-index cache cannot be invalidated, so a re-appearing index would
-- return a destroyed aggregate state.  Result must match optimization off.
DROP TABLE IF EXISTS gt_low_cardinality_eviction;
CREATE TABLE gt_low_cardinality_eviction ENGINE = Memory EMPTY AS
SELECT k, count() AS c, sum(v) AS s FROM (SELECT toLowCardinality(toString(999999 - number)) AS k, number AS v FROM numbers(30000)) GROUP BY k ORDER BY k ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_low_cardinality_eviction
SELECT k, count() AS c, sum(v) AS s FROM (SELECT toLowCardinality(toString(999999 - number)) AS k, number AS v FROM numbers(30000)) GROUP BY k ORDER BY k ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'LowCardinality eviction: result matches optimization off';
SELECT count() FROM
(
    SELECT k, count() AS c, sum(v) AS s FROM (SELECT toLowCardinality(toString(999999 - number)) AS k, number AS v FROM numbers(30000)) GROUP BY k ORDER BY k ASC LIMIT 10
) AS optimized
INNER JOIN gt_low_cardinality_eviction AS full USING (k, c, s)
SETTINGS max_block_size = 4096;

-- A mid-block eviction must invalidate the consecutive-key cache: a key admitted by
-- the stale skip bitmap, pushed, then evicted, must not hand its destroyed state to
-- a later equal row.  Runs of equal keys + a stateful aggregate make it observable.
-- Small max_block_size guarantees multiple blocks (so the heap is full at a block
-- start and the precomputed skip bitmap is exercised) without a large row count.
DROP TABLE IF EXISTS gt_consecutive_key_cache;
CREATE TABLE gt_consecutive_key_cache ENGINE = Memory EMPTY AS
SELECT k, uniqExact(v) AS u, sum(v) AS s FROM (SELECT intDiv(999999 - number, 4)::UInt32 AS k, number AS v FROM numbers(40000)) GROUP BY k ORDER BY k ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_consecutive_key_cache
SELECT k, uniqExact(v) AS u, sum(v) AS s FROM (SELECT intDiv(999999 - number, 4)::UInt32 AS k, number AS v FROM numbers(40000)) GROUP BY k ORDER BY k ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'consecutive-key cache after eviction: result matches optimization off';
SELECT count() FROM
(
    SELECT k, uniqExact(v) AS u, sum(v) AS s FROM (SELECT intDiv(999999 - number, 4)::UInt32 AS k, number AS v FROM numbers(40000)) GROUP BY k ORDER BY k ASC LIMIT 10
) AS optimized
INNER JOIN gt_consecutive_key_cache AS full USING (k, u, s)
SETTINGS max_block_size = 4096;

SELECT 'heap_engaged_guard';
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0, sum(ProfileEvents['AggregationTopKKeysEvicted']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';

SELECT 'per_family_engagement';
SELECT log_comment, max(ProfileEvents['AggregationTopKRowsSkipped'] + ProfileEvents['AggregationTopKKeysEvicted']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04501_engage_%'
GROUP BY log_comment ORDER BY log_comment;

-- Keys that are bitwise distinct yet compare equal (-0.0 vs +0.0, NaNs with
-- different payloads) tie under ORDER BY: the heap must never evict a key tied
-- with the boundary.

SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'alternating negative and positive zero, stateful aggregate, LIMIT 1';
SELECT k, uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0, toFloat64(0), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC LIMIT 1;

SELECT 'same result without the optimization';
SELECT k, uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0, toFloat64(0), reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC LIMIT 1
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'alternating NaN payloads, LIMIT 1';
SELECT isNaN(k), uniqExact(v) FROM
(
    SELECT
        if(number % 2 = 0,
           reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090561))),
           reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9221120237041090562)))) AS k,
        number % 5 AS v
    FROM numbers(100000)
)
GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 1;

DROP TABLE IF EXISTS gt_zeros_tied;
CREATE TABLE gt_zeros_tied ENGINE = Memory EMPTY AS
SELECT k, uniqExact(v) AS u FROM (SELECT multiIf(number % 4 = 0, toFloat64(0), number % 4 = 1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808))), toFloat64(-1000000 + intDiv(toInt64(number), 4))) AS k, number % 5 AS v FROM numbers(100000))
GROUP BY k;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_zeros_tied
SELECT k, uniqExact(v) AS u FROM (SELECT multiIf(number % 4 = 0, toFloat64(0), number % 4 = 1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808))), toFloat64(-1000000 + intDiv(toInt64(number), 4))) AS k, number % 5 AS v FROM numbers(100000))
GROUP BY k;
SET enable_group_by_top_k_optimization = 1;

SELECT 'zeros tied at the boundary among real eviction churn';
SELECT count(), countIf(complete) FROM
(
    SELECT l.u = f.u AS complete
    FROM
    (
        SELECT k, uniqExact(v) AS u FROM (SELECT multiIf(number % 4 = 0, toFloat64(0), number % 4 = 1, reinterpretAsFloat64(reinterpretAsFixedString(toUInt64(9223372036854775808))), toFloat64(-1000000 + intDiv(toInt64(number), 4))) AS k, number % 5 AS v FROM numbers(100000))
        GROUP BY k ORDER BY k DESC LIMIT 3
    ) AS l
    INNER JOIN gt_zeros_tied AS f ON l.k = f.k
);

-- A boundary tie-set that keeps growing must not grow the heap without bound.
-- Prefix mode (ORDER BY a, a prefix of GROUP BY a,b): early rows vary `a` so the
-- heap evicts (which disables the never-evicted freeze), then ~1.3M distinct `b`
-- all share the in-heap boundary prefix `a = 0` and tie, so the heap would grow
-- forever without the `tie_overflow` cap.  It must freeze instead; results stay
-- correct (prefix mode never erases) and peak memory stays bounded.
SELECT 'tie-overflow after eviction freezes the heap';
SELECT a FROM (SELECT if(number < 2000, number % 10, 0)::UInt32 AS a, number AS b FROM numbers(1300000)) GROUP BY a, b ORDER BY a ASC LIMIT 5
SETTINGS log_comment = '04501_tie_overflow' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['AggregationTopKKeysEvicted']) > 0 AS evicted, sum(ProfileEvents['AggregationTopKHeapsFrozen']) > 0 AS froze
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04501_tie_overflow';

-- Every group the frozen-heap query returns must carry its complete count
-- (the freeze must not drop or corrupt accumulated state).
DROP TABLE IF EXISTS gt_tie_overflow;
CREATE TABLE gt_tie_overflow ENGINE = Memory EMPTY AS
SELECT a, b, count() AS c FROM (SELECT if(number < 2000, number % 10, 0)::UInt32 AS a, number AS b FROM numbers(1300000)) GROUP BY a, b;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_tie_overflow
SELECT a, b, count() AS c FROM (SELECT if(number < 2000, number % 10, 0)::UInt32 AS a, number AS b FROM numbers(1300000)) GROUP BY a, b;
SET enable_group_by_top_k_optimization = 1;

SELECT 'tie-overflow: every returned group complete';
SELECT count(), countIf(complete) FROM
(
    SELECT l.c = f.c AS complete
    FROM (SELECT a, b, count() AS c FROM (SELECT if(number < 2000, number % 10, 0)::UInt32 AS a, number AS b FROM numbers(1300000)) GROUP BY a, b ORDER BY a ASC LIMIT 5) AS l
    INNER JOIN gt_tie_overflow AS f USING (a, b)
);

-- The adaptive freeze: the heap freezes once it has observed many rows while
-- full but never skipped or evicted (distinct-key count == LIMIT).  Results
-- must match the non-optimized plan.

SET optimize_trivial_group_by_limit_query = 0;

DROP TABLE IF EXISTS gt_freeze_smaller_keys;
CREATE TABLE gt_freeze_smaller_keys ENGINE = Memory EMPTY AS
SELECT k, count() AS c FROM (SELECT if(number < 400000, 1000 + number % 3, number % 100)::UInt32 AS k FROM numbers(600000)) GROUP BY k ORDER BY k ASC LIMIT 3;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_freeze_smaller_keys
SELECT k, count() AS c FROM (SELECT if(number < 400000, 1000 + number % 3, number % 100)::UInt32 AS k FROM numbers(600000)) GROUP BY k ORDER BY k ASC LIMIT 3;
SET enable_group_by_top_k_optimization = 1;

SELECT 'freeze then smaller keys: result matches non-optimized';
SELECT count() FROM
(
    SELECT k, count() AS c FROM (SELECT if(number < 400000, 1000 + number % 3, number % 100)::UInt32 AS k FROM numbers(600000)) GROUP BY k ORDER BY k ASC LIMIT 3
) AS optimized
INNER JOIN gt_freeze_smaller_keys AS full USING (k, c);

DROP TABLE IF EXISTS gt_no_order_by_freeze;
CREATE TABLE gt_no_order_by_freeze ENGINE = Memory EMPTY AS
SELECT k, sum(v) AS s FROM (SELECT (number % 5)::UInt32 AS k, 1 AS v FROM numbers(600000)) GROUP BY k;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_no_order_by_freeze
SELECT k, sum(v) AS s FROM (SELECT (number % 5)::UInt32 AS k, 1 AS v FROM numbers(600000)) GROUP BY k;
SET enable_group_by_top_k_optimization = 1;

SELECT 'no-ORDER-BY freeze (cardinality == LIMIT): every returned group complete';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT k, sum(v) AS s FROM (SELECT (number % 5)::UInt32 AS k, 1 AS v FROM numbers(600000)) GROUP BY k LIMIT 5) AS l
    INNER JOIN gt_no_order_by_freeze AS f USING (k)
);

DROP TABLE IF EXISTS gt_composite_no_order_by_freeze;
CREATE TABLE gt_composite_no_order_by_freeze ENGINE = Memory EMPTY AS
SELECT a, b, min(v) AS s FROM (SELECT number % 10 + 1 AS a, number % 10 + 2 AS b, number AS v FROM numbers(600000)) GROUP BY a, b;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_composite_no_order_by_freeze
SELECT a, b, min(v) AS s FROM (SELECT number % 10 + 1 AS a, number % 10 + 2 AS b, number AS v FROM numbers(600000)) GROUP BY a, b;
SET enable_group_by_top_k_optimization = 1;

SELECT 'composite no-ORDER-BY freeze (cardinality == LIMIT, q3 shape)';
SELECT count(), countIf(complete) FROM
(
    SELECT l.s = f.s AS complete
    FROM (SELECT a, b, min(v) AS s FROM (SELECT number % 10 + 1 AS a, number % 10 + 2 AS b, number AS v FROM numbers(600000)) GROUP BY a, b LIMIT 10) AS l
    INNER JOIN gt_composite_no_order_by_freeze AS f USING (a, b)
);

SELECT 'Eviction-heavy stream must not freeze (results stay top-K correct)';
SELECT k, count() FROM (SELECT toUInt32(999999 - number) % 1000000 AS k FROM numbers(1000000)) GROUP BY k ORDER BY k ASC LIMIT 3;

SELECT 'freeze path ran';
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['AggregationTopKHeapsFrozen']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select';

-- Tie overflow after skipped rows: the heap freezes, later rows for a
-- previously skipped key build a fresh partial group, and the synthesized
-- sort+limit must discard it (it ranks below the ~1.2M complete tied groups).
--   row 0:           key (0.0, nan)  -- admitted, becomes the boundary
--   row 1:           key (1.0, 0.0)  -- worse, skipped
--   rows 2..~1.2M:   keys (0.0, nan_i), bit-distinct NaN payloads -- tie with
--                    the boundary, grow the tie-set past capacity + 2^20, and
--                    keep going long enough that the freeze (checked at block
--                    starts) fires before the tail arrives
--   tail rows:       key (1.0, 0.0) again -- admitted into the frozen table as
--                    a fresh group missing row 1's contribution

SET max_block_size = 65536;

SELECT 'tie overflow after skips';
SELECT count() FROM
(
    SELECT k1, k2, count() AS cnt
    FROM
    (
        SELECT
            if(number = 1 OR number >= 1200000, 1.0, 0.0) AS k1,
            if(number = 1 OR number >= 1200000, 0.0, reinterpret(0x7FF0000000000001 + number, 'Float64')) AS k2
        FROM numbers(1200100)
    )
    GROUP BY k1, k2
    LIMIT 1
) SETTINGS log_comment = '04501_tie_overflow_skips';

-- No row of the partial (1.0, 0.0) group may surface.
SELECT k1, cnt FROM
(
    SELECT k1, k2, count() AS cnt
    FROM
    (
        SELECT
            if(number = 1 OR number >= 1200000, 1.0, 0.0) AS k1,
            if(number = 1 OR number >= 1200000, 0.0, reinterpret(0x7FF0000000000001 + number, 'Float64')) AS k2
        FROM numbers(1200100)
    )
    GROUP BY k1, k2
    LIMIT 1
);

SYSTEM FLUSH LOGS query_log;

SELECT
    sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0 AS skipped,
    sum(ProfileEvents['AggregationTopKHeapsFrozen']) AS frozen
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04501_tie_overflow_skips'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();

-- Evicting a group from the top-K heap must reuse its aggregate-state arena
-- slot.  A descending key stream under `ORDER BY k ASC LIMIT N` admits every
-- new key and evicts an older one, so without reuse the arena grows by one
-- state per distinct key seen (20M here) even though the hash table stays
-- bounded, defeating the optimization's memory contract for non-`count`
-- aggregates and failing the memory limit below.  The limit is per-query, not a
-- session `SET`: the verification query below reads `system.query_log`, whose
-- cost tracks the whole suite's log volume rather than anything under test.

SELECT 'arena slot reuse under eviction';
SELECT k, sum(v) FROM
(
    SELECT 20000000 - number AS k, number AS v FROM numbers(20000000)
)
GROUP BY k
ORDER BY k ASC
LIMIT 10
SETTINGS log_comment = '04501_state_arena_reuse', max_memory_usage = 100000000;

SYSTEM FLUSH LOGS query_log;

-- Prove the eviction path actually ran (otherwise this test guards nothing).
SELECT sum(ProfileEvents['AggregationTopKKeysEvicted']) > 1000000 AS evicted
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04501_state_arena_reuse'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();

DROP TABLE gt_low_cardinality_eviction;
DROP TABLE gt_consecutive_key_cache;
DROP TABLE gt_zeros_tied;
DROP TABLE gt_tie_overflow;
DROP TABLE gt_freeze_smaller_keys;
DROP TABLE gt_no_order_by_freeze;
DROP TABLE gt_composite_no_order_by_freeze;
