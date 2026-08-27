-- The top-K threshold merge (Fagin's Threshold Algorithm) must return exactly the same result
-- as the ordinary merge. The data is constructed so that every asserted top/bottom set has
-- unique ordering values (no boundary ties), making the result deterministic; every query runs
-- with the optimization enabled and disabled, and both blocks must match the reference.

-- Force two-level aggregation states and several partial tables, so the threshold merge engages
-- even on this small dataset. The adaptive aggregator keeps the tables single-level while it is
-- staging (it converts only when the per-thread scan outgrows its watermarks, which needs far
-- more data), so it is pinned off to let `group_by_two_level_threshold` act; its interplay with
-- the threshold merge is covered by the events test.
SET enable_adaptive_aggregator = 0;
SET max_threads = 4;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_block_size = 4096;
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS threshold_top_k;
CREATE TABLE threshold_top_k (k UInt64, k2 UInt8, v UInt64, s String, u UInt64, u_null Nullable(UInt64), r UInt64)
    ENGINE = MergeTree ORDER BY ();

-- Head keys 0..99: key k appears (k + 1) times - the counts 1..100 are unique at the low end.
INSERT INTO threshold_top_k
    SELECT k, k % 7, k * 1000 + r, toString(k * 1000 + r), r, nullIf(r, 0), r
    FROM (SELECT number AS k FROM numbers(100)) ARRAY JOIN range(k + 1) AS r;
-- Head keys 100..199: key k appears (100 + k) times - the counts 200..299 are unique at the top.
INSERT INTO threshold_top_k
    SELECT k, k % 7, k * 1000 + r, toString(k * 1000 + r), r, nullIf(r, 0), r
    FROM (SELECT 100 + number AS k FROM numbers(100)) ARRAY JOIN range(100 + k) AS r;
-- Tail: 8000 keys with 8 rows each, so the bucket tables are much larger than the LIMIT. The
-- tail count of 8 stays away from every asserted boundary (the bottom counts 1..5, the top
-- counts 295..299, and the HAVING band 95..99 all come from the head with strictly distinct
-- neighbours), so every asserted top/bottom set stays free of boundary ties.
INSERT INTO threshold_top_k
    SELECT 200 + intDiv(number, 8), (200 + intDiv(number, 8)) % 7, (200 + intDiv(number, 8)) * 1000 + number % 8,
           toString((200 + intDiv(number, 8)) * 1000 + number % 8), number % 8, nullIf(number % 8, 0), number % 8
    FROM numbers(64000);

SELECT 'count() DESC';
SELECT k, count() AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, count() AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'count() ASC';
SELECT k, count() AS c FROM threshold_top_k GROUP BY k ORDER BY c ASC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, count() AS c FROM threshold_top_k GROUP BY k ORDER BY c ASC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'count() DESC with other aggregates';
SELECT k, count() AS c, min(v), max(v), uniqExact(u) FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, count() AS c, min(v), max(v), uniqExact(u) FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'count() DESC, LIMIT with OFFSET';
SELECT k, count() AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, count() AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'count() DESC, composite key';
SELECT k, k2, count() AS c FROM threshold_top_k GROUP BY k, k2 ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, k2, count() AS c FROM threshold_top_k GROUP BY k, k2 ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'count() DESC, String key';
SELECT toString(k) AS ks, count() AS c FROM threshold_top_k GROUP BY ks ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT toString(k) AS ks, count() AS c FROM threshold_top_k GROUP BY ks ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'uniqExact DESC';
SELECT k, uniqExact(u) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, uniqExact(u) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'uniqExact over Nullable DESC';
SELECT k, uniqExact(u_null) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, uniqExact(u_null) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'countIf DESC';
SELECT k, countIf(r >= 5) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, countIf(r >= 5) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'sum(u) DESC';
SELECT k, sum(u) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, sum(u) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'sum(u) ASC';
SELECT k, sum(u) AS c FROM threshold_top_k GROUP BY k ORDER BY c ASC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, sum(u) AS c FROM threshold_top_k GROUP BY k ORDER BY c ASC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'max(v) DESC';
SELECT k, max(v) AS m FROM threshold_top_k GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, max(v) AS m FROM threshold_top_k GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'min(v) ASC';
SELECT k, min(v) AS m FROM threshold_top_k GROUP BY k ORDER BY m ASC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, min(v) AS m FROM threshold_top_k GROUP BY k ORDER BY m ASC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

SELECT 'min(v) DESC';
SELECT k, min(v) AS m FROM threshold_top_k GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, min(v) AS m FROM threshold_top_k GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

-- String ordering values are excluded from the threshold merge (the up-front peek of every
-- cell's partial value would copy the whole ordering payload); the results must stay correct.
SELECT 'max(s) DESC (String values)';
SELECT k, max(s) AS m FROM threshold_top_k GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;
SELECT k, max(s) AS m FROM threshold_top_k GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 0;

-- Shapes the optimization must not break (it is skipped for them, but the results must stay correct).
SELECT 'HAVING';
SELECT k, count() AS c FROM threshold_top_k GROUP BY k HAVING c < 100 ORDER BY c DESC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;

SELECT 'ORDER BY aggregate, key';
SELECT k, uniqExact(u) AS c FROM threshold_top_k GROUP BY k ORDER BY c DESC, k ASC LIMIT 5 SETTINGS enable_aggregation_top_k_threshold_merge = 1;

SELECT 'WITH TOTALS';
SELECT k, count() AS c FROM threshold_top_k WHERE k >= 100 AND k < 200 GROUP BY k WITH TOTALS ORDER BY c DESC LIMIT 3 SETTINGS enable_aggregation_top_k_threshold_merge = 1;

SELECT 'LIMIT WITH TIES';
SELECT c FROM (SELECT k, count() AS c FROM threshold_top_k GROUP BY k) ORDER BY c DESC LIMIT 3 WITH TIES SETTINGS enable_aggregation_top_k_threshold_merge = 1;

DROP TABLE threshold_top_k;
