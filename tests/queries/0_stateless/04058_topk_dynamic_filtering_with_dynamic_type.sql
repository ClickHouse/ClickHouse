-- Regression test: use_top_k_dynamic_filtering must not crash when the ORDER BY column is
-- a Dynamic (or Variant) type.  Previously __topKFilter would return Nullable(UInt8) at
-- runtime while the query plan expected UInt8, triggering an "Unexpected return type"
-- logical error (STID 1611-483a).

SET allow_suspicious_types_in_order_by = 1;

-- ===== Dynamic column =====

DROP TABLE IF EXISTS t_topk_dynamic;

CREATE TABLE t_topk_dynamic (
    id Int64,
    v  Dynamic(max_types = 8),
    payload UInt64
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;

-- Insert in separate batches to produce multiple parts so the threshold tracker
-- gets set during the first part's merge and applied to subsequent parts.
INSERT INTO t_topk_dynamic SELECT number, number       AS v, number FROM numbers(1000);
INSERT INTO t_topk_dynamic SELECT number, number * 2   AS v, number FROM numbers(1000);
INSERT INTO t_topk_dynamic SELECT number, number * 3   AS v, number FROM numbers(1000);

-- Must not throw "Unexpected return type from __topKFilter" (previous crash).
-- The Dynamic sort column bypasses the dynamic-filter prewhere optimization;
-- correctness of the result is still verified by the ORDER BY.
SELECT v, payload
FROM t_topk_dynamic
ORDER BY v ASC, payload ASC
LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_dynamic;

-- ===== Variant column =====

DROP TABLE IF EXISTS t_topk_variant;

CREATE TABLE t_topk_variant (
    id Int64,
    v  Variant(Int64, String),
    payload UInt64
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;

INSERT INTO t_topk_variant SELECT number, toInt64(number)     AS v, number FROM numbers(1000);
INSERT INTO t_topk_variant SELECT number, toInt64(number * 2) AS v, number FROM numbers(1000);
INSERT INTO t_topk_variant SELECT number, toInt64(number * 3) AS v, number FROM numbers(1000);

-- Same crash scenario with Variant — must also be handled gracefully.
SELECT v, payload
FROM t_topk_variant
ORDER BY v ASC, payload ASC
LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1;

DROP TABLE t_topk_variant;
