-- Regression test: __topKFilter must return UInt8, matching its declared type, for every
-- ORDER BY column type. Comparisons over Dynamic, Variant, or a Tuple holding a Nullable
-- element resolve to Nullable(UInt8) and used to raise "Unexpected return type"
-- (STID 1611-483a).

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

-- ===== Tuple containing a Nullable element =====
-- The tuple itself is not Nullable, so a top-level nullability check does not catch it,
-- but its comparison resolves to Nullable(UInt8).

DROP TABLE IF EXISTS t_topk_tuple_nullable;

CREATE TABLE t_topk_tuple_nullable (
    id Int64,
    k  Tuple(UInt64, Nullable(UInt64))
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;

-- The three inserts must stay three parts: with a single part the threshold tracker is
-- never set before the read, so the filter is never exercised.
SYSTEM STOP MERGES t_topk_tuple_nullable;

INSERT INTO t_topk_tuple_nullable SELECT number, tuple(number, number) FROM numbers(0, 1000);
INSERT INTO t_topk_tuple_nullable SELECT number, tuple(number, number) FROM numbers(1000, 1000);
INSERT INTO t_topk_tuple_nullable SELECT number, tuple(number, number) FROM numbers(2000, 1000);

-- `query_plan_max_limit_for_top_k_optimization` is randomized in the flaky check; pin it so a
-- small value (e.g. 1) cannot disable the optimization and leave this arm covering nothing.
-- max_threads = 1 so the threshold is always set before the remaining parts are read.
SELECT k
FROM t_topk_tuple_nullable
ORDER BY k ASC NULLS LAST
LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_tuple_nullable ORDER BY k ASC NULLS LAST LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1
) WHERE explain LIKE '%__topKFilter%';

DROP TABLE t_topk_tuple_nullable;

-- ===== Tuple containing a LowCardinality(Nullable) element =====
-- Creatable without allow_suspicious_low_cardinality_types.

DROP TABLE IF EXISTS t_topk_tuple_lc_nullable;

CREATE TABLE t_topk_tuple_lc_nullable (
    id Int64,
    k  Tuple(LowCardinality(Nullable(String)))
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;

SYSTEM STOP MERGES t_topk_tuple_lc_nullable;

INSERT INTO t_topk_tuple_lc_nullable SELECT number, tuple(leftPad(toString(number), 8, '0')) FROM numbers(0, 1000);
INSERT INTO t_topk_tuple_lc_nullable SELECT number, tuple(leftPad(toString(number), 8, '0')) FROM numbers(1000, 1000);
INSERT INTO t_topk_tuple_lc_nullable SELECT number, tuple(leftPad(toString(number), 8, '0')) FROM numbers(2000, 1000);

-- max_threads = 1 so the threshold is always set before the remaining parts are read;
-- with more readers this arm only reaches the filter in about 7 runs out of 10.
SELECT k
FROM t_topk_tuple_lc_nullable
ORDER BY k ASC NULLS LAST
LIMIT 5
SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1,
         query_plan_max_limit_for_top_k_optimization = 100;

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_tuple_lc_nullable ORDER BY k ASC NULLS LAST LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1,
             query_plan_max_limit_for_top_k_optimization = 100
) WHERE explain LIKE '%__topKFilter%';

DROP TABLE t_topk_tuple_lc_nullable;

-- ===== The filter must not change the answer when NULLs are present =====
-- Each pair below emits the same rows with the filter on and off.
-- Element 0 repeats (`number % 5`) so that the Nullable element decides the order:
-- tuple comparison stops at the first differing element, so a distinct element 0 would
-- leave the NULL placement unobservable and NULLS LAST/FIRST byte-identical.

DROP TABLE IF EXISTS t_topk_tuple_nulls;

CREATE TABLE t_topk_tuple_nulls (
    id Int64,
    k  Tuple(UInt64, Nullable(UInt64))
) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 64;

SYSTEM STOP MERGES t_topk_tuple_nulls;

INSERT INTO t_topk_tuple_nulls SELECT number, tuple(number % 5, if(number % 97 = 0, NULL, number)) FROM numbers(0, 1000);
INSERT INTO t_topk_tuple_nulls SELECT number, tuple(number % 5, if(number % 97 = 0, NULL, number)) FROM numbers(1000, 1000);
INSERT INTO t_topk_tuple_nulls SELECT number, tuple(number % 5, if(number % 97 = 0, NULL, number)) FROM numbers(2000, 1000);

SELECT count() > 0 FROM (
    EXPLAIN actions = 1
    SELECT k FROM t_topk_tuple_nulls ORDER BY k ASC NULLS LAST LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1
) WHERE explain LIKE '%__topKFilter%';

SELECT k FROM t_topk_tuple_nulls ORDER BY k ASC NULLS LAST LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;
SELECT k FROM t_topk_tuple_nulls ORDER BY k ASC NULLS LAST LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 0, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;

SELECT k FROM t_topk_tuple_nulls ORDER BY k ASC NULLS FIRST LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;
SELECT k FROM t_topk_tuple_nulls ORDER BY k ASC NULLS FIRST LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 0, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;

SELECT k FROM t_topk_tuple_nulls ORDER BY k DESC NULLS LAST LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;
SELECT k FROM t_topk_tuple_nulls ORDER BY k DESC NULLS LAST LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 0, query_plan_max_limit_for_top_k_optimization = 100, max_threads = 1;

DROP TABLE t_topk_tuple_nulls;
