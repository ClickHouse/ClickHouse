-- Tags: no-parallel-replicas

-- Reading a Buffer whose destination declares a column differently logs a warning per read.
SET send_logs_level = 'error';
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t04849_ti_dst;
DROP TABLE IF EXISTS t04849_ti_buf;
DROP TABLE IF EXISTS t04849_rev_dst;
DROP TABLE IF EXISTS t04849_rev_buf;
DROP TABLE IF EXISTS t04849_lf_dst;
DROP TABLE IF EXISTS t04849_lf_buf;
DROP TABLE IF EXISTS t04849_same_dst;
DROP TABLE IF EXISTS t04849_same_buf;

-- Every arm stores 32 matching rows out of 64, so an all-rows or no-rows answer is visible.
-- Each arm is paired with a control that disables only the consumer under test: the control
-- must return the same count, or the arm cannot tell a fix from a wrong result.

CREATE TABLE t04849_ti_dst (id UInt64, version UInt64, str Array(String),
    INDEX idx (str) TYPE text(tokenizer = array) GRANULARITY 100000000)
ENGINE = ReplacingMergeTree(version) ORDER BY id;
INSERT INTO t04849_ti_dst SELECT number, 1, if(number % 2 = 0, ['bar', 'baz'], ['baz']) FROM numbers(64);
CREATE TABLE t04849_ti_buf (id UInt64, version UInt64, str Array(LowCardinality(String)))
    ENGINE = Buffer(currentDatabase(), t04849_ti_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

CREATE TABLE t04849_rev_dst (id UInt64, version UInt64, str Array(LowCardinality(String)),
    INDEX idx (str) TYPE text(tokenizer = array) GRANULARITY 100000000)
ENGINE = ReplacingMergeTree(version) ORDER BY id;
INSERT INTO t04849_rev_dst SELECT number, 1, if(number % 2 = 0, ['bar', 'baz'], ['baz']) FROM numbers(64);
CREATE TABLE t04849_rev_buf (id UInt64, version UInt64, str Array(String))
    ENGINE = Buffer(currentDatabase(), t04849_rev_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- The types agree here, so this arm pins that the fix does not regress the ordinary case.
CREATE TABLE t04849_same_dst (id UInt64, version UInt64, str Array(String),
    INDEX idx (str) TYPE text(tokenizer = array) GRANULARITY 100000000)
ENGINE = ReplacingMergeTree(version) ORDER BY id;
INSERT INTO t04849_same_dst SELECT number, 1, if(number % 2 = 0, ['bar', 'baz'], ['baz']) FROM numbers(64);
CREATE TABLE t04849_same_buf (id UInt64, version UInt64, str Array(String))
    ENGINE = Buffer(currentDatabase(), t04849_same_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- Lazy FINAL rewrites the filter against a second read it builds for the deduplicating keys, so
-- it needs more than one part holding the same key.
CREATE TABLE t04849_lf_dst (id UInt64, version UInt64, str Array(String))
ENGINE = ReplacingMergeTree(version) ORDER BY id;
SYSTEM STOP MERGES t04849_lf_dst;
INSERT INTO t04849_lf_dst SELECT number, 1, if(number % 2 = 0, ['bar', 'baz'], ['baz']) FROM numbers(64);
INSERT INTO t04849_lf_dst SELECT number, 2, if(number % 2 = 0, ['bar', 'baz'], ['baz']) FROM numbers(64);
CREATE TABLE t04849_lf_buf (id UInt64, version UInt64, str Array(LowCardinality(String)))
    ENGINE = Buffer(currentDatabase(), t04849_lf_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- Both filters read the converted column, so the PREWHERE prefix must leave the pass-through
-- output typed as the destination produces it.
-- `query_plan_direct_read_from_text_index` is randomized off ~5%; pin it on so the arm reaches
-- the consumer it is written for.
SELECT 'A text index direct read, PREWHERE and WHERE on the converted column';
SELECT count() FROM t04849_ti_buf PREWHERE hasAllTokens(str, 'bar') WHERE hasAllTokens(str, 'bar')
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'A control, skip indexes off';
SELECT count() FROM t04849_ti_buf PREWHERE hasAllTokens(str, 'bar') WHERE hasAllTokens(str, 'bar')
SETTINGS use_skip_indexes = 0;

SELECT 'B single PREWHERE on the converted column';
SELECT count() FROM t04849_ti_buf PREWHERE hasAllTokens(str, 'bar')
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'B control, skip indexes off';
SELECT count() FROM t04849_ti_buf PREWHERE hasAllTokens(str, 'bar') SETTINGS use_skip_indexes = 0;

-- The header match is by name, so the opposite type direction reaches the same defect.
SELECT 'R reversed types, PREWHERE and WHERE on the converted column';
SELECT count() FROM t04849_rev_buf PREWHERE hasAllTokens(str, 'bar') WHERE hasAllTokens(str, 'bar')
SETTINGS query_plan_direct_read_from_text_index = 1;

SELECT 'R control, skip indexes off';
SELECT count() FROM t04849_rev_buf PREWHERE hasAllTokens(str, 'bar') WHERE hasAllTokens(str, 'bar')
SETTINGS use_skip_indexes = 0;

SELECT 'S matching types, PREWHERE and WHERE on the same column';
SELECT count() FROM t04849_same_buf PREWHERE hasAllTokens(str, 'bar') WHERE hasAllTokens(str, 'bar')
SETTINGS query_plan_direct_read_from_text_index = 1;

-- The direct read is in the plan here, so this file exercises that consumer for real. The
-- diverging arms above cannot carry this assertion: once the pass-through keeps the
-- destination's type, their haystack is a CAST rather than a bare column and the rewrite
-- declines, which is why they pin the setting instead.
SELECT 'S plan, the direct read from the text index is in the plan';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM t04849_same_buf PREWHERE hasAllTokens(str, 'bar') WHERE hasAllTokens(str, 'bar')
    SETTINGS query_plan_direct_read_from_text_index = 1
) WHERE explain ILIKE '%__text_index_idx%';

SELECT 'S control, skip indexes off';
SELECT count() FROM t04849_same_buf PREWHERE hasAllTokens(str, 'bar') WHERE hasAllTokens(str, 'bar')
SETTINGS use_skip_indexes = 0;

-- Lazy FINAL clones the filter onto its own read, which advertises the Buffer's type. Skip indexes
-- are not involved on this path, so the control turns off lazy FINAL instead.
-- `min_filtered_ratio_for_lazy_final = 0` avoids the fallback to regular FINAL, which randomized
-- `index_granularity` can otherwise trigger.
SELECT 'L lazy FINAL, PREWHERE and WHERE on the converted column';
SELECT count() FROM t04849_lf_buf FINAL PREWHERE has(str, 'bar') WHERE has(str, 'bar')
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;

-- Lazy FINAL is in the plan, so the arm above cannot pass by falling back to ordinary FINAL.
SELECT 'L plan, lazy FINAL is in the plan';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 0
    SELECT count() FROM t04849_lf_buf FINAL PREWHERE has(str, 'bar') WHERE has(str, 'bar')
    SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0
) WHERE explain LIKE '%InputSelector%';

SELECT 'L control, lazy FINAL off';
SELECT count() FROM t04849_lf_buf FINAL PREWHERE has(str, 'bar') WHERE has(str, 'bar')
SETTINGS query_plan_optimize_lazy_final = 0;

SELECT 'M lazy FINAL, single predicate';
SELECT count() FROM t04849_lf_buf FINAL PREWHERE has(str, 'bar')
SETTINGS query_plan_optimize_lazy_final = 1, min_filtered_ratio_for_lazy_final = 0;

SELECT 'M control, lazy FINAL off';
SELECT count() FROM t04849_lf_buf FINAL PREWHERE has(str, 'bar')
SETTINGS query_plan_optimize_lazy_final = 0;

DROP TABLE t04849_ti_buf;
DROP TABLE t04849_ti_dst;
DROP TABLE t04849_rev_buf;
DROP TABLE t04849_rev_dst;
DROP TABLE t04849_same_buf;
DROP TABLE t04849_same_dst;
DROP TABLE t04849_lf_buf;
DROP TABLE t04849_lf_dst;
