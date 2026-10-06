-- Tags: no-parallel-replicas

-- Regression test for a query-plan optimizer abort: the same text-index predicate placed in both
-- PREWHERE and WHERE, walked through a Merge -> Distributed -> MergeTree plan, made
-- optimizeDirectReadFromTextIndex register the synthetic __text_index_..._has_<hash> read column
-- twice for the same reading step (the step is optimized on more than one pass), aborting with
-- "Column ... already added for reading". See https://github.com/ClickHouse/ClickHouse/issues/110697

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
-- The double-registration is reached only when the whole Merge -> Distributed -> MergeTree plan is
-- built and optimized on the initiator (local replica).
SET prefer_localhost_replica = 1;

DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS logs_dist;
DROP TABLE IF EXISTS logs_merge;

CREATE TABLE logs
(
    ts DateTime,
    attributes Map(String, String),
    msg String,
    INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = 'array') GRANULARITY 1,
    INDEX msg_idx msg TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY ts;

INSERT INTO logs VALUES (1, {'ip':'192.168.1.1'}, 'alpha beta'), (2, {'ip':'10.0.0.1'}, 'delta epsilon'), (3, {'ip':'192.168.1.1'}, 'delta zzz');

CREATE TABLE logs_dist AS logs ENGINE = Distributed(test_shard_localhost, currentDatabase(), logs);
CREATE TABLE logs_merge AS logs ENGINE = Merge(currentDatabase(), '^logs_dist$');

-- The trigger: identical text-index predicate in PREWHERE and WHERE over the Merge table.
-- force_data_skipping_indices guarantees the text-index direct-read path engages.
-- query_plan_direct_read_from_text_index is pinned to 1: the double-registration only happens when
-- direct read is actually enabled and the whole Merge -> Distributed -> MergeTree pipeline is built
-- (the abort is thrown during pipeline build, not plan optimization). The runner randomizes this
-- setting to 0 on ~5% of runs, which disables the optimization and would silently skip the crash
-- path, so this repro query must pin it to exercise the fix deterministically on every run.
SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE has(mapValues(attributes), toNullable('192.168.1.1'))
SETTINGS force_data_skipping_indices = 'attributes_vals_idx', query_plan_direct_read_from_text_index = 1;

-- Direct read from the text index must remain engaged (optimization preserved, not disabled).
-- query_plan_direct_read_from_text_index is pinned to 1 here: it is randomized by the test runner and
-- when off it disables the whole optimization, so the __text_index_..._has_<hash> column would be
-- absent and this visibility check would spuriously report the optimization as disabled.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM logs_merge
    PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
    WHERE has(mapValues(attributes), toNullable('192.168.1.1'))
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_attributes_vals_idx_has%';

-- Non-equal predicates in PREWHERE and WHERE still return correct results.
SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE has(mapValues(attributes), toNullable('10.0.0.1'))
SETTINGS force_data_skipping_indices = 'attributes_vals_idx';

-- Direct-read PREWHERE predicate plus a DIFFERENT text-index predicate in WHERE that uses a
-- tokenizing text function (hasAnyTokens). The fix gates only the direct-read virtual-column
-- registration on a re-visited step; the tokenizer/preprocessor rewrite of the WHERE predicate must
-- still run so row-level evaluation returns the same result whether direct read is on or off.
-- Compare both values of query_plan_direct_read_from_text_index: if the rewrite were dropped, the two
-- would diverge. rows with ip 192.168.1.1 = {1,3}; of those hasAnyTokens(msg, ['delta']) matches {3}.
SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE hasAnyTokens(msg, ['delta'])
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE hasAnyTokens(msg, ['delta'])
SETTINGS query_plan_direct_read_from_text_index = 1;

-- The WHERE tokenizing predicate keeps its tokenizer rewrite (the 3-argument form with the index
-- tokenizer 'splitByNonAlpha' appended) even though the step is re-visited after the PREWHERE already
-- registered a direct-read column. This is the exact second-pass rewrite the fix preserves.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM logs_merge
    PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
    WHERE hasAnyTokens(msg, ['delta'])
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%hasAnyTokens(%splitByNonAlpha%';

DROP TABLE logs_merge;
DROP TABLE logs_dist;
DROP TABLE logs;
