-- Tags: no-parallel-replicas

-- Regression test for #110697.

SET enable_analyzer = 1;
SET allow_experimental_full_text_index = 1;
-- The abort only reproduces with the whole plan built on the local initiator.
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

-- `query_plan_direct_read_from_text_index` is randomized off ~5%; pin it on so the abort path is exercised.
SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE has(mapValues(attributes), toNullable('192.168.1.1'))
SETTINGS force_data_skipping_indices = 'attributes_vals_idx', query_plan_direct_read_from_text_index = 1;

-- The direct-read column stays in the plan (optimization preserved).
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM logs_merge
    PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
    WHERE has(mapValues(attributes), toNullable('192.168.1.1'))
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_attributes_vals_idx_has%';

SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE has(mapValues(attributes), toNullable('10.0.0.1'))
SETTINGS force_data_skipping_indices = 'attributes_vals_idx';

-- Direct read off then on must give the same result (the `WHERE` tokenizer rewrite is preserved).
SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE hasAnyTokens(msg, ['delta'])
SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT count() FROM logs_merge
PREWHERE has(mapValues(attributes), toNullable('192.168.1.1'))
WHERE hasAnyTokens(msg, ['delta'])
SETTINGS query_plan_direct_read_from_text_index = 1;

-- The 3-argument tokenizer rewrite (`splitByNonAlpha`) survives on the re-visited step.
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
