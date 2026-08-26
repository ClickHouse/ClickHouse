-- Tags: no-parallel-replicas

-- Text-index direct read aborted the server when a text-search predicate used a derived expression
-- (mapValues(attributes)) over a Merge -> Distributed -> MergeTree plan. See the commit message.

SET allow_experimental_full_text_index = 1;
SET enable_full_text_index = 1;
-- The old analyzer builds the WHERE FilterStep DAG with mapValues(attributes) as a named input.
SET enable_analyzer = 0;
SET prefer_localhost_replica = 1;
-- Pin the trigger setting; the runner randomizes it and would otherwise skip the crash path.
SET query_plan_direct_read_from_text_index = 1;

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

INSERT INTO logs VALUES (1, {'ip':'192.168.1.1'}, 'a'), (2, {'ip':'10.0.0.1'}, 'b');

CREATE TABLE logs_dist AS logs ENGINE = Distributed(test_shard_localhost, currentDatabase(), logs);
CREATE TABLE logs_merge AS logs ENGINE = Merge(currentDatabase(), '^logs_dist$');

-- The abort trigger: text-search predicate over the derived mapValues(attributes). Previously
-- aborted; must return 0 (no token '0' matches the indexed array values).
SELECT count() FROM logs_merge
PREWHERE hasAnyTokens(mapValues(attributes), toLowCardinality('0'))
WHERE has(mapValues(attributes), '192.168.1.1');

-- Same shape with a matching row: the result must be correct, not merely non-crashing.
-- row 1 has attribute value '192.168.1.1' -> PREWHERE keeps it, WHERE keeps it -> count 1.
SELECT count() FROM logs_merge
PREWHERE hasAnyTokens(mapValues(attributes), '192.168.1.1')
WHERE has(mapValues(attributes), '192.168.1.1');

-- Direct read must stay engaged over the SAME Merge topology: the synthetic
-- __text_index_attributes_vals_idx column is present in the plan with the setting on and absent with
-- it off. The toLowCardinality needle is load-bearing: with a plain String needle this shape does not
-- reach the path the first query above exercises, so the oracle would pass without covering it.
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM logs_merge
    PREWHERE hasAnyTokens(mapValues(attributes), toLowCardinality('0'))
    WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_attributes_vals_idx%';

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM logs_merge
    PREWHERE hasAnyTokens(mapValues(attributes), toLowCardinality('0'))
    WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS query_plan_direct_read_from_text_index = 0
)
WHERE explain ILIKE '%__text_index_attributes_vals_idx%';

DROP TABLE logs_merge;
DROP TABLE logs_dist;
DROP TABLE logs;
