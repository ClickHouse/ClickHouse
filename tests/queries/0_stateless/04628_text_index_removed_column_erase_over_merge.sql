-- Tags: no-parallel-replicas

-- Text-index direct read aborted the server when a text-search predicate used a derived expression
-- (mapValues(attributes)) over a Merge -> Distributed -> MergeTree plan. See the commit message.

SET enable_full_text_index = 1;
-- The old analyzer builds the WHERE FilterStep DAG with mapValues(attributes) as a named input.
SET enable_analyzer = 0;
SET prefer_localhost_replica = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS logs;
DROP TABLE IF EXISTS logs_dist;
DROP TABLE IF EXISTS logs_merge;

CREATE TABLE logs
(
    ts DateTime,
    attributes Map(String, String),
    msg String,
    INDEX attributes_vals_idx mapValues(attributes) TYPE text(tokenizer = 'array'),
    INDEX attributes_keys_idx mapKeys(attributes) TYPE text(tokenizer = 'array'),
    INDEX msg_idx msg TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY ts;

INSERT INTO logs VALUES (1, {'ip':'192.168.1.1'}, 'a'), (2, {'ip':'10.0.0.1'}, 'b');

CREATE TABLE logs_dist AS logs ENGINE = Distributed(test_shard_localhost, currentDatabase(), logs);
CREATE TABLE logs_merge AS logs ENGINE = Merge(currentDatabase(), '^logs_dist$');

SELECT 'No matching token';
SELECT count() FROM logs_merge
PREWHERE hasAnyTokens(mapValues(attributes), toLowCardinality('0'))
WHERE has(mapValues(attributes), '192.168.1.1');

SELECT 'Matching token';
SELECT count() FROM logs_merge
PREWHERE hasAnyTokens(mapValues(attributes), '192.168.1.1')
WHERE has(mapValues(attributes), '192.168.1.1');

-- The toLowCardinality needle is load-bearing: a plain String needle does not reach the WHERE filter
-- step, so the oracles below would pass without covering the path the queries above take.
SELECT 'Direct read on';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM logs_merge
    PREWHERE hasAnyTokens(mapValues(attributes), toLowCardinality('0'))
    WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_attributes_vals_idx%';

SELECT 'Direct read off';
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT count() FROM logs_merge
    PREWHERE hasAnyTokens(mapValues(attributes), toLowCardinality('0'))
    WHERE has(mapValues(attributes), '192.168.1.1')
    SETTINGS query_plan_direct_read_from_text_index = 0
)
WHERE explain ILIKE '%__text_index_attributes_vals_idx%';

-- mapValues(attributes) is typed in the reading step's header only. A second occurrence means the
-- filter step above it was widened by a column its DAG no longer takes as an input.
SELECT 'Filter header not widened';
SELECT count() FROM
(
    EXPLAIN header = 1
    SELECT count() FROM logs_merge
    PREWHERE hasAnyTokens(mapValues(attributes), toLowCardinality('0'))
    WHERE has(mapValues(attributes), '192.168.1.1')
)
WHERE explain LIKE '%mapValues(attributes) Array(String)%';

DROP TABLE logs_merge;
DROP TABLE logs_dist;
DROP TABLE logs;
