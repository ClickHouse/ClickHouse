-- Test text-index PREWHERE direct read when ReadFromMergeTree is the query-plan root (no WHERE/aggregation above it)

DROP TABLE IF EXISTS t_text_root;
SET allow_experimental_full_text_index = 1;
-- Direct read is disabled under parallel replicas; pin it off so the optimization is deterministic.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_text_root;

CREATE TABLE t_text_root
(
    msg String,
    INDEX msg_idx msg TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_text_root VALUES ('alpha beta'), ('gamma delta'), ('alpha zzz');

-- Root-level MergeTree read with a text-index PREWHERE and nothing above the reading step.
SELECT msg FROM t_text_root
PREWHERE hasAnyTokens(msg, ['alpha'])
ORDER BY msg
SETTINGS query_plan_direct_read_from_text_index = 1;

-- Direct read from the text index is engaged (synthetic column present in the plan).
SELECT count() > 0 FROM
(
    EXPLAIN actions = 1
    SELECT msg FROM t_text_root
    PREWHERE hasAnyTokens(msg, ['alpha'])
    SETTINGS query_plan_direct_read_from_text_index = 1
)
WHERE explain ILIKE '%__text_index_msg_idx%';

DROP TABLE IF EXISTS t_text_root;
