SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_prewhere = 1;
SET use_skip_indexes = 1;
SET enable_parallel_replicas = 0;

CREATE TABLE empty_prewhere_text_index_snapshot
(
    id UInt64,
    msg String,
    INDEX msg_text msg TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO empty_prewhere_text_index_snapshot VALUES (1, 'foo');

SELECT count()
FROM empty_prewhere_text_index_snapshot
PREWHERE hasToken(msg, 'foo') AND id IN (SELECT toUInt64(1) WHERE false);

DROP TABLE empty_prewhere_text_index_snapshot;
