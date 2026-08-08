SET explain_query_plan_default = 'legacy';
SET query_plan_optimize_prewhere = 1;
SET optimize_move_to_prewhere = 1;
SET use_skip_indexes_on_data_read = 0;

CREATE TABLE t_empty_in_prewhere
(
    id UInt64,
    uuid UUID,
    INDEX uuid_bf uuid TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO t_empty_in_prewhere VALUES
    (1, '00000000-0000-0000-0000-000000000001'),
    (2, '00000000-0000-0000-0000-000000000002');

SELECT id
FROM t_empty_in_prewhere
WHERE uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000001') WHERE false);

SELECT count()
FROM viewExplain('EXPLAIN', 'indexes = 1',
    (SELECT id
     FROM t_empty_in_prewhere
     WHERE uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000001') WHERE false)))
WHERE explain LIKE '%Name: uuid_bf%';

SELECT id
FROM t_empty_in_prewhere
WHERE uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000001'));

SELECT count()
FROM viewExplain('EXPLAIN', 'indexes = 1',
    (SELECT id
     FROM t_empty_in_prewhere
     WHERE uuid IN (SELECT toUUID('00000000-0000-0000-0000-000000000001'))))
WHERE explain LIKE '%Name: uuid_bf%';

DROP TABLE t_empty_in_prewhere;
