-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102074
-- `PREWHERE` column not included in read plan when used inside parameterized view with `JOIN`.

DROP TABLE IF EXISTS prewhere_bug_events;
DROP TABLE IF EXISTS prewhere_bug_dim;
DROP VIEW IF EXISTS prewhere_bug_view;

CREATE TABLE prewhere_bug_events
(
    id UInt64,
    project_id UInt64,
    event_created_at DateTime64(3),
    value String,
    is_deleted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree(event_created_at, is_deleted)
ORDER BY (project_id, id);

CREATE TABLE prewhere_bug_dim
(
    project_id UInt64,
    name String
) ENGINE = MergeTree()
ORDER BY project_id;

INSERT INTO prewhere_bug_events VALUES (1, 100, '2024-01-01 00:00:00.000', 'hello', 0);
INSERT INTO prewhere_bug_dim VALUES (100, 'project_a');

CREATE VIEW prewhere_bug_view AS
SELECT t.*, d.name
FROM (
    SELECT *
    FROM prewhere_bug_events FINAL
    PREWHERE project_id IN ({project_ids:Array(UInt64)})
        AND event_created_at <= toDateTime64({cutoff:String}, 3)
) AS t
ANY LEFT JOIN prewhere_bug_dim AS d
    ON t.project_id = d.project_id;

SELECT count(event_created_at) FROM prewhere_bug_view(project_ids=[100], cutoff='2099-01-01 00:00:00');
SELECT count() FROM prewhere_bug_view(project_ids=[100], cutoff='2099-01-01 00:00:00');

DROP VIEW prewhere_bug_view;
DROP TABLE prewhere_bug_dim;
DROP TABLE prewhere_bug_events;
