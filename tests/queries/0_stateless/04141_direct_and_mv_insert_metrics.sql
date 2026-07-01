DROP TABLE IF EXISTS mv_metrics_src;
DROP TABLE IF EXISTS mv_metrics_dst;
DROP VIEW IF EXISTS mv_metrics_mv;

CREATE TABLE mv_metrics_src
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE mv_metrics_dst
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id;

CREATE MATERIALIZED VIEW mv_metrics_mv TO mv_metrics_dst AS
SELECT id, s
FROM mv_metrics_src;

CREATE TEMPORARY TABLE expected_events
(
    event String
)
ENGINE = Memory;

INSERT INTO expected_events VALUES
    ('DirectInsertedRows'),
    ('DirectInsertedBytes'),
    ('MaterializedViewInsertedRows'),
    ('MaterializedViewInsertedBytes');

CREATE TEMPORARY TABLE before_events AS
SELECT
    expected_events.event,
    ifNull(system.events.value, 0) AS value
FROM expected_events
LEFT JOIN system.events ON expected_events.event = system.events.event;

INSERT INTO mv_metrics_src
SELECT
    number,
    toString(number)
FROM numbers(10);

CREATE TEMPORARY TABLE after_events AS
SELECT
    expected_events.event,
    ifNull(system.events.value, 0) AS value
FROM expected_events
LEFT JOIN system.events ON expected_events.event = system.events.event;

SELECT count() FROM mv_metrics_src;
SELECT count() FROM mv_metrics_dst;

SELECT
    after_events.event,
    after_events.value - before_events.value AS delta
FROM after_events
INNER JOIN before_events USING event
WHERE event IN
(
    'DirectInsertedRows',
    'MaterializedViewInsertedRows'
)
ORDER BY event;

SELECT
    after_events.event,
    after_events.value > before_events.value AS increased
FROM after_events
INNER JOIN before_events USING event
WHERE event IN
(
    'DirectInsertedBytes',
    'MaterializedViewInsertedBytes'
)
ORDER BY event;

DROP VIEW mv_metrics_mv;
DROP TABLE mv_metrics_src;
DROP TABLE mv_metrics_dst;
