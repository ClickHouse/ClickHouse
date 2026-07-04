SET optimize_constant_columns_after_filter = 1;

DROP TABLE IF EXISTS constant_column_after_filter_write;
DROP TABLE IF EXISTS constant_column_after_filter_projection_write;

CREATE TABLE constant_column_after_filter_write
(
    event_type String,
    message String
)
ENGINE = MergeTree
ORDER BY message
SETTINGS min_bytes_for_wide_part = 1000000000;

INSERT INTO constant_column_after_filter_write
SELECT event_type, message
FROM values('event_type String, message String', ('pageview', 'a'), ('click', 'b'), ('pageview', 'c'))
WHERE event_type = 'pageview';

SELECT 'insert-select';
SELECT event_type, message
FROM constant_column_after_filter_write
ORDER BY message;

CREATE TABLE constant_column_after_filter_projection_write
(
    time DateTime,
    event_type String,
    message String
)
ENGINE = MergeTree
ORDER BY time
SETTINGS min_bytes_for_wide_part = 1000000000;

ALTER TABLE constant_column_after_filter_projection_write ADD PROJECTION pageview_projection
(
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

INSERT INTO constant_column_after_filter_projection_write VALUES
    ('2024-01-01 00:00:00', 'pageview', 'a'),
    ('2024-01-02 00:00:00', 'click', 'b'),
    ('2024-01-03 00:00:00', 'pageview', 'c');

SELECT 'projection';
SELECT event_type, time, message
FROM constant_column_after_filter_projection_write
WHERE event_type = 'pageview'
ORDER BY time
SETTINGS force_optimize_projection = 1;

SELECT sum(rows)
FROM system.projection_parts
WHERE database = currentDatabase()
    AND table = 'constant_column_after_filter_projection_write'
    AND name = 'pageview_projection'
    AND active;

DROP TABLE constant_column_after_filter_write;
DROP TABLE constant_column_after_filter_projection_write;
