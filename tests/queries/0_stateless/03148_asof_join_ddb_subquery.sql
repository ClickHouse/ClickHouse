DROP TABLE IF EXISTS events;
CREATE TABLE events (begin Float64, value Int32) ENGINE = MergeTree() ORDER BY begin;

INSERT INTO events VALUES (1, 0), (3, 1), (6, 2), (8, 3);

SET allow_experimental_analyzer = 1;
SET join_algorithm = 'full_sorting_merge';
SET joined_subquery_requires_alias = 0;

SELECT
    begin,
    value IN (
        SELECT e1.value
        FROM (
            SELECT *
            FROM events e1
            WHERE e1.value = events.value
        ) AS e1
        ASOF JOIN (
            SELECT number :: Float64 AS begin
            FROM numbers(10)
            WHERE number >= 1 AND number < 10
        )
        USING (begin)
    )
FROM events
ORDER BY begin ASC;

DROP TABLE IF EXISTS events;
