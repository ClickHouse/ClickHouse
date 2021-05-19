DROP TABLE IF EXISTS segment;

CREATE TABLE segment
(
    `id` String,
    `start` UInt64,
    `end` UInt64
)
ENGINE = MergeTree
ORDER BY start;

INSERT INTO segment VALUES ('a', 1, 3), ('a', 2, 4), ('a', 5, 6), ('a', 5, 7), ('b', 10, 12), ('b', 13, 19), ('b', 14, 16);

SELECT
    id,
    segmentLengthSum(start, end)
FROM segment
GROUP BY id
ORDER BY id;

DROP TABLE segment;
