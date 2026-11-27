DROP TABLE IF EXISTS t;

CREATE TABLE t
(
    i Int32
)
ENGINE = MergeTree
PARTITION BY i
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO t SELECT number FROM numbers(3);

SELECT arraySort(groupArray(i))
FROM t
WHERE tuple(i, i) NOT IN (tuple(1, 2));


SELECT arraySort(groupArray(i))
FROM t
WHERE NOT has([tuple(1, 2)], (i, i));
