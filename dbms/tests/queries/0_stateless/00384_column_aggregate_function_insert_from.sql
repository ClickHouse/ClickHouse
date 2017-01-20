DROP TABLE IF EXISTS test.aggregates;
CREATE TABLE test.aggregates (d Date, s AggregateFunction(uniq, UInt64)) ENGINE = MergeTree(d, d, 8192);

INSERT INTO test.aggregates
    SELECT toDate('2016-10-31') AS d, uniqState(toUInt64(arrayJoin(range(100)))) AS s
    UNION ALL
    SELECT toDate('2016-11-01') AS d, uniqState(toUInt64(arrayJoin(range(100)))) AS s;

INSERT INTO test.aggregates SELECT toDate('2016-10-31') + number AS d, uniqState(toUInt64(arrayJoin(range(100)))) AS s FROM (SELECT * FROM system.numbers LIMIT 2) GROUP BY d;

SELECT d, uniqMerge(s) FROM test.aggregates GROUP BY d ORDER BY d;

INSERT INTO test.aggregates
    SELECT toDate('2016-12-01') AS d, uniqState(toUInt64(arrayJoin(range(100)))) AS s
    UNION ALL
    SELECT toDate('2016-12-02') AS d, uniqState(toUInt64(arrayJoin(range(100)))) AS s
    UNION ALL
    SELECT toDate('2016-12-03') AS d, uniqState(toUInt64(arrayJoin(range(100)))) AS s;

SELECT d, uniqMerge(s) FROM test.aggregates GROUP BY d ORDER BY d;

DROP TABLE test.aggregates;
