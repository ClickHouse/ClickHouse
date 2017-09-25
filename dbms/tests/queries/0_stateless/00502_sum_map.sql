DROP TABLE IF EXISTS test.sum_map;
CREATE TABLE test.sum_map(date Date, timeslot DateTime, statusMap Nested(status UInt16, requests UInt64)) ENGINE = Log;

INSERT INTO test.sum_map VALUES ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10]), ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10]), ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10]), ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10]);

SELECT * FROM test.sum_map ORDER BY timeslot;
SELECT sumMap(statusMap.status, statusMap.requests) FROM test.sum_map;
SELECT sumMapMerge(s) FROM (SELECT sumMapState(statusMap.status, statusMap.requests) AS s FROM test.sum_map);
SELECT timeslot, sumMap(statusMap.status, statusMap.requests) FROM test.sum_map GROUP BY timeslot ORDER BY timeslot;
SELECT timeslot, sumMap(statusMap.status, statusMap.requests).1, sumMap(statusMap.status, statusMap.requests).2 FROM test.sum_map GROUP BY timeslot ORDER BY timeslot;

DROP TABLE test.sum_map;
