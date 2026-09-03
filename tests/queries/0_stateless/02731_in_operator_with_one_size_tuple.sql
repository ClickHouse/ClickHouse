CREATE TABLE test(`report_date` Date, `sspid` UInt64) ENGINE MergeTree PARTITION BY report_date ORDER BY report_date;

INSERT INTO test SELECT toDate('2023-04-20'), 0;
INSERT INTO test SELECT toDate('2023-04-19'), 0;
INSERT INTO test SELECT toDate('2023-04-17'), 1;
INSERT INTO test SELECT toDate('2023-04-17'), 1;


SELECT * FROM test  WHERE tuple(report_date) IN tuple(toDate('2023-04-17'));
DROP TABLE test;