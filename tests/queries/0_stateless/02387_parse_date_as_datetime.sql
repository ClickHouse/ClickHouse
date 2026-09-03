CREATE TEMPORARY TABLE test (`i` Int64, `d` DateTime);

INSERT INTO test FORMAT JSONEachRow {"i": 123, "d": "2022-05-03"};

INSERT INTO test FORMAT JSONEachRow {"i": 456, "d": "2022-05-03 01:02:03"};

SELECT * FROM test ORDER BY i;

DROP TABLE test;

CREATE TEMPORARY TABLE test (`i` Int64, `d` DateTime64);

INSERT INTO test FORMAT JSONEachRow {"i": 123, "d": "2022-05-03"};

INSERT INTO test FORMAT JSONEachRow {"i": 456, "d": "2022-05-03 01:02:03"};

SELECT * FROM test ORDER BY i;

DROP TABLE test;
