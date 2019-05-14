SET send_logs_level = 'none';

DROP TABLE IF EXISTS test;
CREATE TABLE test(date Date, id Int8, name String, value Int64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY (id, date);

INSERT INTO test VALUES('2000-01-01', 1, 'test string 1', 1, 1);
INSERT INTO test VALUES('2000-01-01', 2, 'test string 2', 2, 1);

SET enable_optimize_predicate_expression = 1;

SELECT '-------ENABLE OPTIMIZE PREDICATE-------';
SELECT * FROM (SELECT * FROM test FINAL) WHERE id = 1;
SELECT * FROM (SELECT * FROM test LIMIT 1) WHERE id = 1;
SELECT * FROM (SELECT id FROM test GROUP BY id LIMIT 1 BY id) WHERE id = 1;

SET force_primary_key = 1;

SELECT '-------FORCE PRIMARY KEY-------';
SELECT * FROM (SELECT * FROM test FINAL) WHERE id = 1; -- { serverError 277 }
SELECT * FROM (SELECT * FROM test LIMIT 1) WHERE id = 1; -- { serverError 277 }
SELECT * FROM (SELECT id FROM test GROUP BY id LIMIT 1 BY id) WHERE id = 1; -- { serverError 277 }

SELECT '-------CHECK STATEFUL FUNCTIONS-------';
SELECT n, z, changed FROM (
  SELECT n, z, runningDifferenceStartingWithFirstValue(n) AS changed FROM (
     SELECT ts, n,z FROM system.one ARRAY JOIN [1,3,4,5,6] AS ts,
        [1,2,2,2,1] AS n, ['a', 'a', 'b', 'a', 'b'] AS z
      ORDER BY n, ts DESC
  )
) WHERE changed = 0;


SELECT arrayJoin(arrayMap(x -> x, arraySort(groupArray((ts, n))))) AS k FROM (
  SELECT ts, n, z FROM system.one ARRAY JOIN [1, 3, 4, 5, 6] AS ts, [1, 2, 2, 2, 1] AS n, ['a', 'a', 'b', 'a', 'b'] AS z
  ORDER BY n ASC, ts DESC
) WHERE z = 'a' GROUP BY z;


DROP TABLE IF EXISTS test;
