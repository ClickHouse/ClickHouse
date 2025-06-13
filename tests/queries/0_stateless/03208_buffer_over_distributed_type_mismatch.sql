
DROP TABLE IF EXISTS realtimedrep;
CREATE TABLE realtimedrep (`amount` Int32) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO realtimedrep FORMAT Values (100);

DROP TABLE IF EXISTS realtimedistributed;
CREATE TABLE realtimedistributed (`amount` Int32) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), realtimedrep, rand());

DROP TABLE IF EXISTS realtimebuff__fuzz_19;
CREATE TABLE realtimebuff__fuzz_19 (`amount` UInt32) ENGINE = Buffer(currentDatabase(), 'realtimedistributed', 16, 3600, 36000, 10000, 1000000, 10000000, 100000000);
INSERT INTO realtimebuff__fuzz_19 FORMAT Values (101);

DROP TABLE IF EXISTS realtimebuff__fuzz_20;
CREATE TABLE realtimebuff__fuzz_20 (`amount` Nullable(Int32)) ENGINE = Buffer(currentDatabase(), 'realtimedistributed', 16, 3600, 36000, 10000, 1000000, 10000000, 100000000);
INSERT INTO realtimebuff__fuzz_20 FORMAT Values (101);

SELECT amount FROM realtimebuff__fuzz_19 t1 ORDER BY ALL;
SELECT amount + 1 FROM realtimebuff__fuzz_19 t1 ORDER BY ALL;
SELECT amount + 1 FROM realtimebuff__fuzz_20 t1 ORDER BY ALL;
SELECT sum(amount) = 100 FROM realtimebuff__fuzz_19 ORDER BY ALL; -- { serverError CANNOT_CONVERT_TYPE }
SELECT sum(amount) = 100 FROM realtimebuff__fuzz_20 ORDER BY ALL; -- { serverError CANNOT_CONVERT_TYPE }

SELECT amount FROM realtimebuff__fuzz_19 t1
JOIN (SELECT number :: UInt32 AS amount FROM numbers(3) ) t2 ON t1.amount = t2.amount
ORDER BY ALL
SETTINGS allow_experimental_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }

SELECT amount FROM realtimebuff__fuzz_19 t1
JOIN (SELECT number :: UInt32 AS amount FROM numbers(3) ) t2 ON t1.amount = t2.amount
ORDER BY ALL
SETTINGS allow_experimental_analyzer = 1;

SELECT amount FROM realtimebuff__fuzz_19 t1
JOIN (SELECT number :: UInt32 AS amount FROM numbers(300) ) t2 ON t1.amount = t2.amount
ORDER BY ALL
SETTINGS allow_experimental_analyzer = 0; -- { serverError UNKNOWN_IDENTIFIER }

SELECT amount FROM realtimebuff__fuzz_19 t1
JOIN (SELECT number :: UInt32 AS amount FROM numbers(300) ) t2 ON t1.amount = t2.amount
ORDER BY ALL
SETTINGS allow_experimental_analyzer = 1;

SELECT t2.amount + 1 FROM (SELECT number :: UInt32 AS amount FROM numbers(300) ) t1
JOIN realtimebuff__fuzz_19 t2 USING (amount)
ORDER BY ALL
;

SELECT t2.amount + 1 FROM (SELECT number :: UInt32 AS amount FROM numbers(300) ) t1
JOIN realtimebuff__fuzz_19 t2 ON t1.amount = t2.amount
ORDER BY ALL
;

SELECT amount FROM realtimebuff__fuzz_19 t1
JOIN realtimebuff__fuzz_19 t2 ON t1.amount = t2.amount
; -- { serverError NOT_IMPLEMENTED,UNKNOWN_IDENTIFIER }

SELECT amount FROM realtimebuff__fuzz_19 t1
JOIN realtimebuff__fuzz_19 t2 ON t1.amount = t2.amount
JOIN realtimebuff__fuzz_19 t3 ON t1.amount = t3.amount
; -- { serverError NOT_IMPLEMENTED,AMBIGUOUS_COLUMN_NAME }


-- fuzzers:

SELECT
    toLowCardinality(1) + materialize(toLowCardinality(2))
FROM realtimebuff__fuzz_19
GROUP BY toLowCardinality(1)
FORMAT Null
;

SELECT intDivOrZero(intDivOrZero(toLowCardinality(-128), toLowCardinality(-1)) = 0, materialize(toLowCardinality(4)))
FROM realtimebuff__fuzz_19 GROUP BY materialize(toLowCardinality(-127)), intDivOrZero(0, 0) = toLowCardinality(toLowCardinality(0))
WITH TOTALS ORDER BY ALL DESC NULLS FIRST
FORMAT Null
;
