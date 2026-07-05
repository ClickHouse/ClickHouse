DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Int) Engine = MergeTree() ORDER BY (c0);
INSERT INTO t0 VALUES (1);

-- ../src/Interpreters/AggregationCommon.h:90:35: runtime error: downcast of address 0x743320010d90 which does not point to an object of type 'const ColumnFixedSizeHelper'
-- 0x743320010d90: note: object is of type 'DB::ColumnConst'
--  00 00 00 00  b8 42 24 fb 47 5d 00 00  01 00 00 00 00 00 00 00  40 28 01 20 33 74 00 00  01 00 00 00
--               ^~~~~~~~~~~~~~~~~~~~~~~
--               vptr for 'DB::ColumnConst'
-- SUMMARY: UndefinedBehaviorSanitizer: undefined-behavior ../src/Interpreters/AggregationCommon.h:90:35
SELECT DISTINCT multiIf(1, 2, 1, materialize(toInt128(3)), 4), c0 FROM t0;

DROP TABLE IF EXISTS t0__fuzz_41;
CREATE TABLE t0__fuzz_41 (c0 DateTime) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t0__fuzz_41 FORMAT Values (1) (2) (3) (4) (5) (6) (7) (8) (9) (10);

SELECT multiIf(1, 2, 1, materialize(3), 4), c0 FROM t0__fuzz_41 FORMAT Null;
SELECT multiIf(0, 2, 1, materialize(3), 4), c0 FROM t0__fuzz_41 FORMAT Null;

SELECT DISTINCT subquery_1.id, subquery_2.id FROM (SELECT 1 AS id, 2 AS value) AS subquery_1, (SELECT 3 AS id,  4) AS subquery_2;
