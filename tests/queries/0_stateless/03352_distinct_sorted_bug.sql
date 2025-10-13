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
