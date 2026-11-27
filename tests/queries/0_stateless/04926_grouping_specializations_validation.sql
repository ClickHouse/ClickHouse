-- The `grouping` specializations (`__groupingOrdinary` etc.) are registered functions, so they can
-- be called directly with arbitrary arguments; malformed calls must raise ordinary errors, not
-- logical ones. Also, `grouping` over a `LowCardinality` key must return plain `UInt64`.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_grouping_validation;
CREATE TABLE t_grouping_validation (k1 String, k2 LowCardinality(String), v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_grouping_validation VALUES ('a', 'x', 1), ('b', 'y', 2);

SELECT '-- grouping over a LowCardinality key stays UInt64';
EXPLAIN QUERY TREE SELECT k1, k2, grouping(k1) + grouping(k2) AS level, sum(v)
FROM t_grouping_validation GROUP BY k1, k2;
SELECT k1, k2, grouping(k1) + grouping(k2) AS level, sum(v)
FROM t_grouping_validation GROUP BY k1, k2 ORDER BY ALL;

SELECT '-- a well-formed direct call executes';
SELECT __groupingForRollup(CAST(0 AS UInt64), 42, CAST([0] AS Array(UInt64)), CAST(1 AS UInt64), CAST(1 AS UInt8));

SELECT '-- malformed direct calls are rejected';
SELECT __groupingForCube(CAST(-8.9757207546686 AS Decimal(38, 13)), CAST(['1970-01-14'] AS Array(Date)), CAST('1970-01-05' AS Date), CAST(-11448 AS Int32)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __groupingForCube(CAST([] AS Array(Int16)), CAST(27 AS UInt64), CAST(1 AS UInt8)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __groupingForRollup(CAST(1.5 AS Float64), materialize(2), CAST([] AS Array(UInt32)), CAST(4 AS UInt64), CAST(9282217 AS UInt64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __groupingForCube(materialize(CAST(0 AS UInt64)), 42, CAST([70] AS Array(UInt64)), CAST(3 AS UInt64), CAST(1 AS UInt8)); -- { serverError BAD_ARGUMENTS }
SELECT __groupingForGroupingSets(materialize(CAST(5 AS UInt64)), 42, CAST([0] AS Array(UInt64)), CAST([[0]] AS Array(Array(UInt64))), CAST(1 AS UInt8)); -- { serverError BAD_ARGUMENTS }
SELECT __groupingForRollup(materialize(CAST(5 AS UInt64)), 42, CAST([0] AS Array(UInt64)), CAST(1 AS UInt64), CAST(1 AS UInt8)); -- { serverError BAD_ARGUMENTS }
SELECT __groupingForCube(materialize(CAST(8 AS UInt64)), 42, CAST([0] AS Array(UInt64)), CAST(3 AS UInt64), CAST(1 AS UInt8)); -- { serverError BAD_ARGUMENTS }
SELECT __groupingForRollup(CAST(0 AS UInt64), 1, 2, CAST([0] AS Array(UInt64)), CAST(2 AS UInt64), CAST(1 AS UInt8)); -- { serverError BAD_ARGUMENTS }
SELECT __groupingForRollup(CAST(0 AS UInt64), CAST([0] AS Array(UInt64)), CAST(1 AS UInt64), CAST(1 AS UInt8)); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT __groupingOrdinary(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, CAST(range(64) AS Array(UInt64)), CAST(0 AS UInt8)); -- { serverError TOO_MANY_COLUMNS }
-- The result carries one bit per argument, so 64 arguments are the maximum and 65 are rejected.
SELECT __groupingForRollup(materialize(CAST(0 AS UInt64)), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, CAST(range(64) AS Array(UInt64)), CAST(100 AS UInt64), CAST(0 AS UInt8));
SELECT __groupingForRollup(materialize(CAST(0 AS UInt64)), 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, CAST(range(65) AS Array(UInt64)), CAST(100 AS UInt64), CAST(0 AS UInt8)); -- { serverError TOO_MANY_COLUMNS }

SELECT '-- a direct call shipped to a remote server stays intact';
SELECT __groupingForRollup(CAST(0 AS UInt64), 42, CAST([0] AS Array(UInt64)), CAST(1 AS UInt64), CAST(1 AS UInt8)) FROM remote('127.0.0.{1,2}', system.one);

-- The shape of this call is identical to an analyzer-built node; the original AST tells the
-- rewrite for old remote servers to leave it alone.
SELECT '-- a direct call with the exact analyzer-built shape also ships unchanged';
SELECT __groupingOrdinary(dummy, CAST([0] AS Array(UInt64)), CAST(0 AS UInt8)) FROM remote('127.0.0.{1,2}', system.one);

DROP TABLE t_grouping_validation;
