-- Tags: no-parallel

-- Test for STID 1635-2ab2: constraint column substitution must not corrupt
-- correlated column references in subqueries, causing "Bad cast from type
-- DB::FunctionNode to DB::ColumnNode" crash.
--
-- Root cause: ComponentCollectorVisitor and SubstituteColumnVisitor in
-- ConvertQueryToCNFPass descended into QUERY/UNION children, collecting and
-- substituting correlated column references from the constraint graph. When a
-- substitution replaced a ColumnNode with a FunctionNode (e.g. cityHash64(a)),
-- the correlated_columns_list was corrupted, and CollectTopLevelColumnIdentifiers
-- crashed on a bad cast.

DROP TABLE IF EXISTS t_constraint_corr;

CREATE TABLE t_constraint_corr
(
    i Int64,
    a String,
    b UInt64,
    CONSTRAINT c1 ASSUME b = cityHash64(a)
)
ENGINE = MergeTree ORDER BY i
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_constraint_corr VALUES (1, 'cat', 1), (2, 'dog', 2);
INSERT INTO t_constraint_corr SELECT number AS i, toString(number) AS a, 1 AS b FROM numbers(1000);

-- Case 1: The original AST fuzzer crash query (simplified).
-- Without the fix, this aborts with LOGICAL_ERROR (Bad cast FunctionNode -> ColumnNode).
-- With the fix, the query executes successfully.
SELECT count() FROM t_constraint_corr
WHERE exists(
    (SELECT toUInt8(1) PREWHERE murmurHash3_64(xxHash32(a, intDiv(2, xxHash32(0, toInt256(0) = b))), multiply(10, 10)))
)
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_using_constraints = 1;

-- Case 2: Correlated subquery with transitivity constraints.
-- The constraint optimizer correctly deduces a=d transitively (a=b, b=c, c=d),
-- so "a != (SELECT d)" simplifies to False, and count() = 0.
DROP TABLE IF EXISTS t_constraint_trans;

CREATE TABLE t_constraint_trans
(
    a Int64,
    b Int64,
    c Int64,
    d Int32,
    CONSTRAINT c1 ASSUME (a = b) AND (c = d),
    CONSTRAINT c2 ASSUME b = c
)
ENGINE = TinyLog;

INSERT INTO t_constraint_trans VALUES (1, 2, 3, 4);

SELECT count() IGNORE NULLS FROM t_constraint_trans
WHERE a != (SELECT d)
SETTINGS enable_analyzer = 1, convert_query_to_cnf = 1, optimize_substitute_columns = 1, optimize_using_constraints = 1;

DROP TABLE t_constraint_trans;
DROP TABLE t_constraint_corr;
