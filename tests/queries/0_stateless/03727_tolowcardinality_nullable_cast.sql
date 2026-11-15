-- Test for issue #89412: Bad cast from ColumnNullable to ColumnLowCardinality

DROP TABLE IF EXISTS test_tolowcardinality_nullable;

-- Test 1: Original fiddle query from issue #89412
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (c0 Nullable(Int)) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY (toLowCardinality(c0)) SETTINGS allow_nullable_key = 1;
INSERT INTO TABLE t0 (c0) VALUES (0);
DELETE FROM t0 WHERE c0 = 1;
DROP TABLE t0;

-- Test 2: Using toLowCardinality with Nullable in PARTITION BY
CREATE TABLE test_tolowcardinality_nullable
(
    id UInt32,
    str Nullable(String)
)
ENGINE = MergeTree()
PARTITION BY toLowCardinality(str)
ORDER BY id
SETTINGS allow_nullable_key = 1;

INSERT INTO test_tolowcardinality_nullable VALUES (1, 'a'), (2, 'b'), (3, NULL), (4, 'a');

-- Query that triggers KeyCondition optimization with toLowCardinality
SELECT * FROM test_tolowcardinality_nullable WHERE toLowCardinality(str) = 'a' ORDER BY id;

-- Mutation that also uses the partition key
ALTER TABLE test_tolowcardinality_nullable DELETE WHERE id = 1 SETTINGS mutations_sync = 2;

SELECT * FROM test_tolowcardinality_nullable ORDER BY id;

DROP TABLE test_tolowcardinality_nullable;

-- Test 2: Direct toLowCardinality on Nullable column
SELECT toLowCardinality(materialize(toNullable('test'))) AS result;
SELECT toLowCardinality(materialize(CAST(NULL AS Nullable(String)))) AS result;

-- Test 3: toLowCardinality in WHERE clause with Nullable
DROP TABLE IF EXISTS test_tolowcardinality_where;

CREATE TABLE test_tolowcardinality_where
(
    id UInt32,
    val Nullable(String)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_tolowcardinality_where VALUES (1, 'x'), (2, 'y'), (3, NULL);

SELECT id FROM test_tolowcardinality_where WHERE toLowCardinality(val) = 'x' ORDER BY id;

DROP TABLE test_tolowcardinality_where;
