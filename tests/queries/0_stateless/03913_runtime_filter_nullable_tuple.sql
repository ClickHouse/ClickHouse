-- Reproducer for a bug where __applyFilter runtime filter returned Nullable(UInt8) instead of UInt8
-- when joining on Tuple columns with Nullable subfields.

SET allow_suspicious_low_cardinality_types = 1;
SET enable_analyzer = 1;

-- Simple reproducer
SELECT * FROM
    (SELECT (toNullable(42), 43) AS c FROM numbers(1)) AS t1
    JOIN
    (SELECT (42, 43) AS c FROM numbers(1)) AS t2
    USING (c);

-- Original reproducer by fuzzer
DROP TABLE IF EXISTS test_table__fuzz_2;
DROP TABLE IF EXISTS test_table__fuzz_4;

CREATE TABLE test_table__fuzz_2 (`id` LowCardinality(UInt16), `value` Tuple(value_0_level_0 Tuple(value_0_level_1 Nullable(String), value_1_level_1 String), value_1_level_0 Nullable(String))) ENGINE = MergeTree ORDER BY id;
CREATE TABLE test_table__fuzz_4 (`id` LowCardinality(UInt16), `value` Tuple(value_0_level_0 Tuple(value_0_level_1 LowCardinality(Nullable(String)), value_1_level_1 String), value_1_level_0 Nullable(String))) ENGINE = MergeTree ORDER BY id;

INSERT INTO test_table__fuzz_2 VALUES (1, (('a', 'b'), 'c'));
INSERT INTO test_table__fuzz_4 VALUES (1, (('a', 'b'), 'c'));

SELECT value AS alias_value FROM test_table__fuzz_4 INNER JOIN (SELECT isNullable(isNotNull(38)), * FROM test_table__fuzz_2) AS alias272 ON equals(value AS alias_value, alias272.value) GROUP BY less(toNullable(38) AS alias174, assumeNotNull(2)), 1, equals(isZeroOrNull(1), toUInt128(materialize(toNullable(toUInt128(38))))) WITH CUBE WITH TOTALS;

DROP TABLE IF EXISTS test_table__fuzz_2;
DROP TABLE IF EXISTS test_table__fuzz_4;
