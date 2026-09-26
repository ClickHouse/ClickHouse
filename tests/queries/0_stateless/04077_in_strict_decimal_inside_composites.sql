-- { echo }

-- Strict Decimal conversion must reject lossy precision inside composite types
-- (Tuple, Array, Map), not just at the top level.

-- Scalar Decimal->Decimal: exact and lossy
SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.30', 'Decimal64(2)'));  -- 1: exact
SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'));  -- 0: lossy

-- Scalar Float64->Decimal: exact and lossy
SELECT CAST('33.3', 'Decimal64(1)') IN (33.3);   -- 1: exact
SELECT CAST('33.3', 'Decimal64(1)') IN (33.33);  -- 0: lossy

-- Multiple values in IN: Decimal->Decimal
SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'), CAST('33.30', 'Decimal64(2)'));  -- 1: second exact
SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'), CAST('33.35', 'Decimal64(2)'));  -- 0: both lossy

-- Multiple values in IN: Float64->Decimal
SELECT CAST('33.3', 'Decimal64(1)') IN (33.33, 33.3);   -- 1: second exact
SELECT CAST('33.3', 'Decimal64(1)') IN (33.33, 33.35);  -- 0: both lossy

-- Materialized values: Decimal->Decimal
SELECT CAST(materialize('33.3'), 'Decimal64(1)') IN (CAST('33.30', 'Decimal64(2)'));  -- 1: exact
SELECT CAST(materialize('33.3'), 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'));  -- 0: lossy

-- Materialized values: Float64->Decimal
SELECT CAST(materialize('33.3'), 'Decimal64(1)') IN (33.3);   -- 1: exact
SELECT CAST(materialize('33.3'), 'Decimal64(1)') IN (33.33);  -- 0: lossy

-- Nullable: Decimal->Decimal
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (CAST('33.30', 'Decimal64(2)'));  -- 1: exact
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (CAST('33.33', 'Decimal64(2)'));  -- 0: lossy

-- Nullable: Float64->Decimal
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (33.3);   -- 1: exact
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (33.33);  -- 0: lossy

-- LowCardinality: Decimal->Decimal (LowCardinality does not support Decimal)
SELECT toLowCardinality(CAST('33.3', 'Decimal64(1)')) IN (CAST('33.30', 'Decimal64(2)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toLowCardinality(CAST('33.3', 'Decimal64(1)')) IN (CAST('33.33', 'Decimal64(2)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- LowCardinality: Float64->Decimal
SELECT toLowCardinality(CAST('33.3', 'Decimal64(1)')) IN (33.3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toLowCardinality(CAST('33.3', 'Decimal64(1)')) IN (33.33); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- LowCardinality(Nullable): Decimal->Decimal
SELECT toLowCardinality(CAST('33.3', 'Nullable(Decimal64(1))')) IN (CAST('33.30', 'Decimal64(2)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toLowCardinality(CAST('33.3', 'Nullable(Decimal64(1))')) IN (CAST('33.33', 'Decimal64(2)')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- LowCardinality(Nullable): Float64->Decimal
SELECT toLowCardinality(CAST('33.3', 'Nullable(Decimal64(1))')) IN (33.3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toLowCardinality(CAST('33.3', 'Nullable(Decimal64(1))')) IN (33.33); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Array: Decimal->Decimal
SELECT CAST(['33.3'], 'Array(Decimal64(1))') IN ([CAST('33.30', 'Decimal64(2)')]);  -- 1: exact
SELECT CAST(['33.3'], 'Array(Decimal64(1))') IN ([CAST('33.33', 'Decimal64(2)')]);  -- 0: lossy

-- Array: Float64->Decimal
SELECT CAST(['33.3'], 'Array(Decimal64(1))') IN ([33.3]);   -- 1: exact
SELECT CAST(['33.3'], 'Array(Decimal64(1))') IN ([33.33]);  -- 0: lossy

-- Tuple: Decimal->Decimal
SELECT (CAST('33.3', 'Decimal64(1)'), 1) IN ((CAST('33.30', 'Decimal64(2)'), 1));  -- 1: exact
SELECT (CAST('33.3', 'Decimal64(1)'), 1) IN ((CAST('33.33', 'Decimal64(2)'), 1));  -- 0: lossy

-- Tuple: Float64->Decimal
SELECT (CAST('33.3', 'Decimal64(1)'), 1) IN ((33.3, 1));   -- 1: exact
SELECT (CAST('33.3', 'Decimal64(1)'), 1) IN ((33.33, 1));  -- 0: lossy

-- Nested Tuple: Decimal->Decimal (lossy in outer element)
SELECT (CAST('33.3', 'Decimal64(1)'), (1, CAST('44.4', 'Decimal64(1)'))) IN ((CAST('33.30', 'Decimal64(2)'), (1, CAST('44.40', 'Decimal64(2)'))));  -- 1: exact
SELECT (CAST('33.3', 'Decimal64(1)'), (1, CAST('44.4', 'Decimal64(1)'))) IN ((CAST('33.33', 'Decimal64(2)'), (1, CAST('44.40', 'Decimal64(2)'))));  -- 0: outer lossy

-- Nested Tuple: Decimal->Decimal (lossy in inner element)
SELECT (CAST('33.3', 'Decimal64(1)'), (1, CAST('44.4', 'Decimal64(1)'))) IN ((CAST('33.30', 'Decimal64(2)'), (1, CAST('44.44', 'Decimal64(2)'))));  -- 0: inner lossy

-- Nested Tuple: Float64->Decimal
SELECT (CAST('33.3', 'Decimal64(1)'), (1, CAST('44.4', 'Decimal64(1)'))) IN ((33.3, (1, 44.4)));   -- 1: exact
SELECT (CAST('33.3', 'Decimal64(1)'), (1, CAST('44.4', 'Decimal64(1)'))) IN ((33.33, (1, 44.4)));  -- 0: outer lossy
SELECT (CAST('33.3', 'Decimal64(1)'), (1, CAST('44.4', 'Decimal64(1)'))) IN ((33.3, (1, 44.44)));  -- 0: inner lossy

-- Map: Decimal->Decimal
SELECT CAST(map('a', CAST('33.3', 'Decimal64(1)')), 'Map(String, Decimal64(1))') IN (map('a', CAST('33.30', 'Decimal64(2)')));  -- 1: exact
SELECT CAST(map('a', CAST('33.3', 'Decimal64(1)')), 'Map(String, Decimal64(1))') IN (map('a', CAST('33.33', 'Decimal64(2)')));  -- 0: lossy

-- Map: Float64->Decimal
SELECT CAST(map('a', CAST('33.3', 'Decimal64(1)')), 'Map(String, Decimal64(1))') IN (map('a', 33.3));   -- 1: exact
SELECT CAST(map('a', CAST('33.3', 'Decimal64(1)')), 'Map(String, Decimal64(1))') IN (map('a', 33.33));  -- 0: lossy

-- Table-based: Decimal->Decimal and Float64->Decimal
DROP TABLE IF EXISTS test_decimal_in;
CREATE TABLE test_decimal_in (d Decimal64(1)) ENGINE = MergeTree ORDER BY d;
INSERT INTO test_decimal_in VALUES (33.3), (44.4);
SELECT d FROM test_decimal_in WHERE d IN (CAST('33.30', 'Decimal64(2)'));                            -- 33.3
SELECT d FROM test_decimal_in WHERE d IN (CAST('33.33', 'Decimal64(2)'));                            -- empty
SELECT d FROM test_decimal_in WHERE d IN (33.3);                                                     -- 33.3
SELECT d FROM test_decimal_in WHERE d IN (33.33);                                                    -- empty
SELECT d FROM test_decimal_in WHERE d IN (CAST('33.30', 'Decimal64(2)'), CAST('44.40', 'Decimal64(2)')) ORDER BY d;  -- 33.3, 44.4
SELECT d FROM test_decimal_in WHERE d IN (CAST('33.33', 'Decimal64(2)'), CAST('44.44', 'Decimal64(2)'));             -- empty
DROP TABLE test_decimal_in;

-- NOT IN: Decimal->Decimal
SELECT CAST('33.3', 'Decimal64(1)') NOT IN (CAST('33.30', 'Decimal64(2)'));  -- 0: exact match
SELECT CAST('33.3', 'Decimal64(1)') NOT IN (CAST('33.33', 'Decimal64(2)'));  -- 1: lossy, excluded

-- NOT IN: Float64->Decimal
SELECT CAST('33.3', 'Decimal64(1)') NOT IN (33.3);   -- 0: exact match
SELECT CAST('33.3', 'Decimal64(1)') NOT IN (33.33);  -- 1: lossy, excluded

-- Mixed valid/invalid in IN: Decimal->Decimal
SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'), CAST('33.35', 'Decimal64(2)'), CAST('33.30', 'Decimal64(2)'));  -- 1: last exact
SELECT CAST('33.3', 'Decimal64(1)') IN (CAST('33.33', 'Decimal64(2)'), CAST('33.35', 'Decimal64(2)'), CAST('33.37', 'Decimal64(2)'));  -- 0: all lossy

-- Mixed valid/invalid in IN: Float64->Decimal
SELECT CAST('33.3', 'Decimal64(1)') IN (33.33, 33.35, 33.3);   -- 1: last exact
SELECT CAST('33.3', 'Decimal64(1)') IN (33.33, 33.35, 33.37);  -- 0: all lossy

-- NULL handling: Nullable Decimal with NULL in set
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (CAST('33.30', 'Decimal64(2)'), NULL);  -- 1: first exact
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (CAST('33.33', 'Decimal64(2)'), NULL);  -- 0: lossy, NULL doesn't help
SELECT CAST(NULL, 'Nullable(Decimal64(1))') IN (CAST('33.30', 'Decimal64(2)'), NULL);    -- \N: NULL IN (..., NULL)

-- NULL handling: Float64->Decimal with NULL
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (33.3, NULL);   -- 1: first exact
SELECT CAST('33.3', 'Nullable(Decimal64(1))') IN (33.33, NULL);  -- 0: lossy, NULL doesn't help
