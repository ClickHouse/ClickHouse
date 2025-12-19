-- Test for JSONExtract Decimal precision preservation
-- This test verifies that JSONExtract correctly handles decimal values
-- without losing precision when extracting from JSON numbers
-- Fixes issue #69082

DROP TABLE IF EXISTS test_json_decimal_precision;

CREATE TABLE test_json_decimal_precision
(
    id UInt32,
    json_data String
) ENGINE = Memory;

-- Insert test data with decimal values that could lose precision
INSERT INTO test_json_decimal_precision VALUES
(1, '{"dec": 111244542.003}'),
(2, '{"dec": "111244542.003"}'),
(3, '{"dec": 123.4567890123456789}'),
(4, '{"dec": "123.4567890123456789"}'),
(5, '{"dec": 0.0000000000000001}'),
(6, '{"dec": "0.0000000000000001"}'),
(7, '{"dec": 999999999999999.9999999999999999}'),
(8, '{"dec": "999999999999999.9999999999999999"}'),
(9, '{"dec": 1.23456789012345678901234567890123456789}'),
(10, '{"dec": "1.23456789012345678901234567890123456789"}'),
(11, '{"dec": 0.00000000000000000000000000000000000001}'),
(12, '{"dec": "0.00000000000000000000000000000000000001"}'),
(13, '{"dec": 0.1}'),
(14, '{"dec": "0.1"}'),
(15, '{"dec": 0.123456789012345678901234567890123456789}'),
(16, '{"dec": "0.123456789012345678901234567890123456789"}');

-- Test 1: Verify that JSONExtract preserves precision for numeric JSON values
SELECT 
    'Test 1: JSONExtract with numeric JSON values' as test_name,
    id,
    json_data,
    JSONExtract(json_data, 'dec', 'Decimal(32,16)') as extracted_decimal,
    JSONExtractString(json_data, 'dec')::Decimal(32,16) as string_cast_decimal,
    JSONExtract(json_data, 'dec', 'Decimal(32,16)') = JSONExtractString(json_data, 'dec')::Decimal(32,16) as precision_preserved
FROM test_json_decimal_precision
WHERE id IN (1, 3, 5, 7, 9, 11, 13, 15)
ORDER BY id;

-- Test 2: Verify that JSONExtract preserves precision for string JSON values
SELECT 
    'Test 2: JSONExtract with string JSON values' as test_name,
    id,
    json_data,
    JSONExtract(json_data, 'dec', 'Decimal(32,16)') as extracted_decimal,
    JSONExtractString(json_data, 'dec')::Decimal(32,16) as string_cast_decimal,
    JSONExtract(json_data, 'dec', 'Decimal(32,16)') = JSONExtractString(json_data, 'dec')::Decimal(32,16) as precision_preserved
FROM test_json_decimal_precision
WHERE id IN (2, 4, 6, 8, 10, 12, 14, 16)
ORDER BY id;

-- Test 3: Verify specific precision values match expected results
SELECT 
    'Test 3: Specific precision verification' as test_name,
    id,
    json_data,
    JSONExtract(json_data, 'dec', 'Decimal(32,16)') as extracted_decimal,
    JSONExtractString(json_data, 'dec')::Decimal(32,16) as string_cast_decimal,
    JSONExtract(json_data, 'dec', 'Decimal(32,16)') = JSONExtractString(json_data, 'dec')::Decimal(32,16) as precision_preserved
FROM test_json_decimal_precision
ORDER BY id;

-- Test 4: Verify that the fix resolves the original issue from #69082
SELECT 
    'Test 4: Original issue #69082 verification' as test_name,
    JSONExtract('{"dec": 111244542.003}', 'dec', 'Decimal(31,16)') as direct_extraction,
    JSONExtractString('{"dec": 111244542.003}', 'dec')::Decimal(32,16) as string_cast,
    toString(JSONExtract('{"dec": 111244542.003}', 'dec', 'Decimal(31,16)')) as direct_extraction_str,
    toString(JSONExtractString('{"dec": 111244542.003}', 'dec')::Decimal(32,16)) as string_cast_str,
    JSONExtract('{"dec": 111244542.003}', 'dec', 'Decimal(31,16)') = JSONExtractString('{"dec": 111244542.003}', 'dec')::Decimal(32,16) as issue_resolved;

-- Test 5: Edge cases with very large and very small numbers
SELECT 
    'Test 5: Edge cases with extreme precision' as test_name,
    id,
    json_data,
    JSONExtract(json_data, 'dec', 'Decimal(38,37)') as extracted_decimal,
    JSONExtractString(json_data, 'dec')::Decimal(38,37) as string_cast_decimal,
    JSONExtract(json_data, 'dec', 'Decimal(38,37)') = JSONExtractString(json_data, 'dec')::Decimal(38,37) as precision_preserved
FROM test_json_decimal_precision
WHERE id IN (9, 10, 11, 12)
ORDER BY id;

-- Test 6: Performance test with multiple values
SELECT 
    'Test 6: Performance test with multiple values' as test_name,
    count() as total_rows,
    countIf(JSONExtract(json_data, 'dec', 'Decimal(32,16)') = JSONExtractString(json_data, 'dec')::Decimal(32,16)) as precision_preserved_count,
    countIf(JSONExtract(json_data, 'dec', 'Decimal(32,16)') != JSONExtractString(json_data, 'dec')::Decimal(32,16)) as precision_lost_count
FROM test_json_decimal_precision;

-- Test 7: Verify that the fix works with different decimal scales
SELECT 
    'Test 7: Different decimal scales' as test_name,
    JSONExtract('{"dec": 123.456}', 'dec', 'Decimal(10,2)') as scale_2,
    JSONExtract('{"dec": 123.456}', 'dec', 'Decimal(10,3)') as scale_3,
    JSONExtract('{"dec": 123.456}', 'dec', 'Decimal(10,4)') as scale_4,
    JSONExtract('{"dec": 123.456}', 'dec', 'Decimal(10,5)') as scale_5;

-- Test 8: Verify that the fix works with negative numbers
SELECT 
    'Test 8: Negative numbers' as test_name,
    JSONExtract('{"dec": -111244542.003}', 'dec', 'Decimal(32,16)') as negative_extraction,
    JSONExtractString('{"dec": -111244542.003}', 'dec')::Decimal(32,16) as negative_string_cast,
    JSONExtract('{"dec": -111244542.003}', 'dec', 'Decimal(32,16)') = JSONExtractString('{"dec": -111244542.003}', 'dec')::Decimal(32,16) as negative_precision_preserved;

-- Test 9: Simple case that demonstrates the fix
SELECT 
    'Test 9: Simple precision demonstration' as test_name,
    JSONExtract('{"amount": 16.4}', 'amount', 'Decimal64(6)') as json_extract_result,
    CAST(JSONExtractString('{"amount": 16.4}', 'amount'), 'Decimal64(6)') as string_cast_result,
    JSONExtract('{"amount": 16.4}', 'amount', 'Decimal64(6)') = CAST(JSONExtractString('{"amount": 16.4}', 'amount'), 'Decimal64(6)') as precision_preserved; 