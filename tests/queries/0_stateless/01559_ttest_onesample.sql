-- Test one-sample Student t-test
-- This test verifies the basic functionality of the one-sample t-test

-- Test with a simple case where sample mean differs from population mean
SELECT studentTTestOneSample(value, 0.0) as result
FROM (
    SELECT 1.0 as value
    UNION ALL SELECT 2.0
    UNION ALL SELECT 3.0
    UNION ALL SELECT 4.0
    UNION ALL SELECT 5.0
);

-- Test with confidence interval
SELECT studentTTestOneSample(0.95)(value, 0.0) as result
FROM (
    SELECT 1.0 as value
    UNION ALL SELECT 2.0
    UNION ALL SELECT 3.0
    UNION ALL SELECT 4.0
    UNION ALL SELECT 5.0
);

-- Test with sample mean close to population mean (should not reject null hypothesis)
SELECT studentTTestOneSample(value, 3.0) as result
FROM (
    SELECT 2.8 as value
    UNION ALL SELECT 3.0
    UNION ALL SELECT 3.2
    UNION ALL SELECT 2.9
    UNION ALL SELECT 3.1
);

-- Test with insufficient data (should return NaN)
SELECT studentTTestOneSample(value, 0.0) as result
FROM (
    SELECT 1.0 as value
);

-- Test with constant data (should return NaN)
SELECT studentTTestOneSample(value, 0.0) as result
FROM (
    SELECT 1.0 as value
    UNION ALL SELECT 1.0
    UNION ALL SELECT 1.0
); 
