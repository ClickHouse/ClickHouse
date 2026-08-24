SET param_test_a=30;
SELECT * REPLACE({test_a:UInt32} as number) FROM numbers(2);
