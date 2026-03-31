-- Tests for the Sparkbar aggregate function combinator.
-- {fn}Sparkbar(width, begin_x, end_x)(x_col, ...) applies the nested aggregate function
-- per x-axis bucket and renders the results as a Unicode sparkbar string.

-- Basic: countSparkbar on integers, uniform distribution across 5 buckets
SELECT countSparkbar(5, 0, 4)(number) FROM numbers(5);

-- sumSparkbar: sum values that increase linearly across 5 buckets
SELECT sumSparkbar(5, 0, 4)(number, toFloat64(number + 1)) FROM numbers(5);

-- avgSparkbar: constant values in each bucket → all bars equal height
SELECT avgSparkbar(4, 0, 3)(number % 4, 10.0) FROM numbers(8);

-- Empty result set → empty string
SELECT countSparkbar(5, 0, 4)(number) FROM numbers(0);

-- Single bucket with data, rest empty
SELECT sumSparkbar(5, 0, 4)(0, 1.0) FROM numbers(1);

-- Date axis: count rows per month across 12 months
SELECT countSparkbar(12, toDate('2023-01-01'), toDate('2023-12-31'))(toDate('2023-01-01') + (number % 12) * 28) FROM numbers(120);

-- Values outside the specified range are silently ignored
SELECT sumSparkbar(5, 2, 6)(number, 1.0) FROM numbers(10);

-- Result type is String
SELECT toTypeName(countSparkbar(5, 0, 4)(number)) FROM numbers(5);

-- Width of 2 (minimum allowed)
SELECT countSparkbar(2, 0, 9)(number) FROM numbers(10);

-- Error: wrong number of parameters
SELECT countSparkbar(5)(number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT countSparkbar(5, 0)(number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Error: width out of range
SELECT countSparkbar(1, 0, 9)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT countSparkbar(1025, 0, 9)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- Error: begin_x >= end_x
SELECT countSparkbar(5, 5, 5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT countSparkbar(5, 9, 0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- Error: no key argument
SELECT countSparkbar(5, 0, 9)() FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
