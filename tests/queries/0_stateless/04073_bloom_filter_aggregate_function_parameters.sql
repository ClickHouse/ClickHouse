-- Tests for groupBloomFilter aggregate function -- parameters and validation

-- Accepted parameter forms
WITH
    (SELECT groupBloomFilterState(1000, 0.01)(number) FROM numbers(100)) AS explicit_fpr_bf,
    (SELECT groupBloomFilterState(4096, 5)(number) FROM numbers(100)) AS explicit_size_bf,
    (SELECT groupBloomFilterState(1000, 0.01, 42)(number) FROM numbers(100)) AS explicit_seed_bf,
    (SELECT groupBloomFilterState(number) FROM numbers(100)) AS default_bf,
    (SELECT groupBloomFilterState(number) FROM numbers(10)) AS default_type_bf,
    (
        SELECT groupBloomFilterMergeState(state)
        FROM
        (
            SELECT groupBloomFilterState(toUInt64(number)) AS state FROM numbers(10)
            UNION ALL
            SELECT groupBloomFilterState(toUInt64(number + 100)) AS state FROM numbers(10)
        )
    ) AS merged_default_bf,
    (SELECT groupBloomFilterState(1000, 1)(number) FROM numbers(100)) AS integer_second_param_bf,
    (SELECT groupBloomFilterState(1000, 0.999)(number) FROM numbers(10)) AS high_fpr_bf,
    (SELECT groupBloomFilterState(4096, 20)(number) FROM numbers(100)) AS max_hashes_bf,
    (SELECT groupBloomFilterState(100, 1e-10)(number) FROM numbers(100)) AS tiny_fpr_bf
SELECT
    bloomFilterContains(explicit_fpr_bf, toUInt64(50)),
    bloomFilterContains(explicit_size_bf, toUInt64(50)),
    bloomFilterContains(explicit_seed_bf, toUInt64(50)),
    bloomFilterContains(default_bf, toUInt64(42)),
    bloomFilterContains(default_bf, toUInt64(200)),
    toTypeName(default_type_bf) LIKE 'AggregateFunction(groupBloomFilter%',
    bloomFilterContains(merged_default_bf, toUInt64(5)),
    bloomFilterContains(integer_second_param_bf, toUInt64(50)),
    toTypeName(high_fpr_bf) LIKE 'AggregateFunction(groupBloomFilter%',
    bloomFilterContains(max_hashes_bf, toUInt64(50)),
    bloomFilterContains(tiny_fpr_bf, toUInt64(42));

-- expected_elements = 0 must throw
SELECT groupBloomFilterState(0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- filter_size_bytes = 0 must throw
SELECT groupBloomFilterState(0, 5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- num_hashes = 0 must throw
SELECT groupBloomFilterState(1000, 0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- num_hashes above the maximum allowed value must throw
SELECT groupBloomFilterState(4096, 21)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- false_positive_rate = 0.0 must throw
SELECT groupBloomFilterState(1000, 0.0)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- Integer-like parameters must not allow lossy conversion
SELECT groupBloomFilterState(1000, 1.5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupBloomFilterState(1000.5, 5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupBloomFilterState(1000, 5, 42.5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
SELECT groupBloomFilterState(1000.5, 0.01)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- More than 3 parameters must throw
SELECT groupBloomFilterState(1000, 0.01, 0, 99)(number) FROM numbers(10); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Auto-sized parameters requiring a filter larger than the maximum allowed size must throw before casting to size_t
SELECT groupBloomFilterState(1000000000000, 0.0000001)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- Auto-sized parameters around the maximum allowed size must throw if the rounded filter would be too large
SELECT groupBloomFilterState(200000000, 0.0001)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }

-- Explicit filter size above the maximum allowed size must throw
SELECT groupBloomFilterState(268435464, 5)(number) FROM numbers(10); -- { serverError BAD_ARGUMENTS }
