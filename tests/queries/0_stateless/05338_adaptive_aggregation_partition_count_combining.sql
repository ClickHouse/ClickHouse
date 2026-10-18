-- Each input block starts with distinct keys to fill a worker's frozen table before repeated pairs arrive.
-- The suffix repeats keys in adjacent pairs, then revisits them after other keys, exercising both
-- consecutive-run counting and combining across intervening records in other partitions.
-- The two 16-byte keys share a `CRC32C` hash and exercise exact equality when a hash match alone cannot
-- identify a group.
-- Empty keys, embedded zero bytes, and long keys exercise the string record's length and byte comparisons.

SET max_threads = 4;
SET max_block_size = 16384;
SET adaptive_aggregator_freeze_threshold = 128;
SET adaptive_aggregator_disable_thaw = 1;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_adaptive_aggregator = 1;
SET query_plan_aggregation_bucket_top_k = 1;

CREATE TEMPORARY TABLE combining_input ENGINE = Memory AS
SELECT number AS n,
    multiIf(
        n % 16384 < 4096, concat('prefix_', toString(n)),
        intDiv(n, 2) % 23 = 0, '',
        intDiv(n, 2) % 23 = 1, repeat('long_key_', 457),
        intDiv(n, 2) % 23 = 2, unhex('75726c2d6b65792dda3f470aa0699ce3'),
        intDiv(n, 2) % 23 = 3, unhex('75726c2d6b65792d925757aa8e54803d'),
        intDiv(n, 2) % 23 = 4, 'x',
        intDiv(n, 2) % 23 = 5, 'x\0',
        concat('key_', toString(intDiv(n, 2) % 257))) AS k
FROM numbers(65536);

CREATE TEMPORARY TABLE expected_counts ENGINE = Memory AS
SELECT k, count() AS c FROM combining_input GROUP BY k SETTINGS enable_adaptive_aggregator = 0;

-- Distinct misses end the cache trial, and the rest of the block still stages one count per key.
CREATE TEMPORARY TABLE unique_input ENGINE = Memory AS
SELECT concat('unique_', toString(n)) AS k FROM combining_input;

SELECT 'Unique string counts', count() = 65536 AND min(c) = 1 AND max(c) = 1
FROM (SELECT k, count() AS c FROM unique_input GROUP BY k);

-- Sparse repetitions combine during the trial, then keep their exact counts across its cutoff.
CREATE TEMPORARY TABLE sparse_reuse_input ENGINE = Memory AS
SELECT if(n % 16384 >= 4096 AND n % 16 < 2, 'shared_key', concat('unique_', toString(n))) AS k
FROM combining_input;

SELECT 'Sparse reuse counts',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT k, count() AS c FROM sparse_reuse_input GROUP BY k))
        = (SELECT arraySort(groupArray((k, c))) FROM
            (SELECT k, count() AS c FROM sparse_reuse_input GROUP BY k SETTINGS enable_adaptive_aggregator = 0));

SELECT 'String counts',
    (SELECT arraySort(groupArray((k, c))) FROM (SELECT k, count() AS c FROM combining_input GROUP BY k))
        = (SELECT arraySort(groupArray((k, c))) FROM expected_counts);

SELECT 'Short string counts',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT substring(k, 1, 16) AS k, count() AS c FROM combining_input GROUP BY k))
        = (SELECT arraySort(groupArray((k, c))) FROM
            (SELECT substring(k, 1, 16) AS k, count() AS c FROM combining_input GROUP BY k
                SETTINGS enable_adaptive_aggregator = 0));

SELECT 'FixedString counts',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT toFixedString(substring(k, 1, 127), 128) AS k, count() AS c FROM combining_input GROUP BY k))
        = (SELECT arraySort(groupArray((k, c))) FROM
            (SELECT toFixedString(substring(k, 1, 127), 128) AS k, count() AS c FROM combining_input GROUP BY k
                SETTINGS enable_adaptive_aggregator = 0));

SELECT 'Legacy string counts',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT k, count() AS c FROM combining_input GROUP BY k
            SETTINGS enable_packed_string_keys_in_aggregation = 0))
        = (SELECT arraySort(groupArray((k, c))) FROM expected_counts);

SELECT 'LowCardinality string counts',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT toLowCardinality(k) AS k, count() AS c FROM combining_input GROUP BY k))
        = (SELECT arraySort(groupArray((k, c))) FROM expected_counts);

SELECT 'Nullable string counts',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT if(n % 31 = 0, NULL, k) AS k, count() AS c FROM combining_input GROUP BY k))
        = (SELECT arraySort(groupArray((k, c))) FROM
            (SELECT if(n % 31 = 0, NULL, k) AS k, count() AS c FROM combining_input GROUP BY k
                SETTINGS enable_adaptive_aggregator = 0));

SELECT 'Serialized counts',
    (SELECT arraySort(groupArray((r, k, c))) FROM
        (SELECT n % 3 AS r, k, count() AS c FROM combining_input GROUP BY r, k))
        = (SELECT arraySort(groupArray((r, k, c))) FROM
            (SELECT n % 3 AS r, k, count() AS c FROM combining_input GROUP BY r, k
                SETTINGS enable_adaptive_aggregator = 0));

-- A block's repeated suffix has one missed key, so its first frozen append receives a single count record.
SELECT 'Single count record',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT if(n % 16384 < 4096, k, 'constant_key') AS k, count() AS c FROM combining_input GROUP BY k))
        = (SELECT arraySort(groupArray((k, c))) FROM
            (SELECT if(n % 16384 < 4096, k, 'constant_key') AS k, count() AS c FROM combining_input GROUP BY k
                SETTINGS enable_adaptive_aggregator = 0));

-- Combined multiplicities contribute to the pruning bounds as well as to the final exact counts.
SELECT 'Top counts',
    (SELECT arraySort(groupArray(c)) FROM
        (SELECT k, count() AS c FROM combining_input GROUP BY k ORDER BY c DESC LIMIT 5))
        = (SELECT arraySort(groupArray(c)) FROM (SELECT c FROM expected_counts ORDER BY c DESC LIMIT 5)),
    (SELECT count() FROM
        (SELECT k, count() AS c FROM combining_input GROUP BY k ORDER BY c DESC LIMIT 5)
        WHERE (k, c) NOT IN (SELECT k, c FROM expected_counts)) = 0;

-- Spilled records carry owned key bytes and combined counts, so disk replay preserves exact groups.
SELECT 'Spilled counts',
    (SELECT arraySort(groupArray((k, c))) FROM
        (SELECT k, count() AS c FROM combining_input GROUP BY k
            SETTINGS max_bytes_before_external_group_by = 524288))
        = (SELECT arraySort(groupArray((k, c))) FROM expected_counts);
