-- Tags: long

-- The randomized `prefer_external_sort_block_bytes = 1` writes spill blocks of 128 rows, which makes the
-- spilling queries an order of magnitude slower in sanitizer builds.
SET prefer_external_sort_block_bytes = DEFAULT;
SET max_bytes_ratio_before_external_limit_by = 0;
SET max_untracked_memory = 0;
SET max_threads = 1;
SET max_block_size = 65536;

-- The keys take about 330 thousand distinct values of `x`, more than the hash table of any key type below
-- holds within the 64 MiB spill threshold. The threshold leaves room for the table to grow over several
-- chunks before the switch, so the switch extracts tens of thousands of seen groups. The extraction differs
-- by hash method: how the table is traversed, where `NULL` and the empty string are kept, and how the keys
-- are restored into columns. Keys repeat early in the input, so the seen groups carry counts of one, two and
-- three rows, and the window `[1, 3)` continues from them after the switch. Without `ORDER BY` any rows of a
-- key that fit the window are valid, so each query checks what does not depend on the chosen rows: their
-- number, a checksum of their keys, and that no row repeats. These match the in-memory `LIMIT BY`.
CREATE VIEW limit_by_input AS SELECT number AS v, intHash32(number) % 524288 AS x FROM numbers(524288);

-- A `String` key keeps up to 11 bytes inside the hash-table cell and longer strings in the arena. The empty
-- string is the zero key, which the table keeps in a separate cell.
SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT multiIf(v % 100000 = 7, '', x % 2 = 0, toString(x), concat('arena-string-', toString(x))) AS k, v FROM limit_by_input LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 67108864, log_comment = '05341_external_limit_by_seen_groups/string';

-- A `Nullable(String)` key uses a table with a submap per class of key lengths, here with every submap: the
-- empty string, keys of 1 to 8, 9 to 16 and 17 to 24 bytes, and longer keys. The `NULL` group is kept outside
-- the table. The extraction takes several batches; each resumes the traversal where the previous one stopped.
SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT multiIf(v % 100000 = 3, NULL, v % 100000 = 7, '', x % 4 = 0, toString(x), x % 4 = 1, concat('key-9-16-', toString(x)), x % 4 = 2, concat('key-17-24-bytes-', toString(x)), concat('key-longer-than-24-bytes-', toString(x))) AS k, v FROM limit_by_input LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 67108864, log_comment = '05341_external_limit_by_seen_groups/nullable_string';

-- `LowCardinality` keys are hashed by their dictionary values and restored into a `LowCardinality` column.
-- The `NULL` group of `LowCardinality(Nullable(String))` is kept outside the table and differs from the empty
-- string.
SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT toLowCardinality(multiIf(v % 100000 = 3, NULL, v % 100000 = 7, '', toString(x))) AS k, v FROM limit_by_input LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 67108864, log_comment = '05341_external_limit_by_seen_groups/low_cardinality_nullable';

-- Fixed-size keys are packed into one 128-bit value, the `UInt64` before the `UInt8`, so the extraction
-- restores the columns in the order of the packing.
SELECT count(), sum(cityHash64(a, b)), uniqExact(v) = count() FROM (SELECT toUInt8(x % 256) AS a, toUInt64(intDiv(x, 256)) * 1000000007 AS b, v FROM limit_by_input LIMIT 2 OFFSET 1 BY a, b) SETTINGS max_bytes_before_external_limit_by = 67108864, log_comment = '05341_external_limit_by_seen_groups/packed_keys';

-- Keys that the table stores serialized are deserialized into their columns when they are extracted, and
-- the spill compares them by these columns.
SELECT count(), sum(cityHash64(k)), uniqExact(v) = count() FROM (SELECT tuple(toString(x % 1024), [intDiv(x, 1024), x % 3]) AS k, v FROM limit_by_input LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 67108864, log_comment = '05341_external_limit_by_seen_groups/serialized';

-- A key of a type without an order, such as an aggregate state, cannot be sorted. It is compared through its
-- 128-bit fingerprint once spilled, so its seen groups carry fingerprints instead of keys.
SELECT count(), sum(finalizeAggregation(k)), uniqExact(v) = count() FROM (SELECT initializeAggregation('sumState', toUInt64(x)) AS k, v FROM limit_by_input LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 67108864, log_comment = '05341_external_limit_by_seen_groups/aggregate_state';

-- With `ORDER BY` the spilling `LIMIT BY` keeps the same rows as the in-memory one, in the same order: the
-- rows kept before the switch come first, and the merged rows are sorted back into their arrival order. The
-- sort keeps its input in memory, which counts toward the spill threshold, so the threshold is higher; the
-- external sort is disabled because spilling the sort would release that memory. The checksum depends on the
-- position of each row, and the first and the last rows are shown.
SELECT count(), sum(cityHash64(rowNumberInAllBlocks(), k, v)), groupArray(3)((k, v)), arraySlice(groupArray((k, v)), -3) FROM (SELECT multiIf(v % 100000 = 7, '', x % 2 = 0, toString(x), concat('arena-string-', toString(x))) AS k, v FROM limit_by_input ORDER BY v DESC LIMIT 2 OFFSET 1 BY k) SETTINGS max_bytes_before_external_limit_by = 83886080, max_bytes_before_external_sort = 0, max_bytes_ratio_before_external_sort = 0, log_comment = '05341_external_limit_by_seen_groups/order_by';

DROP VIEW limit_by_input;

-- Every spilling query spilled and merged its runs.
SYSTEM FLUSH LOGS query_log;
SELECT
    substring(log_comment, length('05341_external_limit_by_seen_groups/') + 1) AS test_case,
    ProfileEvents['ExternalLimitByWritePart'] > 0 AS spilled,
    ProfileEvents['ExternalLimitByMerge'] > 0 AS merged
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND startsWith(log_comment, '05341_external_limit_by_seen_groups/')
ORDER BY test_case;
