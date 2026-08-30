-- Emit of fixed-width right-side columns must produce the same values whether it goes through the
-- direct gather or through the generic per-row path. Every arm pins the settings that select the
-- emit path, because the test runner randomizes them. The block at the end asserts which arms
-- actually took the gather: without it a green run cannot tell the two paths apart.
--
-- Every arm that must gather carries a `FixedString(40)` column. Whether the in-memory row store
-- claims the narrow columns instead depends on a planner estimate, which is not stable across runs,
-- but 40 bytes is above the row store's inclusive 32-byte limit, so that one column stays on the
-- columnar path in either case. Arms that must NOT gather carry no such column.

DROP TABLE IF EXISTS dg_build;
DROP TABLE IF EXISTS dg_probe;
DROP TABLE IF EXISTS dg_mixed;
DROP TABLE IF EXISTS dg_mixed_probe;
DROP TABLE IF EXISTS dg_list;
DROP TABLE IF EXISTS dg_list_mixed;
DROP TABLE IF EXISTS dg_list_probe;
DROP TABLE IF EXISTS dg_nullable;
DROP TABLE IF EXISTS dg_rowstore;
DROP TABLE IF EXISTS dg_rowstore_probe;

-- One payload column per admitted type. `Interval` is admitted by the gather but cannot be a table
-- column, so it has no arm here.
CREATE TABLE dg_build
(
    k UInt64,
    c_u8 UInt8, c_i8 Int8, c_u16 UInt16, c_i16 Int16, c_u32 UInt32, c_i32 Int32,
    c_u64 UInt64, c_i64 Int64, c_u128 UInt128, c_i128 Int128, c_u256 UInt256, c_i256 Int256,
    c_bf16 BFloat16, c_f32 Float32, c_f64 Float64,
    c_date Date, c_date32 Date32, c_dt DateTime, c_dt64 DateTime64(3), c_time Time, c_time64 Time64(3),
    c_ipv4 IPv4, c_ipv6 IPv6, c_uuid UUID,
    c_d32 Decimal32(2), c_d64 Decimal64(4), c_d128 Decimal128(6), c_d256 Decimal256(8),
    c_fs7 FixedString(7), c_fs40 FixedString(40)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO dg_build SELECT
    number,
    toUInt8(number % 251), toInt8(number % 127) - 63, toUInt16(number * 7 % 65521), toInt16(number % 32767) - 16000,
    toUInt32(number * 7919), toInt32(number % 100000) - 50000,
    number * 1000003, -toInt64(number) * 7,
    toUInt128(number) * toUInt128(18446744073709551557), toInt128(number) * toInt128(-9223372036854775807),
    toUInt256(number) * toUInt256(18446744073709551557), toInt256(number) * toInt256(-9223372036854775807),
    CAST(number % 97, 'BFloat16'), toFloat32(number) / 3, toFloat64(number) / 7,
    toDate('2020-01-01') + (number % 4000), toDate32('1950-01-01') + (number % 20000),
    toDateTime('2020-01-01 00:00:00', 'UTC') + (number * 37),
    toDateTime64('2020-01-01 00:00:00.000', 3, 'UTC') + number,
    CAST(number % 86399, 'Time'), CAST(number % 86399, 'Time64(3)'),
    CAST(toUInt32(number * 7919), 'IPv4'), CAST(concat('::ffff:', toString(number % 256), '.1.2.3'), 'IPv6'),
    reinterpretAsUUID(toFixedString(leftPad(toString(number), 16, '0'), 16)),
    CAST(number * 3 AS Decimal32(2)), CAST(number * 5 AS Decimal64(4)),
    CAST(number * 7 AS Decimal128(6)), CAST(number * 11 AS Decimal256(8)),
    toFixedString(leftPad(toString(number), 7, 'x'), 7),
    toFixedString(leftPad(toString(number), 40, 'w'), 40)
FROM numbers(2000);

-- 1000 probe rows against 2000 unique build keys keeps the estimated fanout below the row store's
-- `min_rows_ratio_for_hash_join_row_store` of 5, so these columns reach the columnar emit path.
CREATE TABLE dg_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_probe SELECT number * 2 FROM numbers(1000);

-- Arm 1: the same join under three algorithms. `full_sorting_merge` shares no code with this change,
-- so it is a differential oracle inside one binary.
SELECT 'arm1 hash', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    log_comment = 'dg_arm1_hash', ast_fuzzer_runs = 0;

SELECT 'arm1 parallel_hash', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_build USING (k)
SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    log_comment = 'dg_arm1_parallel_hash', ast_fuzzer_runs = 0;

SELECT 'arm1 full_sorting_merge', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_build USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    log_comment = 'dg_arm1_fsm', ast_fuzzer_runs = 0;

-- Arm 2: 100 probe keys have no match. With `join_use_nulls = 0` an unmatched right value is the type
-- default, which is what a zero ref word makes the gather write. `Enum8` defaults to its first value
-- instead of zero, so it is excluded from the gather and stays on the generic path in the same query.
CREATE TABLE dg_mixed (k UInt64, c_u64 UInt64, c_fs7 FixedString(7), c_fs40 FixedString(40), c_enum Enum8('a' = 1, 'b' = 2, 'c' = 3))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_mixed SELECT number + 1, number * 1000003, toFixedString(leftPad(toString(number), 7, 'x'), 7),
    toFixedString(leftPad(toString(number), 40, 'w'), 40),
    CAST(2 + number % 2, 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)') FROM numbers(500);
CREATE TABLE dg_mixed_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_mixed_probe SELECT number FROM numbers(600);

SELECT 'arm2 unmatched defaults', count(), sum(cityHash64(*)),
    countIf(c_u64 = 0), countIf(c_fs7 = toFixedString('', 7)), countIf(c_enum = 'a')
FROM dg_mixed_probe LEFT JOIN dg_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    log_comment = 'dg_arm2_defaults', ast_fuzzer_runs = 0;

SELECT 'arm2 unmatched nulls', count(), sum(cityHash64(*)),
    countIf(c_u64 IS NULL), countIf(c_fs7 IS NULL), countIf(c_enum IS NULL)
FROM dg_mixed_probe LEFT JOIN dg_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 1,
    log_comment = 'dg_arm2_nulls', ast_fuzzer_runs = 0;

-- Arms 3 and 5: ten build rows per key. The threshold pin selects the emit builder; the gathering
-- sub-arms and the untouched `buildOutputFromRowRefLists` control must agree on the values.
CREATE TABLE dg_list (k UInt64, a UInt64, b Int32, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_list SELECT number % 200, number * 1000003, toInt32(number) - 1000,
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(2000);
CREATE TABLE dg_list_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_list_probe SELECT number FROM numbers(100);

SELECT 'arm3 row list', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0,
    log_comment = 'dg_arm3_by_blocks', ast_fuzzer_runs = 0;

SELECT 'arm3 row ref lists control', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0,
    log_comment = 'dg_arm3_by_ref_lists', ast_fuzzer_runs = 0;

SELECT 'arm5 limit and offset', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7,
    log_comment = 'dg_arm5_limit_offset', ast_fuzzer_runs = 0;

-- Arms 5b and 5c pin which builder ran, and with it the rule that the limit and offset builder is all
-- or nothing: its gather input is the subset of ref words the walk selects, so one column left on the
-- generic path forces every column onto it. The two arms differ only in that switch. The `String`
-- column is what keeps the output mixed: neither the gather nor the row store can take it, and with
-- only one row-store-useful column left the row store cannot initialize at all.
CREATE TABLE dg_list_mixed (k UInt64, a UInt64, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_list_mixed SELECT number % 200, number * 1000003, toString(number) FROM numbers(2000);

SELECT 'arm5b mixed limit and offset', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7,
    log_comment = 'dg_arm5b_mixed_limit_offset', ast_fuzzer_runs = 0;

SELECT 'arm5c mixed by blocks', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0,
    log_comment = 'dg_arm5c_mixed_by_blocks', ast_fuzzer_runs = 0;

-- Arm 4: `SEMI` strictness leaves `output_by_row_list` false, which is the single-inline-ref builder.
SELECT 'arm4 semi', count(), sum(cityHash64(*)) FROM dg_probe SEMI LEFT JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    log_comment = 'dg_arm4_semi', ast_fuzzer_runs = 0;

-- Arm 6: a `Nullable` stored column is not fixed and contiguous, so it falls back.
CREATE TABLE dg_nullable (k UInt64, a Nullable(UInt64), b Nullable(FixedString(7))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_nullable SELECT number, if(number % 5 = 0, NULL, number * 1000003),
    if(number % 7 = 0, NULL, toFixedString(leftPad(toString(number), 7, 'x'), 7)) FROM numbers(2000);

SELECT 'arm6 nullable control', count(), sum(cityHash64(*)), countIf(a IS NULL), countIf(b IS NULL)
FROM dg_probe LEFT JOIN dg_nullable USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    log_comment = 'dg_arm6_nullable', ast_fuzzer_runs = 0;

-- Arm 7: an upstream `arrayJoin` wraps every right column wider than 8 bytes in `ColumnReplicated`,
-- which does not implement `getRawData`. This arm fails with NOT_IMPLEMENTED if that test is not the
-- first one the emit table applies.
SELECT 'arm7 replicated', count(), sum(cityHash64(*)) FROM
(
    SELECT number AS k FROM numbers(10, 10)
) AS l
RIGHT JOIN
(
    SELECT number AS k, reinterpretAsUUID(toFixedString(leftPad(toString(number), 16, '0'), 16)) AS u,
        toFixedString(leftPad(toString(number), 32, 'z'), 32) AS w,
        CAST(number * 7 AS Decimal128(6)) AS d, arrayJoin(range(number)) AS i
    FROM numbers(10)
) AS r USING (k)
SETTINGS enable_lazy_columns_replication = 1, join_algorithm = 'hash', query_plan_join_swap_table = 0,
    join_use_nulls = 0, log_comment = 'dg_arm7_replicated', ast_fuzzer_runs = 0;

-- Arm 8: the row store and the gather coexisting. A fanout of ten with three qualifying columns
-- (the saved key counts) initializes the row store, which claims the narrow columns; `FixedString(40)`
-- is the only fixed and contiguous width above the row store's inclusive 32-byte limit, so it is the
-- one column left on the columnar path and it is gathered.
CREATE TABLE dg_rowstore (k UInt64, n1 UInt64, n2 UInt64, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_rowstore SELECT number, number * 1000003, number * 7, toFixedString(leftPad(toString(number), 40, 'y'), 40)
FROM numbers(100);
CREATE TABLE dg_rowstore_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_rowstore_probe SELECT number % 100 FROM numbers(1000);

SELECT 'arm8 row store mixed', count(), sum(cityHash64(*)) FROM dg_rowstore_probe JOIN dg_rowstore USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    log_comment = 'dg_arm8_row_store', ast_fuzzer_runs = 0;

-- Arm 9: a reranged build side is emitted by `buildOutputFromRowRefLists`, which this change leaves
-- alone. The threshold is pinned to the value that makes arm 3 gather, so nothing being gathered here
-- is the reranging taking effect and not the threshold.
SELECT 'arm9 reranged control', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0,
    allow_experimental_join_right_table_sorting = 1, join_to_sort_minimum_perkey_rows = 2,
    join_to_sort_maximum_table_rows = 10000,
    log_comment = 'dg_arm9_reranged', ast_fuzzer_runs = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, 'dg_', '') AS arm,
    max(ProfileEvents['HashJoinDirectGatheredRows']) > 0 AS gathered
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'dg_arm%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE dg_build;
DROP TABLE dg_probe;
DROP TABLE dg_mixed;
DROP TABLE dg_mixed_probe;
DROP TABLE dg_list;
DROP TABLE dg_list_mixed;
DROP TABLE dg_list_probe;
DROP TABLE dg_nullable;
DROP TABLE dg_rowstore;
DROP TABLE dg_rowstore_probe;
