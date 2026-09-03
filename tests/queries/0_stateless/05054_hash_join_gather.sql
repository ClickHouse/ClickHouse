-- Tags: long
-- The flaky check runs one copy of the changed test per core. This test is heavy enough that the
-- self-contention alone puts it over the 180 second cap.

-- Every right-side encoding has exactly one emit kernel, and the four emit builders - by blocks, by
-- ref lists, by limit and offset, and the not-joined scan - all have to produce the same values
-- through it. Every arm pins the settings that select the builder, because the test runner randomizes
-- them, and checks its values against a source that shares no code with the kernel under test: a
-- `full_sorting_merge` twin of the same query where that is possible, and a hand-computed count
-- otherwise.
--
-- Whether the in-memory row store claims a narrow column instead of the gather depends on a planner
-- estimate that is not stable across runs, so every arm takes that estimate out of its own outcome
-- in one of two ways: it reads a fixture carrying a `FixedString(40)` payload, which is above the
-- row store's inclusive 32-byte limit and so stays on the columnar path either way, or it pins
-- `enable_hash_join_row_store` explicitly. Neither row store setting is randomized by the test
-- runner. Carrying a `FixedString(40)` is not what makes an arm gather: several arms that must NOT
-- gather read fixtures that have one, and it is the emit path their settings and shape select that
-- makes them negative.

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
DROP TABLE IF EXISTS dg_interval;
DROP TABLE IF EXISTS dg_asof;
DROP TABLE IF EXISTS dg_asof_probe;
DROP TABLE IF EXISTS dg_ext;
DROP TABLE IF EXISTS dg_ext_probe;
DROP TABLE IF EXISTS dg_variant;
DROP TABLE IF EXISTS dg_variant_ord1;
DROP TABLE IF EXISTS dg_variant_ord2;
DROP TABLE IF EXISTS dg_variant_array;
DROP TABLE IF EXISTS dg_tuple_enum;
DROP TABLE IF EXISTS dg_enum;
DROP TABLE IF EXISTS dg_enum_list;
DROP TABLE IF EXISTS dg_enum_probe;
DROP TABLE IF EXISTS dg_lc;
DROP TABLE IF EXISTS dg_lc_sj;
DROP TABLE IF EXISTS dg_json;
DROP TABLE IF EXISTS dg_dyn;
DROP TABLE IF EXISTS dg_map;
DROP TABLE IF EXISTS dg_agg;
DROP TABLE IF EXISTS dg_qbit;
DROP TABLE IF EXISTS dg_empty;
DROP TABLE IF EXISTS dg_sparse;
DROP TABLE IF EXISTS dg_list_ext;
DROP TABLE IF EXISTS dg_nullkey;
DROP TABLE IF EXISTS dg_or;
DROP TABLE IF EXISTS dg_or_probe;
DROP TABLE IF EXISTS dg_sj;
DROP TABLE IF EXISTS dg_jg;
DROP TABLE IF EXISTS dg_any_right;
DROP TABLE IF EXISTS dg_semi_right;
DROP TABLE IF EXISTS dg_any_inner;

-- One payload column per gathered type except `Interval`, which arm 1e carries on its own fixture so
-- that its column does not move the reference values of every arm that reads this table.
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

-- 1000 probe rows against 2000 unique build keys usually keeps the estimated fanout below the row
-- store's `min_rows_ratio_for_hash_join_row_store` of 5, but that estimate is not a guarantee, so
-- `c_fs40` is what holds these columns on the columnar emit path.
CREATE TABLE dg_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_probe SELECT number * 2 FROM numbers(1000);

-- Arm 1: the same join under three algorithms. `full_sorting_merge` shares no code with this change,
-- so it is a differential oracle inside one binary.
SELECT 'arm1 hash', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm1 parallel_hash', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_build USING (k)
SETTINGS join_algorithm = 'parallel_hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm1 full_sorting_merge', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_build USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 1d: every payload type of `dg_build` in one query, so a kernel that writes the wrong bytes for
-- any one of them changes the hash. Pinning the row store off leaves all 30 payload columns on the
-- columnar path for all 1000 probe rows, with no planner estimate in the path.
SELECT 'arm1d per-type arming', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 1e: `Interval` is the one gathered type `dg_build` does not carry. It is storable in a table
-- (`cannotBeStoredInTables` is not overridden for it), so it gets a fixture of its own, and `countIf`
-- checks its values rather than only the hash.
CREATE TABLE dg_interval (k UInt64, iv IntervalDay) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_interval SELECT number, toIntervalDay(number) FROM numbers(2000);

SELECT 'arm1e interval arming', count(), sum(cityHash64(*)), countIf(iv = toIntervalDay(k))
FROM dg_probe JOIN dg_interval USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 2: 100 probe keys have no match. With `join_use_nulls = 0` an unmatched right value is the type
-- default, which is what a zero ref word makes the gather write - not zero bytes, because an `Enum8`
-- defaults to its first declared value. `countIf(c_enum = 'a')` is that default counted directly.
-- The row store is pinned off in both halves so that `c_enum` stays on the emit kernels, which is
-- where its default is decided; arm 28 covers the enum shapes systematically.
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
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm2 unmatched nulls', count(), sum(cityHash64(*)),
    countIf(c_u64 IS NULL), countIf(c_fs7 IS NULL), countIf(c_enum IS NULL)
FROM dg_mixed_probe LEFT JOIN dg_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 1,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arms 3 and 5: ten build rows per key. The threshold pin selects the emit builder, and all three -
-- by blocks, by ref lists, by limit and offset - have to agree on the values. `buildOutputFromBlocks`
-- expands a key's rows into one word each while `buildOutputFromRowRefLists` hands the kernel the
-- list words as they are, so the counts are equal but the work is not.
CREATE TABLE dg_list (k UInt64, a UInt64, b Int32, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_list SELECT number % 200, number * 1000003, toInt32(number) - 1000,
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(2000);
CREATE TABLE dg_list_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_list_probe SELECT number FROM numbers(100);

SELECT 'arm3 row list', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm3 row ref lists control', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

-- Every arm below that asks for the limit and offset builder also pins `enable_analyzer = 1`: only
-- the `TableJoin` the analyzer builds carries `joined_block_split_single_row`, so with the old
-- analyzer the arm silently gets the by-blocks builder that arms 3 and 14 already cover.
SELECT 'arm5 limit and offset', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7, enable_analyzer = 1;

-- Arms 5b and 5c pin which builder ran. The limit and offset builder records the ref words its walk
-- selects, so it reaches the kernels like every other builder: 2 columns x 1000 rows, `a` through
-- the fixed-width plane copy and the `LowCardinality(String)` one through its own
-- `insertRangeFrom`. The two arms differ only in the builder switch. Only `a` is narrow enough for
-- the row store to claim, so both arms also pin the row store off to keep the emit path theirs to
-- choose.
CREATE TABLE dg_list_mixed (k UInt64, a UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_list_mixed SELECT number % 200, number * 1000003, toString(number % 50) FROM numbers(2000);

SELECT 'arm5b mixed limit and offset', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7, enable_hash_join_row_store = 0, enable_analyzer = 1;

SELECT 'arm5c mixed by blocks', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0,
    enable_hash_join_row_store = 0;

-- Arm 4: `SEMI` strictness leaves `output_by_row_list` false, which is the single-inline-ref builder.
SELECT 'arm4 semi', count(), sum(cityHash64(*)) FROM dg_probe SEMI LEFT JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 6: a `Nullable` stored column gathers its null map and its nested plane; an unmatched row
-- writes a set null byte over a zeroed nested value, which is what `insertDefaultInto` inserts.
-- The row store unwraps `Nullable` and would claim both payloads, so it is pinned off. The
-- `full_sorting_merge` twin pins the same values through an algorithm sharing no code.
CREATE TABLE dg_nullable (k UInt64, a Nullable(UInt64), b Nullable(FixedString(7))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_nullable SELECT number, if(number % 5 = 0, NULL, number * 1000003),
    if(number % 7 = 0, NULL, toFixedString(leftPad(toString(number), 7, 'x'), 7)) FROM numbers(2000);

SELECT 'arm6 nullable', count(), sum(cityHash64(*)), countIf(a IS NULL), countIf(b IS NULL)
FROM dg_probe LEFT JOIN dg_nullable USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm6 nullable full_sorting_merge control', count(), sum(cityHash64(*)), countIf(a IS NULL), countIf(b IS NULL)
FROM dg_probe LEFT JOIN dg_nullable USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 7: lazy replication under an upstream `arrayJoin` wraps the right columns wider than 8 bytes
-- (`u`, `w`, `d`) in `ColumnReplicated`, which the gather reads through the per-block indexes:
-- `row' = indexes[row]` addresses the nested column. The two key ranges overlap, so all 45 rows
-- reach the probe emit path. Every column here fits the row store's inclusive 32-byte limit, so it
-- is pinned off to keep them all on the columnar path.
SELECT 'arm7 replicated', count(), sum(cityHash64(*)) FROM
(
    SELECT number AS k FROM numbers(1, 9)
) AS l
RIGHT JOIN
(
    SELECT number AS k, reinterpretAsUUID(toFixedString(leftPad(toString(number), 16, '0'), 16)) AS u,
        toFixedString(leftPad(toString(number), 32, 'z'), 32) AS w,
        CAST(number * 7 AS Decimal128(6)) AS d, arrayJoin(range(number)) AS i
    FROM numbers(10)
) AS r USING (k)
SETTINGS enable_lazy_columns_replication = 1, join_algorithm = 'hash', query_plan_join_swap_table = 0,
    join_use_nulls = 0, join_output_by_rowlist_perkey_rows_threshold = 1000000,
    joined_block_split_single_row = 0, enable_hash_join_row_store = 0;

-- Arm 8: the row store and the gather coexisting. A ratio of zero admits the row store without
-- consulting any row-count estimate; `USING` saves no right key, so `n1` and `n2` are the two
-- row-store-useful columns `initRowStore` needs, leaving `w` at 40 bytes as the one gathered column.
CREATE TABLE dg_rowstore (k UInt64, n1 UInt64, n2 UInt64, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_rowstore SELECT number, number * 1000003, number * 7, toFixedString(leftPad(toString(number), 40, 'y'), 40)
FROM numbers(100);
CREATE TABLE dg_rowstore_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_rowstore_probe SELECT number % 100 FROM numbers(1000);

SELECT 'arm8 row store mixed', count(), sum(cityHash64(*)) FROM dg_rowstore_probe JOIN dg_rowstore USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 1, min_rows_ratio_for_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 9: a reranged build side is the one producer of the range shape, and the kernels consume it as
-- ranges rather than flattening it - one copy per run of consecutive rows. The threshold is pinned to
-- the value that makes arm 3 gather, so the values agreeing with arm 3's is the reranging taking
-- effect rather than the threshold.
SELECT 'arm9 reranged control', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0,
    allow_experimental_join_right_table_sorting = 1, join_to_sort_minimum_perkey_rows = 2,
    join_to_sort_maximum_table_rows = 10000;

-- Arm 10: the extended-type gathers - `String`, `Nullable`, `Array` (nested and doubly nested),
-- `Tuple` - all in one table. The row store pin leaves all 6 payload columns on the columnar path
-- for all 1000 matched probe rows. The `full_sorting_merge` twin pins the same values through an
-- algorithm sharing no code.
CREATE TABLE dg_ext
(
    k UInt64,
    s String,
    ns Nullable(String),
    nu Nullable(UInt64),
    a Array(UInt64),
    aa Array(Array(String)),
    t Tuple(u UInt64, s String, na Nullable(FixedString(7)))
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_ext SELECT
    number,
    repeat(concat('s', toString(number)), 1 + number % 3),
    if(number % 5 = 0, NULL, toString(number * 7)),
    if(number % 7 = 0, NULL, number * 1000003),
    range(number % 4),
    arrayMap(x -> arrayMap(y -> concat('v', toString(x + y)), range(1 + (x % 3))), range(number % 3)),
    CAST((number, toString(number), if(number % 3 = 0, NULL, toFixedString(leftPad(toString(number), 7, 'x'), 7))), 'Tuple(u UInt64, s String, na Nullable(FixedString(7)))')
FROM numbers(2000);

SELECT 'arm10 extended types', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm10 extended full_sorting_merge control', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_ext USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 10c: 100 unmatched probe keys. Every extended type's unmatched value must equal what
-- `insertDefaultInto` writes: an empty string, NULL, an empty array, a tuple of those.
CREATE TABLE dg_ext_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_ext_probe SELECT number FROM numbers(2100);

SELECT 'arm10c extended defaults', count(), sum(cityHash64(*)),
    countIf(s = ''), countIf(ns IS NULL), countIf(nu IS NULL), countIf(a = []), countIf(tupleElement(t, 's') = '')
FROM dg_ext_probe LEFT JOIN dg_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm10c defaults full_sorting_merge control', count(), sum(cityHash64(*)),
    countIf(s = ''), countIf(ns IS NULL), countIf(nu IS NULL), countIf(a = []), countIf(tupleElement(t, 's') = '')
FROM dg_ext_probe LEFT JOIN dg_ext USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 11: `Variant` gathers its local discriminators, offsets, and every nested variant
-- column, remapping each stored block's local discriminator order onto the destination's. An
-- unmatched row is NULL, like `insertDefault`.
CREATE TABLE dg_variant (k UInt64, v Variant(Array(UInt64), String, UInt64), w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_variant SELECT
    number,
    multiIf(
        number % 4 = 0, CAST(NULL, 'Variant(Array(UInt64), String, UInt64)'),
        number % 4 = 1, CAST(number * 1000003, 'Variant(Array(UInt64), String, UInt64)'),
        number % 4 = 2, CAST(concat('s', toString(number)), 'Variant(Array(UInt64), String, UInt64)'),
        CAST(CAST(range(number % 5), 'Array(UInt64)'), 'Variant(Array(UInt64), String, UInt64)')),
    toFixedString(leftPad(toString(number), 40, 'v'), 40)
FROM numbers(2000);

SELECT 'arm11 variant', count(), sum(cityHash64(k, toString(v), w)), countIf(v IS NULL),
    countIf(variantType(v) = 'UInt64'), countIf(variantType(v) = 'String'), countIf(variantType(v) = 'Array(UInt64)')
FROM dg_ext_probe LEFT JOIN dg_variant USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm11 variant full_sorting_merge control', count(), sum(cityHash64(k, toString(v), w)), countIf(v IS NULL),
    countIf(variantType(v) = 'UInt64'), countIf(variantType(v) = 'String'), countIf(variantType(v) = 'Array(UInt64)')
FROM dg_ext_probe LEFT JOIN dg_variant USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 11c: stored blocks whose local discriminator orders actually differ. Arm 11 cannot reach one,
-- because `SerializationVariant` only ever writes the global order, so every block a `MergeTree` read
-- produces has an identity map and arm 11 stays green even if the remap is bypassed. Widening a
-- `Variant` by a cast keeps the locals in source order while renumbering the globals, so the two sides
-- of the union below store their blocks under orders 1, 2, 0 and 0, 2, 1.
CREATE TABLE dg_variant_ord1 (k UInt64, v Variant(String, UInt64), w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_variant_ord1 SELECT
    number,
    multiIf(
        number % 3 = 0, CAST(NULL, 'Variant(String, UInt64)'),
        number % 3 = 1, CAST(concat('s', toString(number)), 'Variant(String, UInt64)'),
        CAST(number * 1000003, 'Variant(String, UInt64)')),
    toFixedString(leftPad(toString(number), 40, 'v'), 40)
FROM numbers(1050);

CREATE TABLE dg_variant_ord2 (k UInt64, v Variant(Array(UInt64), UInt64), w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_variant_ord2 SELECT
    number,
    multiIf(
        number % 3 = 0, CAST(NULL, 'Variant(Array(UInt64), UInt64)'),
        number % 3 = 1, CAST(CAST(range(number % 5), 'Array(UInt64)'), 'Variant(Array(UInt64), UInt64)'),
        CAST(number * 1000003, 'Variant(Array(UInt64), UInt64)')),
    toFixedString(leftPad(toString(number), 40, 'v'), 40)
FROM numbers(1050, 1050);

SELECT 'arm11c variant local order', count(), sum(cityHash64(k, toString(v), w)), countIf(v IS NULL),
    countIf(variantType(v) = 'UInt64'), countIf(variantType(v) = 'String'), countIf(variantType(v) = 'Array(UInt64)')
FROM dg_ext_probe LEFT JOIN (
    SELECT k, CAST(v, 'Variant(Array(UInt64), String, UInt64)') AS v, w FROM dg_variant_ord1
    UNION ALL
    SELECT k, CAST(v, 'Variant(Array(UInt64), String, UInt64)') AS v, w FROM dg_variant_ord2
) AS ord USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0;

SELECT 'arm11c local order full_sorting_merge control', count(), sum(cityHash64(k, toString(v), w)), countIf(v IS NULL),
    countIf(variantType(v) = 'UInt64'), countIf(variantType(v) = 'String'), countIf(variantType(v) = 'Array(UInt64)')
FROM dg_ext_probe LEFT JOIN (
    SELECT k, CAST(v, 'Variant(Array(UInt64), String, UInt64)') AS v, w FROM dg_variant_ord1
    UNION ALL
    SELECT k, CAST(v, 'Variant(Array(UInt64), String, UInt64)') AS v, w FROM dg_variant_ord2
) AS ord USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- Arm 11e: a `Variant` below an `Array`. The array kernel hands its nested plane ranges rather than
-- row words, so this is the only shape that reaches `gatherVariantRanges` - and the only one where a
-- variant's coordinate is an element index instead of a block row.
CREATE TABLE dg_variant_array (k UInt64, va Array(Variant(String, UInt64)), w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_variant_array SELECT
    number,
    arrayMap(x -> multiIf(
        x % 3 = 0, CAST(NULL, 'Variant(String, UInt64)'),
        x % 3 = 1, CAST(concat('s', toString(x)), 'Variant(String, UInt64)'),
        CAST(x * 1000003, 'Variant(String, UInt64)')), range(number % 5)),
    toFixedString(leftPad(toString(number), 40, 'v'), 40)
FROM numbers(2000);

SELECT 'arm11e variant under array', count(), sum(cityHash64(k, toString(va), w)), sum(length(va)), countIf(empty(va))
FROM dg_ext_probe LEFT JOIN dg_variant_array USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0;

SELECT 'arm11e under array full_sorting_merge control', count(), sum(cityHash64(k, toString(va), w)), sum(length(va)), countIf(empty(va))
FROM dg_ext_probe LEFT JOIN dg_variant_array USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- Arm 12: the extended types under lazy replication (arm 7's fixture with `String`, `Array`,
-- `Nullable` and `Tuple` payloads): every replicated column gathers through the per-block indexes.
SELECT 'arm12 replicated extended', count(), sum(cityHash64(*)) FROM
(
    SELECT number AS k FROM numbers(1, 9)
) AS l
RIGHT JOIN
(
    SELECT number AS k, concat('str', toString(number)) AS s, range(number % 4) AS a,
        if(number % 3 = 0, NULL, number * 7) AS n,
        CAST((number, toString(number)), 'Tuple(UInt64, String)') AS t, arrayJoin(range(number)) AS i
    FROM numbers(10)
) AS r USING (k)
SETTINGS enable_lazy_columns_replication = 1, join_algorithm = 'hash', query_plan_join_swap_table = 0,
    join_use_nulls = 0, join_output_by_rowlist_perkey_rows_threshold = 1000000,
    joined_block_split_single_row = 0, enable_hash_join_row_store = 0;

SELECT 'arm12 replicated ref lists control', count(), sum(cityHash64(*)) FROM
(
    SELECT number AS k FROM numbers(1, 9)
) AS l
RIGHT JOIN
(
    SELECT number AS k, concat('str', toString(number)) AS s, range(number % 4) AS a,
        if(number % 3 = 0, NULL, number * 7) AS n,
        CAST((number, toString(number)), 'Tuple(UInt64, String)') AS t, arrayJoin(range(number)) AS i
    FROM numbers(10)
) AS r USING (k)
SETTINGS enable_lazy_columns_replication = 1, join_algorithm = 'hash', query_plan_join_swap_table = 0,
    join_use_nulls = 0, join_output_by_rowlist_perkey_rows_threshold = 0,
    joined_block_split_single_row = 0;

-- Arm 13: an `Enum` leaf inside a `Tuple` gathers like any other fixed-width leaf, and its default
-- comes from `DataTypeTuple::insertDefaultInto`, which recurses into the element types. Both columns
-- gather here.
CREATE TABLE dg_tuple_enum (k UInt64, te Tuple(u UInt64, e Enum8('a' = 1, 'b' = 2)), w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_tuple_enum SELECT number,
    CAST((number, CAST(1 + number % 2, 'Enum8(\'a\' = 1, \'b\' = 2)')), 'Tuple(u UInt64, e Enum8(\'a\' = 1, \'b\' = 2))'),
    toFixedString(leftPad(toString(number), 40, 'e'), 40)
FROM numbers(2000);

SELECT 'arm13 enum in tuple', count(), sum(cityHash64(*)) FROM dg_probe JOIN dg_tuple_enum USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 14: the extended types through the two remaining gathering builders: the row-list one (ten
-- build rows per key, `refsOf` expansion plus adjacent-run merging under `Array`) and the
-- limit-and-offset one (the all-or-nothing walk).
CREATE TABLE dg_list_ext (k UInt64, s String, a Array(UInt64), nu Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_list_ext SELECT number % 200, concat('s', toString(number)), range(number % 4),
    if(number % 6 = 0, NULL, number * 31) FROM numbers(2000);

SELECT 'arm14 extended row list', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0,
    enable_hash_join_row_store = 0;

SELECT 'arm14 extended ref lists control', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

SELECT 'arm14c extended limit and offset', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7, enable_hash_join_row_store = 0, enable_analyzer = 1;

-- Arm asof: an `ASOF` match is a single inline ref word like any other, so it resolves the same emit
-- table and runs the same kernels. Its three emitted right columns are the two payloads and the
-- right `ASOF` key itself, over all 80 probe rows, with the row store pinned off. Ten build rows per
-- key with distinct `ts` keep the chosen row unique, so the values are deterministic.
CREATE TABLE dg_asof (k UInt64, ts UInt64, a UInt64, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_asof SELECT number % 50, intDiv(number, 50) * 10, number * 1000003,
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(500);
-- Probe keys 50..79 have no build row at all, which is the arm's `collect_null` half.
CREATE TABLE dg_asof_probe (k UInt64, ts UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_asof_probe SELECT number, 95 FROM numbers(80);

SELECT 'arm_asof', count(), sum(cityHash64(*)), countIf(a = 0), countIf(w = toFixedString('', 40))
FROM dg_asof_probe ASOF LEFT JOIN dg_asof ON dg_asof_probe.k = dg_asof.k AND dg_asof_probe.ts >= dg_asof.ts
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm_asof full_sorting_merge control', count(), sum(cityHash64(*)), countIf(a = 0), countIf(w = toFixedString('', 40))
FROM dg_asof_probe ASOF LEFT JOIN dg_asof ON dg_asof_probe.k = dg_asof.k AND dg_asof_probe.ts >= dg_asof.ts
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 15: the not-joined scan over the hash map. `RIGHT JOIN` emits every build row that no probe row
-- matched, and those rows are recorded as ref words and emitted by the same kernels as a match. Half
-- of `dg_build`'s 2000 keys are odd and `dg_probe` only holds the even ones, so the two halves are
-- the same size: 1000 matched probe rows through the probe emit and 1000 unmatched build rows through
-- the not-joined scan. The not-joined scan emits the whole saved block, which for `RIGHT JOIN`
-- includes the key, so its per-row column count is one higher than the probe's.
SELECT 'arm15 right non-joined', count(), sum(cityHash64(*)) FROM dg_probe RIGHT JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm15 right non-joined full_sorting_merge control', count(), sum(cityHash64(*))
FROM dg_probe RIGHT JOIN dg_build USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 16: the not-joined scan over the null maps, which is the source a hash-map-only collector would
-- drop. A build row whose join key is NULL is in no hash map cell at all, so `FULL JOIN` can only
-- reach it through the stored null maps. Every fourth build key is NULL, so 100 of the 400 build rows
-- exist only there.
CREATE TABLE dg_nullkey (k Nullable(UInt64), a UInt64, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_nullkey SELECT if(number % 4 = 0, NULL, number), number * 1000003,
    toFixedString(leftPad(toString(number), 40, 'n'), 40) FROM numbers(400);

SELECT 'arm16 full join null keys', count(), sum(cityHash64(a, w)), countIf(dg_nullkey.k IS NULL), countIf(a = 0)
FROM dg_mixed_probe FULL JOIN dg_nullkey ON dg_mixed_probe.k = dg_nullkey.k
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm16 full join null keys full_sorting_merge control', count(), sum(cityHash64(a, w)),
    countIf(dg_nullkey.k IS NULL), countIf(a = 0)
FROM dg_mixed_probe FULL JOIN dg_nullkey ON dg_mixed_probe.k = dg_nullkey.k
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 17: the third not-joined source. Two disjuncts put the used flags per right row instead of per
-- map cell, so the scan walks the stored blocks and their flags rather than the map. Probe rows hit
-- every third build key through `k1` and the same rows again through `k2`, leaving the other two
-- thirds of the build side for the block scan.
CREATE TABLE dg_or (k1 UInt64, k2 UInt64, a UInt64, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_or SELECT number, number + 10000, number * 1000003,
    toFixedString(leftPad(toString(number), 40, 'o'), 40) FROM numbers(400);
CREATE TABLE dg_or_probe (k1 UInt64, k2 UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_or_probe SELECT number * 3, number * 3 + 10000 FROM numbers(200);

SELECT 'arm17 disjunct non-joined', count(), sum(cityHash64(a, w)), countIf(a = 0)
FROM dg_or_probe RIGHT JOIN dg_or ON dg_or_probe.k1 = dg_or.k1 OR dg_or_probe.k2 = dg_or.k2
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- No other algorithm joins on a disjunction, so the oracle is built by hand: both disjuncts select
-- the same build rows here (`k2` is `k1 + 10000` on both sides), so every build row is emitted once
-- and the right-side multiset is the whole build table. `join_use_nulls = 1` then makes the left key
-- NULL exactly on the rows the not-joined scan produced, which pins the split rather than the total:
-- the probe covers build keys 0, 3, ... 399, so 134 rows match and 266 come from the scan.
SELECT 'arm17 disjunct oracle', count(), sum(cityHash64(a, w)), countIf(a = 0) FROM dg_or;

SELECT 'arm17 disjunct split', count(), countIf(dg_or_probe.k1 IS NULL)
FROM dg_or_probe RIGHT JOIN dg_or ON dg_or_probe.k1 = dg_or.k1 OR dg_or_probe.k2 = dg_or.k2
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 1,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 18: the limit and offset builder cut by bytes instead of rows. A one-byte budget ends every
-- chunk at the first row that crosses it, which is the mid-`RowRefList` cursor case: the walk stops
-- inside a key's ten rows and the next call resumes at the same key at the same expanded position.
SELECT 'arm18 byte limit mid list', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 4096, max_joined_block_size_bytes = 1, enable_hash_join_row_store = 0;

-- Arm 19: the same builder at the two ends of its row budget. One row per chunk is the degenerate
-- cursor (every call resumes mid-key), 4096 rows is one chunk for the whole probe block.
SELECT 'arm19 row limit 1', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 1, enable_hash_join_row_store = 0;

SELECT 'arm19 row limit 4096', count(), sum(cityHash64(*)) FROM dg_list_probe JOIN dg_list_ext USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 4096, enable_hash_join_row_store = 0;

-- Arm 21: `ANY` strictness records one ref word per emitted left row like every other strictness, so
-- it runs the same kernels: 30 payload columns over 1000 matched probe rows, the same 30000 as the
-- `ALL` join of arm 1d over the same fixture. The second half joins an unbounded source, which is
-- what makes the planner derive the output header by running the join over an empty block - the
-- emit builders have to reach that and do nothing. Both halves pin `query_plan_join_swap_table`,
-- because swapping an `ANY LEFT` join makes it a `RIGHT ANY` one, which claims its rows differently.
SELECT 'arm21 any left join', count(), sum(cityHash64(*)) FROM dg_probe ANY LEFT JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm21 any left join over unbounded source', count(), sum(cityHash64(*)) FROM
(
    SELECT number, joined FROM system.numbers ANY LEFT JOIN
        (SELECT number * 2 AS number, number * 10 + 1 AS joined FROM system.numbers LIMIT 10) js2
        USING number LIMIT 10
)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 22: `RightAny` is a strictness of its own - the old `ANY`, selected by
-- `any_join_distinct_right_table_keys` - and it picks a key's first row rather than claiming rows
-- per left row. Its word choice must stay the head ref, which is what makes its values equal
-- arm 21's on a build side of unique keys.
SELECT 'arm22 right any', count(), sum(cityHash64(*)) FROM dg_probe ANY LEFT JOIN dg_build USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    any_join_distinct_right_table_keys = 1,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0, join_output_by_rowlist_perkey_rows_threshold = 1000000,
    joined_block_split_single_row = 0;

-- Arm 23: `ANTI` emits no right row at all, so it must record no words and gather nothing however
-- the collector is shaped.
SELECT 'arm23 anti', count() FROM dg_mixed_probe ANTI LEFT JOIN dg_mixed USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0, join_output_by_rowlist_perkey_rows_threshold = 1000000,
    joined_block_split_single_row = 0;

-- Arm 24: a required right key forces `need_filter`, because the key column is emitted from the
-- right side rather than derived from the left one. `ON` (not `USING`) is what puts it there.
SELECT 'arm24 required right key', count(), sum(cityHash64(*)), countIf(dg_build.k = 0)
FROM dg_probe ANY INNER JOIN dg_build ON dg_probe.k = dg_build.k
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0, join_output_by_rowlist_perkey_rows_threshold = 1000000,
    joined_block_split_single_row = 0;

-- Arm 26: `ANY RIGHT JOIN` claims a key once and then emits every row of it, so the probe records
-- the key's whole cell word - a `RowRefList` word as soon as a key has duplicates, not an inline
-- ref. The build side here has ten rows per key for exactly that reason: with unique keys every
-- word is inline and the arm cannot tell the two shapes apart. The `full_sorting_merge` twin pins
-- the values; the count pins that the whole key was emitted and not one row of it.
SELECT 'arm26 any right', count(), sum(cityHash64(*)) FROM dg_list_probe ANY RIGHT JOIN dg_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    any_join_distinct_right_table_keys = 0,
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_hash_join_row_store = 0, join_output_by_rowlist_perkey_rows_threshold = 1000000,
    joined_block_split_single_row = 0;

SELECT 'arm26 any right full_sorting_merge control', count(), sum(cityHash64(*))
FROM dg_list_probe ANY RIGHT JOIN dg_list USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0,
    any_join_distinct_right_table_keys = 0;

-- Arm 27: the same through a `Join` engine, which is how a query reaches `ANY RIGHT` without saying
-- so in the join clause. `SEMI RIGHT` and `ANY INNER` go alongside it: `SEMI RIGHT` records whole
-- key words like `ANY RIGHT`, `ANY INNER` claims and emits a single row, so the three of them cover
-- both word shapes a `StorageJoin` probe can produce.
CREATE TABLE dg_any_right (k UInt64, s String, w FixedString(40)) ENGINE = Join(ANY, RIGHT, k);
CREATE TABLE dg_semi_right (k UInt64, s String, w FixedString(40)) ENGINE = Join(SEMI, RIGHT, k);
CREATE TABLE dg_any_inner (k UInt64, s String, w FixedString(40)) ENGINE = Join(ANY, INNER, k);
INSERT INTO dg_any_right SELECT number % 200, concat('s', toString(number)),
    toFixedString(leftPad(toString(number), 40, 'r'), 40) FROM numbers(2000);
INSERT INTO dg_semi_right SELECT number % 200, concat('s', toString(number)),
    toFixedString(leftPad(toString(number), 40, 'r'), 40) FROM numbers(2000);
INSERT INTO dg_any_inner SELECT number % 200, concat('s', toString(number)),
    toFixedString(leftPad(toString(number), 40, 'r'), 40) FROM numbers(2000);

SELECT 'arm27 storage join any right', count(), sum(cityHash64(*))
FROM dg_list_probe ANY RIGHT JOIN dg_any_right USING (k)
SETTINGS join_use_nulls = 0, enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm27 storage join semi right', count(), sum(cityHash64(*))
FROM dg_list_probe SEMI RIGHT JOIN dg_semi_right USING (k)
SETTINGS join_use_nulls = 0, enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm27 storage join any inner', count(), sum(cityHash64(*))
FROM dg_list_probe ANY INNER JOIN dg_any_inner USING (k)
SETTINGS join_use_nulls = 0, enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 28: every `Enum` shape at once, because an `Enum` is the one fixed-width leaf whose default is
-- not zero bytes - it is the first declared value, so the kernel has to write a captured pattern
-- rather than a zeroed one. 100 of the 600 probe keys have no match, and each column pins a rule:
--   `e8`     250 matched `a` + 100 defaults        = 350
--   `e_neg`  166 matched `z` + 100 defaults        = 266, and `y` stays at its 167 matched rows -
--            a zeroed default would land on `y`, which this enum declares as one of its own values
--   `e16`    250 matched `p` + 100 defaults        = 350   (a two-byte pattern)
--   `ne`     100 NULLs, and `assumeNotNull` gives 167 matched `n` + 100 more: an unmatched row is
--            NULL, so its nested plane keeps the nested *column*'s zeroed default and not the
--            nested type's `z`, which is what `ColumnNullable::insertDefault` writes
--   `ae`     499 elements from the matched rows only - an unmatched row is the empty array
--   `te.e`   250 matched `a` + 100 defaults        = 350, taken through
--            `DataTypeTuple::insertDefaultInto`, which recurses into the element types
-- Reading `te.e` asks for a subcolumn of its own, so the tuple is emitted twice and the gathered
-- count is 8 columns rather than 7: 8 x 600 = 4800.
CREATE TABLE dg_enum
(
    k UInt64,
    e8 Enum8('a' = 1, 'b' = 2),
    e_neg Enum8('z' = -5, 'y' = 0, 'x' = 7),
    e16 Enum16('p' = 1000, 'q' = 2000),
    ne Nullable(Enum8('z' = -5, 'n' = 0, 'x' = 7)),
    ae Array(Enum8('a' = 1, 'b' = 2)),
    te Tuple(u UInt64, e Enum8('a' = 1, 'b' = 2)),
    w FixedString(40)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_enum SELECT number + 1,
    CAST(1 + number % 2, 'Enum8(\'a\' = 1, \'b\' = 2)'),
    CAST(if(number % 3 = 0, 0, if(number % 3 = 1, 7, -5)), 'Enum8(\'z\' = -5, \'y\' = 0, \'x\' = 7)'),
    CAST(1000 + 1000 * (number % 2), 'Enum16(\'p\' = 1000, \'q\' = 2000)'),
    CAST(if(number % 3 = 0, 0, if(number % 3 = 1, 7, -5)), 'Enum8(\'z\' = -5, \'n\' = 0, \'x\' = 7)'),
    CAST(range(1, 1 + number % 3), 'Array(Enum8(\'a\' = 1, \'b\' = 2))'),
    CAST((number, CAST(1 + number % 2, 'Enum8(\'a\' = 1, \'b\' = 2)')), 'Tuple(u UInt64, e Enum8(\'a\' = 1, \'b\' = 2))'),
    toFixedString(leftPad(toString(number), 40, 'w'), 40)
FROM numbers(500);

SELECT 'arm28 enum defaults', count(), sum(cityHash64(*)),
    countIf(e8 = 'a'), countIf(e_neg = 'z'), countIf(e_neg = 'y'), countIf(e16 = 'p'),
    countIf(ne IS NULL), countIf(assumeNotNull(ne) = 'n'), countIf(assumeNotNull(ne) = 'z'),
    sum(length(ae)), countIf(te.e = 'a')
FROM dg_mixed_probe LEFT JOIN dg_enum USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- The same enum shapes with ten build rows per key, which is what lets the threshold pin select the
-- emit builder: `buildOutputFromRowRefLists` is only reached at a per-key fanout above one, so a
-- unique-key fixture like the one above cannot select it. The two sub-arms therefore carry the oracle
-- for the captured pattern between them: two builders reading the same words, which must agree to the
-- byte even though one expands the row lists and the other hands them to the kernel whole.
-- `full_sorting_merge` cannot be that oracle: it fills an unmatched row from the column rather than
-- from the type, so it disagrees with every hash join on enum defaults. 50 of the 250 probe keys
-- have no match, and a matched one brings ten rows: 2000 + 50 = 2050.
CREATE TABLE dg_enum_list
(
    k UInt64,
    e8 Enum8('a' = 1, 'b' = 2),
    e_neg Enum8('z' = -5, 'y' = 0, 'x' = 7),
    ne Nullable(Enum8('z' = -5, 'n' = 0, 'x' = 7)),
    te Tuple(u UInt64, e Enum8('a' = 1, 'b' = 2)),
    w FixedString(40)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_enum_list SELECT number % 200,
    CAST(1 + number % 2, 'Enum8(\'a\' = 1, \'b\' = 2)'),
    CAST(if(number % 3 = 0, 0, if(number % 3 = 1, 7, -5)), 'Enum8(\'z\' = -5, \'y\' = 0, \'x\' = 7)'),
    CAST(if(number % 3 = 0, 0, if(number % 3 = 1, 7, -5)), 'Enum8(\'z\' = -5, \'n\' = 0, \'x\' = 7)'),
    CAST((number, CAST(1 + number % 2, 'Enum8(\'a\' = 1, \'b\' = 2)')), 'Tuple(u UInt64, e Enum8(\'a\' = 1, \'b\' = 2))'),
    toFixedString(leftPad(toString(number), 40, 'w'), 40)
FROM numbers(2000);
CREATE TABLE dg_enum_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_enum_probe SELECT number FROM numbers(250);

SELECT 'arm28 enum row list', count(), sum(cityHash64(*)),
    countIf(e8 = 'a'), countIf(e_neg = 'z'), countIf(e_neg = 'y'),
    countIf(ne IS NULL), countIf(assumeNotNull(ne) = 'n'), countIf(te.e = 'a')
FROM dg_enum_probe LEFT JOIN dg_enum_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm28 enum ref lists control', count(), sum(cityHash64(*)),
    countIf(e8 = 'a'), countIf(e_neg = 'z'), countIf(e_neg = 'y'),
    countIf(ne IS NULL), countIf(assumeNotNull(ne) = 'n'), countIf(te.e = 'a')
FROM dg_enum_probe LEFT JOIN dg_enum_list USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

-- Arm 29: `LowCardinality`, whose kernel is `ColumnLowCardinality::insertRangeFrom` bound to the
-- concrete class. That call is the whole point: it adopts the source dictionary when the destination
-- is still empty and translates only the keys a range actually uses afterwards, so an output column
-- that spans blocks with unrelated dictionaries is its problem and not the emit's. The three inserts
-- below use disjoint string sets and merges are stopped, so the stored blocks keep diverging
-- dictionaries; `uniqExact(lc)` = 7 + 11 + 13 distinct values + the empty default is what says the
-- translation happened rather than one dictionary being adopted for all of them.
-- 50 of the 250 probe keys have no match and a matched one brings ten rows: 2000 + 50 = 2050.
-- A numeric dictionary needs `allow_suspicious_low_cardinality_types`, and is worth the setting: it
-- is the one `LowCardinality` whose keys are not variable-width.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE dg_lc
(
    k UInt64,
    lc LowCardinality(String),
    lcn LowCardinality(Nullable(String)),
    lcu LowCardinality(UInt64),
    alc Array(LowCardinality(String)),
    tlc Tuple(s LowCardinality(String), i Int64),
    w FixedString(40)
)
ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES dg_lc;
INSERT INTO dg_lc SELECT number % 200, concat('a', toString(number % 7)),
    if(number % 6 = 0, NULL, concat('a', toString(number % 7))), number % 5,
    arrayMap(i -> concat('a', toString(i)), range(number % 3)),
    (concat('a', toString(number % 7)), toInt64(number)),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(0, 700);
INSERT INTO dg_lc SELECT number % 200, concat('b', toString(number % 11)),
    if(number % 6 = 0, NULL, concat('b', toString(number % 11))), 100 + number % 5,
    arrayMap(i -> concat('b', toString(i)), range(number % 3)),
    (concat('b', toString(number % 11)), toInt64(number)),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(700, 700);
INSERT INTO dg_lc SELECT number % 200, concat('c', toString(number % 13)),
    if(number % 6 = 0, NULL, concat('c', toString(number % 13))), 200 + number % 5,
    arrayMap(i -> concat('c', toString(i)), range(number % 3)),
    (concat('c', toString(number % 13)), toInt64(number)),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(1400, 600);

SELECT 'arm29 low cardinality', count(), sum(cityHash64(*)),
    countIf(lc = ''), uniqExact(lc), countIf(lcn IS NULL), uniqExact(lcn),
    sum(lcu), uniqExact(lcu), sum(length(alc)), uniqExact(arrayStringConcat(alc, ',')),
    uniqExact(tlc.s), sum(tlc.i)
FROM dg_enum_probe LEFT JOIN dg_lc USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- The other builder over the same words, which hands the kernel the list words whole instead of
-- expanding them: the values have to agree to the byte.
SELECT 'arm29 low cardinality ref lists control', count(), sum(cityHash64(*)),
    countIf(lc = ''), uniqExact(lc), countIf(lcn IS NULL), uniqExact(lcn),
    sum(lcu), uniqExact(lcu), sum(length(alc)), uniqExact(arrayStringConcat(alc, ',')),
    uniqExact(tlc.s), sum(tlc.i)
FROM dg_enum_probe LEFT JOIN dg_lc USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

SELECT 'arm29 low cardinality full_sorting_merge control', count(), sum(cityHash64(*)),
    countIf(lc = ''), uniqExact(lc), countIf(lcn IS NULL), uniqExact(lcn),
    sum(lcu), uniqExact(lcu), sum(length(alc)), uniqExact(arrayStringConcat(alc, ',')),
    uniqExact(tlc.s), sum(tlc.i)
FROM dg_enum_probe LEFT JOIN dg_lc USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- `join_use_nulls` makes every emitted column nullable, which for a `LowCardinality` one moves the
-- nullability inside the dictionary rather than wrapping the column.
SELECT 'arm29 low cardinality use nulls', count(), sum(cityHash64(*)),
    countIf(lc IS NULL), uniqExact(lc), countIf(lcn IS NULL), sum(lcu), sum(length(alc))
FROM dg_enum_probe LEFT JOIN dg_lc USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 1,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- The not-joined scan reaches the same kernels. The probe is cut to keys below 50 so that 150 of the
-- 200 build keys stay unmatched: `join_use_nulls` then makes the left key NULL on exactly the rows
-- the scan produced, which pins the split at 1500 scanned and 500 probed rather than just the total.
SELECT 'arm29 low cardinality right join', count(), sum(cityHash64(*)),
    uniqExact(lc), countIf(p.k IS NULL)
FROM (SELECT k FROM dg_enum_probe WHERE k < 50) AS p RIGHT JOIN dg_lc USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 1,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm29 low cardinality right join full_sorting_merge control', count(), sum(cityHash64(*)),
    uniqExact(lc), countIf(p.k IS NULL)
FROM (SELECT k FROM dg_enum_probe WHERE k < 50) AS p RIGHT JOIN dg_lc USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 1;

-- The continuation builder hands the emit a freshly cloned destination for every chunk, so the
-- dictionary adoption that only happens into an empty destination is re-entered per chunk.
SELECT 'arm29 low cardinality continuation', count(), sum(cityHash64(*)), uniqExact(lc), sum(lcu)
FROM dg_enum_probe LEFT JOIN dg_lc USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7;

-- A `StorageJoin` whose second query sees a block inserted after the first one resolved the emit
-- table: the generation bump has to drop the cached source pointers, and the new block's dictionary
-- has nothing in common with the ones already translated.
CREATE TABLE dg_lc_sj (k UInt64, lc LowCardinality(String), w FixedString(40))
ENGINE = Join(ALL, LEFT, k);
INSERT INTO dg_lc_sj SELECT number, concat('a', toString(number % 7)),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(100);

SELECT 'arm29 storage join before insert', count(), sum(cityHash64(*)), uniqExact(lc)
FROM dg_enum_probe LEFT JOIN dg_lc_sj USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

INSERT INTO dg_lc_sj SELECT number, concat('z', toString(number % 13)),
    toFixedString(leftPad(toString(number), 40, 'z'), 40) FROM numbers(100, 100);

SELECT 'arm29 storage join after insert', count(), sum(cityHash64(*)), uniqExact(lc)
FROM dg_enum_probe LEFT JOIN dg_lc_sj USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- A build side that is three unions rather than three parts, so per-block dictionary divergence is
-- guaranteed by the plan instead of by merges staying stopped.
SELECT 'arm29 low cardinality unioned blocks', count(), sum(cityHash64(*)), uniqExact(lc), sum(u)
FROM dg_enum_probe LEFT JOIN
(
    SELECT number AS k, toLowCardinality(concat('a', toString(number % 7))) AS lc, number AS u FROM numbers(0, 80)
    UNION ALL
    SELECT number AS k, toLowCardinality(concat('b', toString(number % 11))) AS lc, number AS u FROM numbers(80, 80)
    UNION ALL
    SELECT number AS k, toLowCardinality(concat('c', toString(number % 13))) AS lc, number AS u FROM numbers(160, 80)
) AS r USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 30: `JSON`. Its kernel is `ColumnObject::insertRangeFrom` bound to the concrete class, and the
-- three inserts below carry disjoint path sets with merges stopped, so an output column has to
-- reconcile blocks whose structures have nothing in common. That reconciliation is the only thing
-- the call does that a plane copy could not, so `uniqExact` over the sorted path list is the
-- assertion that matters: 3 shapes from the build side plus the empty object of an unmatched row.
-- `w` is read alongside `j` so the emit is mixed: a plane copy and a structural one in one call.
CREATE TABLE dg_json (k UInt64, j JSON, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES dg_json;
INSERT INTO dg_json SELECT number % 200,
    concat('{"a":', toString(number), ',"s":"x', toString(number % 7), '"}'),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(0, 700);
INSERT INTO dg_json SELECT number % 200,
    concat('{"b":', toString(number), ',"t":[1,2,', toString(number % 5), ']}'),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(700, 700);
INSERT INTO dg_json SELECT number % 200,
    concat('{"c":{"d":', toString(number), '},"u":', toString(number % 3), '}'),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(1400, 600);

SELECT 'arm30 json', count(), sum(cityHash64(toString(j))),
    countIf(toString(j) = '{}'), uniqExact(arrayStringConcat(arraySort(JSONAllPaths(j)), ',')),
    sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_json USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm30 json ref lists control', count(), sum(cityHash64(toString(j))),
    countIf(toString(j) = '{}'), uniqExact(arrayStringConcat(arraySort(JSONAllPaths(j)), ',')),
    sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_json USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

SELECT 'arm30 json full_sorting_merge control', count(), sum(cityHash64(toString(j))),
    countIf(toString(j) = '{}'), uniqExact(arrayStringConcat(arraySort(JSONAllPaths(j)), ',')),
    sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_json USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- The not-joined scan and the continuation builder over the same paths: the scan reaches the kernels
-- for 150 unmatched build keys, and the continuation hands the emit a freshly cloned destination per
-- chunk, so each chunk re-enters the structure reconciliation from an empty column.
SELECT 'arm30 json right join', count(), sum(cityHash64(toString(j), w)), countIf(p.k IS NULL)
FROM (SELECT k FROM dg_enum_probe WHERE k < 50) AS p RIGHT JOIN dg_json USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 1,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm30 json continuation', count(), sum(cityHash64(toString(j), w)),
    uniqExact(arrayStringConcat(arraySort(JSONAllPaths(j)), ','))
FROM dg_enum_probe LEFT JOIN dg_json USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7;

-- Arm 31: `Dynamic`. Its rows are a variant list rather than a path set, which is why it is its own
-- kernel and not a case of the `JSON` one, even though both end up in `insertRangeFrom` on their
-- concrete class. The three inserts hold disjoint variant sets, so `uniqExact(dynamicType(d))`
-- counts what the emit had to merge: 6 build variants plus the `None` of an unmatched row.
CREATE TABLE dg_dyn (k UInt64, d Dynamic, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES dg_dyn;
INSERT INTO dg_dyn SELECT number % 200,
    if(number % 2 = 0, CAST(toInt64(number), 'Dynamic'), CAST(concat('s', toString(number)), 'Dynamic')),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(0, 700);
INSERT INTO dg_dyn SELECT number % 200,
    if(number % 2 = 0, CAST([toUInt64(number)], 'Dynamic'), CAST(toDate('2020-01-01') + number % 900, 'Dynamic')),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(700, 700);
INSERT INTO dg_dyn SELECT number % 200,
    if(number % 3 = 0, CAST(NULL, 'Dynamic'),
       if(number % 3 = 1, CAST(toFloat64(number) / 7, 'Dynamic'), CAST(toIPv4(number), 'Dynamic'))),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(1400, 600);

SELECT 'arm31 dynamic', count(), sum(cityHash64(toString(d))), countIf(d IS NULL),
    uniqExact(dynamicType(d)), sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_dyn USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm31 dynamic ref lists control', count(), sum(cityHash64(toString(d))), countIf(d IS NULL),
    uniqExact(dynamicType(d)), sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_dyn USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

SELECT 'arm31 dynamic full_sorting_merge control', count(), sum(cityHash64(toString(d))),
    countIf(d IS NULL), uniqExact(dynamicType(d)), sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_dyn USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

SELECT 'arm31 dynamic right join', count(), sum(cityHash64(toString(d), w)), countIf(p.k IS NULL)
FROM (SELECT k FROM dg_enum_probe WHERE k < 50) AS p RIGHT JOIN dg_dyn USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 1,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm31 dynamic continuation', count(), sum(cityHash64(toString(d), w)),
    uniqExact(dynamicType(d))
FROM dg_enum_probe LEFT JOIN dg_dyn USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7;

-- Arm 32: `Map`, which is its nested `Array(Tuple(key, value))` and nothing besides, so its kernel
-- delegates to the `Array` one. What the arm has to establish is that the delegation lands on
-- covered encodings all the way down: `mlc` recurses into a `LowCardinality` value, `mm` into
-- another `Map`, and `mn` into a `Nullable` one. An unmatched row is the empty map, which is the
-- empty array the `Array` kernel already writes - 50 of the 250 probe keys have no match.
CREATE TABLE dg_map
(
    k UInt64,
    m Map(String, UInt64),
    mn Map(String, Nullable(String)),
    mlc Map(String, LowCardinality(String)),
    mm Map(String, Map(String, UInt64)),
    w FixedString(40)
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_map SELECT number % 200,
    CAST((arrayMap(i -> concat('a', toString(i)), range(number % 4)), range(number % 4)), 'Map(String, UInt64)'),
    CAST((arrayMap(i -> concat('n', toString(i)), range(number % 3)),
          arrayMap(i -> if(i % 2 = 0, NULL, concat('v', toString(i))), range(number % 3))), 'Map(String, Nullable(String))'),
    CAST((arrayMap(i -> concat('l', toString(i)), range(number % 3)),
          arrayMap(i -> concat('d', toString(i % 5)), range(number % 3))), 'Map(String, LowCardinality(String))'),
    CAST((['x'], [CAST((arrayMap(i -> concat('y', toString(i)), range(number % 3)), range(number % 3)), 'Map(String, UInt64)')]),
         'Map(String, Map(String, UInt64))'),
    toFixedString(leftPad(toString(number), 40, 'w'), 40)
FROM numbers(2000);

SELECT 'arm32 map', count(), sum(cityHash64(*)),
    countIf(length(m) = 0), sum(length(m)), sum(arraySum(mapValues(m))),
    sum(length(mn)), countIf(has(mapValues(mn), NULL)),
    sum(length(mlc)), uniqExact(arrayStringConcat(mapValues(mlc), ',')),
    sum(length(mm)), sum(length(mm['x']))
FROM dg_enum_probe LEFT JOIN dg_map USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm32 map ref lists control', count(), sum(cityHash64(*)),
    countIf(length(m) = 0), sum(length(m)), sum(arraySum(mapValues(m))),
    sum(length(mn)), countIf(has(mapValues(mn), NULL)),
    sum(length(mlc)), uniqExact(arrayStringConcat(mapValues(mlc), ',')),
    sum(length(mm)), sum(length(mm['x']))
FROM dg_enum_probe LEFT JOIN dg_map USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

SELECT 'arm32 map full_sorting_merge control', count(), sum(cityHash64(*)),
    countIf(length(m) = 0), sum(length(m)), sum(arraySum(mapValues(m))),
    sum(length(mn)), countIf(has(mapValues(mn), NULL)),
    sum(length(mlc)), uniqExact(arrayStringConcat(mapValues(mlc), ',')),
    sum(length(mm)), sum(length(mm['x']))
FROM dg_enum_probe LEFT JOIN dg_map USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- A `Map` nested under an `Array` is what makes the delegation take the range path rather than the
-- row one: the `Array` kernel hands its child contiguous runs of nested rows, and the `Map` kernel
-- has to pass them through to its own nested array.
SELECT 'arm32 map under array', count(), sum(cityHash64(am)), sum(arraySum(arrayMap(x -> length(x), am)))
FROM dg_enum_probe LEFT JOIN
(
    SELECT number % 200 AS k,
        [CAST((['p'], [toUInt64(number)]), 'Map(String, UInt64)'),
         CAST((arrayMap(i -> concat('q', toString(i)), range(number % 3)), range(number % 3)), 'Map(String, UInt64)')] AS am
    FROM numbers(2000)
) AS r USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm32 map under array full_sorting_merge control', count(), sum(cityHash64(am)),
    sum(arraySum(arrayMap(x -> length(x), am)))
FROM dg_enum_probe LEFT JOIN
(
    SELECT number % 200 AS k,
        [CAST((['p'], [toUInt64(number)]), 'Map(String, UInt64)'),
         CAST((arrayMap(i -> concat('q', toString(i)), range(number % 3)), range(number % 3)), 'Map(String, UInt64)')] AS am
    FROM numbers(2000)
) AS r USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- Arm 33: `AggregateFunction`. A row is a pointer to a state owned by one source column's arena, so
-- this is the encoding where copying rows is a question of ownership rather than of layout: the
-- output spans three stored blocks here, and `ColumnAggregateFunction::insertRangeFrom` is what has
-- to keep all three arenas alive behind it. `uniqExact` and `groupArray` are the two states that
-- actually live in an arena; `sum` is the one that does not. An unmatched row allocates a fresh
-- state, which is why the defaults are counted rather than assumed.
CREATE TABLE dg_agg
(
    k UInt64,
    s AggregateFunction(sum, UInt64),
    u AggregateFunction(uniqExact, String),
    ga AggregateFunction(groupArray, UInt64),
    w FixedString(40)
)
ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES dg_agg;
INSERT INTO dg_agg SELECT k, sumState(v), uniqExactState(sv), groupArrayState(v), any(wf)
FROM (SELECT number % 200 AS k, intDiv(number, 200) AS g, number + 1 AS v, toString(number % 7) AS sv,
             toFixedString(leftPad(toString(number), 40, 'w'), 40) AS wf FROM numbers(0, 700))
GROUP BY k, g;
INSERT INTO dg_agg SELECT k, sumState(v), uniqExactState(sv), groupArrayState(v), any(wf)
FROM (SELECT number % 200 AS k, intDiv(number, 200) AS g, number + 1 AS v, toString(number % 11) AS sv,
             toFixedString(leftPad(toString(number), 40, 'w'), 40) AS wf FROM numbers(700, 700))
GROUP BY k, g;
INSERT INTO dg_agg SELECT k, sumState(v), uniqExactState(sv), groupArrayState(v), any(wf)
FROM (SELECT number % 200 AS k, intDiv(number, 200) AS g, number + 1 AS v, toString(number % 13) AS sv,
             toFixedString(leftPad(toString(number), 40, 'w'), 40) AS wf FROM numbers(1400, 600))
GROUP BY k, g;

SELECT 'arm33 aggregate function', count(), sum(finalizeAggregation(s)),
    countIf(finalizeAggregation(s) = 0), sum(finalizeAggregation(u)),
    sum(length(finalizeAggregation(ga))), countIf(empty(finalizeAggregation(ga))),
    uniqExact(w)
FROM dg_enum_probe LEFT JOIN dg_agg USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm33 aggregate function ref lists control', count(), sum(finalizeAggregation(s)),
    countIf(finalizeAggregation(s) = 0), sum(finalizeAggregation(u)),
    sum(length(finalizeAggregation(ga))), countIf(empty(finalizeAggregation(ga))),
    uniqExact(w)
FROM dg_enum_probe LEFT JOIN dg_agg USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

SELECT 'arm33 aggregate function full_sorting_merge control', count(), sum(finalizeAggregation(s)),
    countIf(finalizeAggregation(s) = 0), sum(finalizeAggregation(u)),
    sum(length(finalizeAggregation(ga))), countIf(empty(finalizeAggregation(ga))),
    uniqExact(w)
FROM dg_enum_probe LEFT JOIN dg_agg USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- The continuation builder is where a dangling state would show: it clones a fresh destination per
-- chunk, so each chunk takes its own references into the source arenas and drops them again.
SELECT 'arm33 aggregate function continuation', count(), sum(finalizeAggregation(s)),
    sum(finalizeAggregation(u)), sum(length(finalizeAggregation(ga)))
FROM dg_enum_probe LEFT JOIN dg_agg USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 1,
    max_joined_block_size_rows = 7;

-- Arm 34: the three encodings that close the set. `QBit` keeps its dimension and stride invariants
-- inside `ColumnQBit`, so its own `insertRangeFrom` is the kernel; an element-less `Tuple` and a
-- `Nothing` carry no values at all, so copying a row of either is a size bump. The last two cannot be
-- table columns, so they arrive from a subquery, where they start out constant and the build boundary
-- materializes them - which is the only way they reach an emit kernel.
CREATE TABLE dg_qbit (k UInt64, q QBit(Float32, 8), w FixedString(40)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dg_qbit SELECT number % 200,
    CAST(arrayMap(i -> toFloat32(number + i), range(8)) AS QBit(Float32, 8)),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(2000);

SELECT 'arm34 qbit', count(), sum(cityHash64(toString(q))),
    countIf(toString(q) = '[0,0,0,0,0,0,0,0]'), sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_qbit USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm34 qbit ref lists control', count(), sum(cityHash64(toString(q))),
    countIf(toString(q) = '[0,0,0,0,0,0,0,0]'), sum(cityHash64(w))
FROM dg_enum_probe LEFT JOIN dg_qbit USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

-- `toString` is what forces the value-less columns into the join output at all: an expression over
-- them alone folds to a constant, and then nothing is emitted and nothing is gathered.
SELECT 'arm34 element-less tuple and nothing', count(), uniqExact(toString(t)),
    uniqExact(toString(n)), countIf(n IS NULL), sum(u)
FROM dg_enum_probe LEFT JOIN
(SELECT number % 200 AS k, tuple() AS t, NULL AS n, number AS u FROM numbers(2000)) AS r USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm34 element-less tuple and nothing ref lists control', count(), uniqExact(toString(t)),
    uniqExact(toString(n)), countIf(n IS NULL), sum(u)
FROM dg_enum_probe LEFT JOIN
(SELECT number % 200 AS k, tuple() AS t, NULL AS n, number AS u FROM numbers(2000)) AS r USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 0, joined_block_split_single_row = 0;

-- Arm 35: an empty right side. Every emitted row is a default and there is no stored block to take
-- the shape from, so the emit resolves it from a column of the output type instead - a plane pointer
-- would otherwise have nothing to point at. All 250 rows are defaults, which makes the whole arm its
-- own oracle, and `countIf(e = 'z')` is the captured enum pattern surviving a join with no data.
CREATE TABLE dg_empty
(
    k UInt64,
    a UInt64,
    s String,
    lc LowCardinality(String),
    m Map(String, UInt64),
    e Enum8('z' = -5, 'y' = 0),
    j JSON,
    w FixedString(40)
)
ENGINE = MergeTree ORDER BY tuple();

SELECT 'arm35 empty right side', count(), sum(a), countIf(s = ''), countIf(lc = ''),
    countIf(length(m) = 0), countIf(e = 'z'), countIf(e = 'y'), countIf(toString(j) = '{}'),
    countIf(w = toFixedString('', 40))
FROM dg_enum_probe LEFT JOIN dg_empty USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

-- Arm 36: the two column classes that have no kernel and must never reach one. A sparse stored
-- column is unwrapped by `recursiveRemoveSparse` and a constant one by `convertToFullColumnIfConst`,
-- both at the build boundary, so the emit only ever sees the full column - and if either stopped
-- happening, the resolve would throw rather than quietly slow down. The build side is 99% default so
-- the part really is written sparsely, and `s` and `c` arrive constant from the subquery.
CREATE TABLE dg_sparse (k UInt64, sp UInt64, w FixedString(40)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.1;
INSERT INTO dg_sparse SELECT number % 200, if(number % 100 = 0, number, 0),
    toFixedString(leftPad(toString(number), 40, 'w'), 40) FROM numbers(2000);

SELECT 'arm36 sparse and const', count(), sum(sp), countIf(sp = 0), sum(cityHash64(w)),
    uniqExact(c), uniqExact(s)
FROM dg_enum_probe LEFT JOIN
(SELECT k, sp, w, 7 AS c, 'x' AS s FROM dg_sparse) AS r USING (k)
SETTINGS join_algorithm = 'hash', query_plan_join_swap_table = 0, join_use_nulls = 0,
    enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm36 sparse and const full_sorting_merge control', count(), sum(sp), countIf(sp = 0),
    sum(cityHash64(w)), uniqExact(c), uniqExact(s)
FROM dg_enum_probe LEFT JOIN
(SELECT k, sp, w, 7 AS c, 'x' AS s FROM dg_sparse) AS r USING (k)
SETTINGS join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0, join_use_nulls = 0;

-- Arm 25: `joinGet` is forced `Left Any`, so it is the one consumer that emits through
-- `buildJoinGetOutput` and its nullable wrap rather than through the kernels: a zero word becomes
-- the type default for `joinGet` and NULL for `joinGetOrNull`, over both a fixed-width and a
-- variable-width column. It therefore gathers nothing, and the values are what pin the wrap.
CREATE TABLE dg_jg (k UInt64, a UInt64, s String, w FixedString(40)) ENGINE = Join(ANY, LEFT, k);
INSERT INTO dg_jg SELECT number, number * 1000003, concat('s', toString(number)),
    toFixedString(leftPad(toString(number), 40, 'j'), 40) FROM numbers(100);

SELECT 'arm25 joinGet hit and miss', joinGet(dg_jg, 'a', toUInt64(7)), joinGet(dg_jg, 's', toUInt64(7)),
    joinGet(dg_jg, 'a', toUInt64(500)), joinGet(dg_jg, 's', toUInt64(500)) = '';

SELECT 'arm25 joinGetOrNull hit and miss', joinGetOrNull(dg_jg, 'a', toUInt64(7)),
    joinGetOrNull(dg_jg, 's', toUInt64(7)), joinGetOrNull(dg_jg, 'a', toUInt64(500)) IS NULL,
    joinGetOrNull(dg_jg, 's', toUInt64(500)) IS NULL;

SELECT 'arm25 joinGet over a range', count(), sum(joinGet(dg_jg, 'a', number)),
    sum(cityHash64(joinGet(dg_jg, 'w', number))), countIf(joinGetOrNull(dg_jg, 'a', number) IS NULL)
FROM numbers(200);

-- Arm 20: `StorageJoin` inserts more build blocks between queries and lets each query select its own
-- subset of the right columns, both of which the emit table has to survive: the insert bumps the
-- stored-blocks generation, and the second query asks for positions the first one never resolved.
-- The engine is declared `ALL` so a plain `LEFT JOIN` matches it and takes the lazy emit path.
CREATE TABLE dg_sj (k UInt64, a UInt64, s String, w FixedString(40)) ENGINE = Join(ALL, LEFT, k);
INSERT INTO dg_sj SELECT number, number * 1000003, concat('s', toString(number)),
    toFixedString(leftPad(toString(number), 40, 'j'), 40) FROM numbers(500);

SELECT 'arm20 storage join subset a', count(), sum(cityHash64(k, a)) FROM dg_probe LEFT JOIN dg_sj USING (k)
SETTINGS join_use_nulls = 0, enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

INSERT INTO dg_sj SELECT number, number * 1000003, concat('s', toString(number)),
    toFixedString(leftPad(toString(number), 40, 'j'), 40) FROM numbers(500, 500);

SELECT 'arm20 storage join subset s w after insert', count(), sum(cityHash64(k, s, w))
FROM dg_probe LEFT JOIN dg_sj USING (k)
SETTINGS join_use_nulls = 0, enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

SELECT 'arm20 storage join all columns', count(), sum(cityHash64(*)) FROM dg_probe LEFT JOIN dg_sj USING (k)
SETTINGS join_use_nulls = 0, enable_hash_join_row_store = 0,
    join_output_by_rowlist_perkey_rows_threshold = 1000000, joined_block_split_single_row = 0;

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
DROP TABLE dg_interval;
DROP TABLE dg_asof;
DROP TABLE dg_asof_probe;
DROP TABLE dg_ext;
DROP TABLE dg_ext_probe;
DROP TABLE dg_variant;
DROP TABLE dg_variant_ord1;
DROP TABLE dg_variant_ord2;
DROP TABLE dg_variant_array;
DROP TABLE dg_tuple_enum;
DROP TABLE dg_enum;
DROP TABLE dg_enum_list;
DROP TABLE dg_enum_probe;
DROP TABLE dg_lc;
DROP TABLE dg_lc_sj;
DROP TABLE dg_json;
DROP TABLE dg_dyn;
DROP TABLE dg_map;
DROP TABLE dg_agg;
DROP TABLE dg_qbit;
DROP TABLE dg_empty;
DROP TABLE dg_sparse;
DROP TABLE dg_list_ext;
DROP TABLE dg_nullkey;
DROP TABLE dg_or;
DROP TABLE dg_or_probe;
DROP TABLE dg_sj;
DROP TABLE dg_jg;
DROP TABLE dg_any_right;
DROP TABLE dg_semi_right;
DROP TABLE dg_any_inner;
