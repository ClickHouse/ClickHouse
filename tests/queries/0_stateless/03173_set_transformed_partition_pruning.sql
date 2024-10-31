SELECT '-- Single partition by function';

DROP TABLE IF EXISTS 03173_single_function;
CREATE TABLE 03173_single_function (
    dt Date,
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY toMonth(dt);

INSERT INTO 03173_single_function
SELECT toDate('2000-01-01') + 10 * number FROM numbers(50)
UNION ALL
SELECT toDate('2100-01-01') + 10 * number FROM numbers(50);
OPTIMIZE TABLE 03173_single_function FINAL;

SELECT count() FROM 03173_single_function WHERE dt IN ('2024-01-20', '2024-05-25') SETTINGS log_comment='03173_single_function';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_single_function';

DROP TABLE IF EXISTS 03173_single_function;

SELECT '-- Nested partition by function';

DROP TABLE IF EXISTS 03173_nested_function;
CREATE TABLE 03173_nested_function(
    id Int32,
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY xxHash32(id) % 3;

INSERT INTO 03173_nested_function SELECT number FROM numbers(100);
OPTIMIZE TABLE 03173_nested_function FINAL;

SELECT count() FROM 03173_nested_function WHERE id IN (10) SETTINGS log_comment='03173_nested_function';
SELECT count() FROM 03173_nested_function WHERE xxHash32(id) IN (2158931063, 1449383981) SETTINGS log_comment='03173_nested_function_subexpr';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function';
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function_subexpr';

DROP TABLE IF EXISTS 03173_nested_function;

SELECT '-- Nested partition by function, LowCardinality';

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS 03173_nested_function_lc;
CREATE TABLE 03173_nested_function_lc(
    id LowCardinality(Int32),
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY xxHash32(id) % 3;

INSERT INTO 03173_nested_function_lc SELECT number FROM numbers(100);
OPTIMIZE TABLE 03173_nested_function_lc FINAL;

SELECT count() FROM 03173_nested_function_lc WHERE id IN (10) SETTINGS log_comment='03173_nested_function_lc';
SELECT count() FROM 03173_nested_function_lc WHERE xxHash32(id) IN (2158931063, 1449383981) SETTINGS log_comment='03173_nested_function_subexpr_lc';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function_lc';
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function_subexpr_lc';

DROP TABLE IF EXISTS 03173_nested_function_lc;

SELECT '-- Nested partition by function, Nullable';

DROP TABLE IF EXISTS 03173_nested_function_null;
CREATE TABLE 03173_nested_function_null(
    id Nullable(Int32),
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY xxHash32(id) % 3
SETTINGS allow_nullable_key=1;

INSERT INTO 03173_nested_function_null SELECT number FROM numbers(100);
OPTIMIZE TABLE 03173_nested_function_null FINAL;

SELECT count() FROM 03173_nested_function_null WHERE id IN (10) SETTINGS log_comment='03173_nested_function_null';
SELECT count() FROM 03173_nested_function_null WHERE xxHash32(id) IN (2158931063, 1449383981) SETTINGS log_comment='03173_nested_function_subexpr_null';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function_null';
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function_subexpr_null';

DROP TABLE IF EXISTS 03173_nested_function_null;

SELECT '-- Nested partition by function, LowCardinality + Nullable';

DROP TABLE IF EXISTS 03173_nested_function_lc_null;

SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE 03173_nested_function_lc_null(
    id LowCardinality(Nullable(Int32)),
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY xxHash32(id) % 3
SETTINGS allow_nullable_key=1;

INSERT INTO 03173_nested_function_lc_null SELECT number FROM numbers(100);
OPTIMIZE TABLE 03173_nested_function_lc_null FINAL;

SELECT count() FROM 03173_nested_function_lc_null WHERE id IN (10) SETTINGS log_comment='03173_nested_function_lc_null';
SELECT count() FROM 03173_nested_function_lc_null WHERE xxHash32(id) IN (2158931063, 1449383981) SETTINGS log_comment='03173_nested_function_subexpr_lc_null';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function_lc_null';
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_function_subexpr_lc_null';

DROP TABLE IF EXISTS 03173_nested_function_lc_null;

SELECT '-- Non-safe cast';

DROP TABLE IF EXISTS 03173_nonsafe_cast;
CREATE TABLE 03173_nonsafe_cast(
    id Int64,
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY xxHash32(id) % 3;

INSERT INTO 03173_nonsafe_cast SELECT number FROM numbers(100);
OPTIMIZE TABLE 03173_nonsafe_cast FINAL;

SELECT count() FROM 03173_nonsafe_cast WHERE id IN (SELECT '50' UNION ALL SELECT '99') SETTINGS log_comment='03173_nonsafe_cast';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nonsafe_cast';

DROP TABLE IF EXISTS 03173_nonsafe_cast;

SELECT '-- Multiple partition columns';

DROP TABLE IF EXISTS 03173_multiple_partition_cols;
CREATE TABLE 03173_multiple_partition_cols (
    key1 Int32,
    key2 Int32
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY (intDiv(key1, 50), xxHash32(key2) % 3);

INSERT INTO 03173_multiple_partition_cols SELECT number, number FROM numbers(100);
OPTIMIZE TABLE 03173_multiple_partition_cols FINAL;

SELECT count() FROM 03173_multiple_partition_cols WHERE key2 IN (4) SETTINGS log_comment='03173_multiple_columns';
SELECT count() FROM 03173_multiple_partition_cols WHERE xxHash32(key2) IN (4251411170) SETTINGS log_comment='03173_multiple_columns_subexpr';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_multiple_columns';
-- Due to xxHash32() in WHERE condition, MinMax is unable to eliminate any parts,
-- so partition pruning leave two parts (for key1 // 50 = 0 and key1 // 50 = 1)
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_multiple_columns_subexpr';

-- Preparing base table for filtering by LowCardinality/Nullable sets
DROP TABLE IF EXISTS 03173_base_data_source;
CREATE TABLE 03173_base_data_source(
    id Int32,
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY xxHash32(id) % 3;

INSERT INTO 03173_base_data_source SELECT number FROM numbers(100);
OPTIMIZE TABLE 03173_base_data_source FINAL;

SELECT '-- LowCardinality set';

SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS 03173_low_cardinality_set;
CREATE TABLE 03173_low_cardinality_set (id LowCardinality(Int32)) ENGINE=Memory AS SELECT 10;

SELECT count() FROM 03173_base_data_source WHERE id IN (SELECT id FROM 03173_low_cardinality_set) SETTINGS log_comment='03173_low_cardinality_set';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_low_cardinality_set';

DROP TABLE IF EXISTS 03173_low_cardinality_set;

SELECT '-- Nullable set';

DROP TABLE IF EXISTS 03173_nullable_set;
CREATE TABLE 03173_nullable_set (id Nullable(Int32)) ENGINE=Memory AS SELECT 10;

SELECT count() FROM 03173_base_data_source WHERE id IN (SELECT id FROM 03173_nullable_set) SETTINGS log_comment='03173_nullable_set';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nullable_set';

DROP TABLE IF EXISTS 03173_nullable_set;

SELECT '-- LowCardinality + Nullable set';

DROP TABLE IF EXISTS 03173_lc_nullable_set;
CREATE TABLE 03173_lc_nullable_set (id LowCardinality(Nullable(Int32))) ENGINE=Memory AS SELECT 10 UNION ALL SELECT NULL;

SELECT count() FROM 03173_base_data_source WHERE id IN (SELECT id FROM 03173_lc_nullable_set) SETTINGS log_comment='03173_lc_nullable_set';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_lc_nullable_set';

DROP TABLE IF EXISTS 03173_lc_nullable_set;

SELECT '-- Not failing with date parsing functions';

DROP TABLE IF EXISTS 03173_date_parsing;
CREATE TABLE 03173_date_parsing (
    id String
)
ENGINE=MergeTree
ORDER BY tuple()
PARTITION BY toDate(id);

INSERT INTO 03173_date_parsing
SELECT toString(toDate('2023-04-01') + number)
FROM numbers(20);

SELECT count() FROM 03173_date_parsing WHERE id IN ('2023-04-02', '2023-05-02');
SELECT count() FROM 03173_date_parsing WHERE id IN ('not a date');

DROP TABLE IF EXISTS 03173_date_parsing;

SELECT '-- Pruning + not failing with nested date parsing functions';

DROP TABLE IF EXISTS 03173_nested_date_parsing;
CREATE TABLE 03173_nested_date_parsing (
    id String
)
ENGINE=MergeTree
ORDER BY tuple()
PARTITION BY toMonth(toDate(id));

INSERT INTO 03173_nested_date_parsing
SELECT toString(toDate('2000-01-01') + 10 * number) FROM numbers(50)
UNION ALL
SELECT toString(toDate('2100-01-01') + 10 * number) FROM numbers(50);

SELECT count() FROM 03173_nested_date_parsing WHERE id IN ('2000-01-21', '2023-05-02') SETTINGS log_comment='03173_nested_date_parsing';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_nested_date_parsing';
SELECT count() FROM 03173_nested_date_parsing WHERE id IN ('not a date');

DROP TABLE IF EXISTS 03173_nested_date_parsing;

SELECT '-- Empty transform functions';

DROP TABLE IF EXISTS 03173_empty_transform;
CREATE TABLE 03173_empty_transform(
    id Int32,
)
ENGINE = MergeTree
ORDER BY tuple()
PARTITION BY xxHash32(id) % 3;

INSERT INTO 03173_empty_transform SELECT number FROM numbers(6);
OPTIMIZE TABLE 03173_empty_transform FINAL;

SELECT id FROM 03173_empty_transform WHERE xxHash32(id) % 3 IN (xxHash32(2::Int32) % 3) SETTINGS log_comment='03173_empty_transform';
SYSTEM FLUSH LOGS;
SELECT ProfileEvents['SelectedParts'] FROM system.query_log WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '03173_empty_transform';

DROP TABLE IF EXISTS 03173_empty_transform;
