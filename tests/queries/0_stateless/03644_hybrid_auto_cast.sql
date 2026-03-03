SET allow_experimental_hybrid_table = 1,
    prefer_localhost_replica = 0;

DROP TABLE IF EXISTS test_tiered_watermark_after;
DROP TABLE IF EXISTS test_tiered_watermark_before;
DROP TABLE IF EXISTS test_tiered_watermark;

CREATE TABLE test_tiered_watermark_after
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt64,
    `categories` Array(UInt32)
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE test_tiered_watermark_before
(
    `id` Int32,
    `name` Nullable(String),
    `date` Date,
    `value` Decimal128(0),
    `categories` Array(Int64)
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_tiered_watermark_after VALUES
    (11, 'Alice', '2025-08-15', 100, [100, 10]),
    (12, 'Bob', '2025-08-20', 200, [200, 20]),
    (13, 'Charlie', '2025-08-25', 300, [300, 30]),
    (14, 'David', '2025-09-05', 400, [400, 40]),
    (15, 'Eve', '2025-09-10', 500, [500, 50]),
    (16, 'Frank', '2025-09-15', 600, [600, 60]);

INSERT INTO test_tiered_watermark_before VALUES
    (21, 'Alice', '2025-08-15', 100, [100, 10]),
    (22, 'Bob', '2025-08-20', 200, [200, 20]),
    (23, 'Charlie', '2025-08-25', 300, [300, 30]),
    (24, 'David', '2025-09-05', 400, [400, 40]),
    (25, 'Eve', '2025-09-10', 500, [500, 50]),
    (26, 'Frank', '2025-09-15', 600, [600, 60]);

CREATE TABLE test_tiered_watermark
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'test_tiered_watermark_after'),
    date >= '2025-09-01',
    remote('127.0.0.1:9000', currentDatabase(), 'test_tiered_watermark_before'),
    date < '2025-09-01'
);

-- the problem
SELECT 'hybrid_table_auto_cast_columns = 0, enable_analyzer = 1 (headers mismatch)';
SET hybrid_table_auto_cast_columns = 0, enable_analyzer = 1;
SELECT max(value) FROM test_tiered_watermark; -- { serverError CANNOT_CONVERT_TYPE }
SELECT sum(if(arrayExists(x -> (x IN (10)), categories), 1, 0)) AS x FROM test_tiered_watermark; -- { serverError THERE_IS_NO_COLUMN }

SELECT 'hybrid_table_auto_cast_columns = 0, enable_analyzer = 0 (headers mismatch)';
SET hybrid_table_auto_cast_columns = 0, enable_analyzer = 0;
SELECT max(value) FROM test_tiered_watermark; -- { serverError CANNOT_CONVERT_TYPE }
SELECT sum(if(arrayExists(x -> (x IN (10)), categories), 1, 0)) AS x FROM test_tiered_watermark; -- works w/o analyzer

-- workaround - explicit cast
SELECT 'hybrid_table_auto_cast_columns = 0, enable_analyzer = 1 manual cast';
SET hybrid_table_auto_cast_columns = 0, enable_analyzer = 1;
SELECT max(value::UInt32) FROM test_tiered_watermark;
SELECT sum(if(arrayExists(x -> (x IN (10)), categories::Array(UInt32)), 1, 0)) AS x FROM test_tiered_watermark;

SELECT 'hybrid_table_auto_cast_columns = 0, enable_analyzer = 0 manual cast';
SET hybrid_table_auto_cast_columns = 0, enable_analyzer = 0;
SELECT max(value::UInt32) FROM test_tiered_watermark;
SELECT sum(if(arrayExists(x -> (x IN (10)), categories::Array(UInt32)), 1, 0)) AS x FROM test_tiered_watermark;

-- feature to add casts automatically
SELECT 'hybrid_table_auto_cast_columns = 1, enable_analyzer = 1';
SET hybrid_table_auto_cast_columns = 1, enable_analyzer = 1;
SELECT max(value) FROM test_tiered_watermark;
SELECT sum(if(arrayExists(x -> (x IN (10)), categories), 1, 0)) AS x FROM test_tiered_watermark;

SELECT 'hybrid_table_auto_cast_columns = 1, enable_analyzer = 0 (analizer required)';
SET hybrid_table_auto_cast_columns = 1, enable_analyzer = 0;
SELECT max(value) FROM test_tiered_watermark; -- { serverError CANNOT_CONVERT_TYPE }

