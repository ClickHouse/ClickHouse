SELECT 'Hybrid creation requires allow_experimental_hybrid_table';
SET allow_experimental_hybrid_table = 0;
CREATE TABLE test_hybrid_requires_setting (`dummy` UInt8) ENGINE = Hybrid(remote('localhost:9000'), 1); -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE IF EXISTS test_hybrid_requires_setting SYNC;

SET allow_experimental_hybrid_table = 1;

SELECT 'Check Hybrid engine is registered';
SELECT name FROM system.table_engines WHERE name = 'Hybrid';

SELECT 'Ensure no leftovers before validation checks';
DROP TABLE IF EXISTS test_tiered_distributed SYNC;
DROP TABLE IF EXISTS test_tiered_distributed_bad_args SYNC;
DROP TABLE IF EXISTS test_tiered_distributed_invalid_first_arg SYNC;

SELECT 'Expect error when Hybrid has no arguments';
CREATE TABLE test_tiered_distributed_bad_args (`id` UInt32,`name` String) ENGINE = Hybrid(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 'Expect error when Hybrid has a single literal argument';
CREATE TABLE test_tiered_distributed_bad_args (`id` UInt32,`name` String) ENGINE = Hybrid(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 'Expect error when Hybrid arguments are literals only';
CREATE TABLE test_tiered_distributed_bad_args (`id` UInt32,`name` String) ENGINE = Hybrid(1, 1); -- { serverError BAD_ARGUMENTS }

SELECT 'Expect error when first argument is a table function of the wrong subtype (can not construct Distributed from file)';
CREATE TABLE test_tiered_distributed_invalid_first_arg (`id` UInt32, `name` String) ENGINE = Hybrid(file('foo.x'), 1); -- { serverError BAD_ARGUMENTS }

SELECT 'Expect error when first argument is not a table function (scalar expression)';
CREATE TABLE test_tiered_distributed_invalid_first_arg (`id` UInt32, `name` String) ENGINE = Hybrid(sin(3), 1); -- { serverError BAD_ARGUMENTS }

SELECT 'Expect error when first argument is a table function of the wrong subtype (can not construct Distributed from url)';
CREATE TABLE test_tiered_distributed_invalid_first_arg (`id` UInt32, `name` String) ENGINE = Hybrid(url('http://google.com', 'RawBLOB'), 1); -- { serverError BAD_ARGUMENTS }

SELECT 'Expect error when predicate references a missing column';
CREATE TABLE test_tiered_distributed_bad_args(`number` UInt64) ENGINE = Hybrid(remote('localhost:9000', system.numbers), number2 < 5); -- { serverError BAD_ARGUMENTS }

SELECT 'Missing column + schema inference';
CREATE TABLE test_tiered_distributed_bad_args ENGINE = Hybrid(remote('localhost:9000', system.numbers), number2 < 5); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_tiered_distributed_bad_args SYNC;

SELECT 'Create Hybrid table with remote() and constant predicate (explicit column list)';
DROP TABLE IF EXISTS test_tiered_distributed SYNC;
CREATE TABLE test_tiered_distributed(`dummy` UInt8) ENGINE = Hybrid(remote('localhost:9000'), 1);
SHOW CREATE TABLE test_tiered_distributed;
DESCRIBE TABLE test_tiered_distributed;
SELECT * FROM test_tiered_distributed;
SELECT database, name, engine, create_table_query, engine_full FROM system.tables WHERE table = 'test_tiered_distributed' FORMAT Vertical;
DROP TABLE IF EXISTS test_tiered_distributed SYNC;

SELECT 'Create Hybrid table with remote table function and predicate (inference)';
DROP TABLE IF EXISTS test_tiered_distributed_numbers_range SYNC;
CREATE TABLE test_tiered_distributed_numbers_range ENGINE = Hybrid(remote('localhost:9000', system.numbers), number < 5);
SHOW CREATE TABLE test_tiered_distributed_numbers_range;
SELECT * FROM test_tiered_distributed_numbers_range ORDER BY number;
DROP TABLE IF EXISTS test_tiered_distributed_numbers_range SYNC;

SELECT 'Create Hybrid table with two remote segments as table';
DROP TABLE IF EXISTS test_tiered_distributed_numbers_dual SYNC;
CREATE TABLE test_tiered_distributed_numbers_dual ENGINE = Hybrid(
    remote('localhost:9000', system.numbers), number < 5,
    remote('localhost:9000', system.numbers), number BETWEEN 10 AND 15
) AS system.numbers;

SHOW CREATE TABLE test_tiered_distributed_numbers_dual;
SELECT * FROM test_tiered_distributed_numbers_dual ORDER BY number SETTINGS enable_analyzer = 0;
SELECT * FROM test_tiered_distributed_numbers_dual ORDER BY number SETTINGS enable_analyzer = 1;
DROP TABLE IF EXISTS test_tiered_distributed_numbers_dual SYNC;

SELECT 'Create Hybrid table combining remote function and local table';
DROP TABLE IF EXISTS test_tiered_distributed_numbers_mixed SYNC;
CREATE TABLE test_tiered_distributed_numbers_mixed
(
    `number` UInt64
) ENGINE = Hybrid(
    remote('localhost:9000', system.numbers), number < 5,
    system.numbers, number BETWEEN 10 AND 15
);
SELECT * FROM test_tiered_distributed_numbers_mixed ORDER BY number;
DROP TABLE IF EXISTS test_tiered_distributed_numbers_mixed SYNC;

SELECT 'Verify Hybrid skips segment with always false predicate on the first segment';
DROP TABLE IF EXISTS test_tiered_distributed_numbers_skip_first SYNC;
CREATE TABLE test_tiered_distributed_numbers_skip_first
(
    `number` UInt64
) ENGINE = Hybrid(
    remote('localhost:9000', system.numbers), 0,
    system.numbers, number BETWEEN 10 AND 15
);
SELECT * FROM test_tiered_distributed_numbers_skip_first ORDER BY number;
DROP TABLE IF EXISTS test_tiered_distributed_numbers_skip_first SYNC;

SELECT 'Verify Hybrid skips segment with always false predicate on the second segment';
DROP TABLE IF EXISTS test_tiered_distributed_numbers_skip_second SYNC;
CREATE TABLE test_tiered_distributed_numbers_skip_second
(
    `number` UInt64
) ENGINE = Hybrid(
    remote('localhost:9000', system.numbers), number < 3,
    system.numbers, 0
);
SELECT * FROM test_tiered_distributed_numbers_skip_second ORDER BY number;
DROP TABLE IF EXISTS test_tiered_distributed_numbers_skip_second SYNC;

SELECT 'Hybrid raises when a segment is missing a column used by the base schema';
DROP TABLE IF EXISTS test_hybrid_segment_full SYNC;
DROP TABLE IF EXISTS test_hybrid_segment_partial SYNC;
DROP TABLE IF EXISTS test_hybrid_missing_column SYNC;

CREATE TABLE test_hybrid_segment_full
(
    `id` UInt32,
    `value` UInt32
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE test_hybrid_segment_partial
(
    `id` UInt32
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_hybrid_segment_full VALUES (1, 10), (2, 20);
INSERT INTO test_hybrid_segment_partial VALUES (3), (4);

CREATE TABLE test_hybrid_missing_column ENGINE = Hybrid(
    remote('localhost:9000', currentDatabase(), 'test_hybrid_segment_full'), id < 3,
    remote('localhost:9000', currentDatabase(), 'test_hybrid_segment_partial'), id >= 3
); -- { serverError BAD_ARGUMENTS }

DROP TABLE IF EXISTS test_hybrid_missing_column SYNC;
DROP TABLE IF EXISTS test_hybrid_segment_partial SYNC;
DROP TABLE IF EXISTS test_hybrid_segment_full SYNC;

-----------------------------

SELECT 'Prepare local MergeTree table for multi-segment tests';
DROP TABLE IF EXISTS test_tiered_local_data SYNC;
CREATE TABLE test_tiered_local_data
(
    `id` UInt32,
    `name` String,
    `event_time` DateTime,
    `value` Float64
) ENGINE = MergeTree()
ORDER BY id;

SELECT 'Populate local table with sample data';
INSERT INTO test_tiered_local_data VALUES
    (0, 'Invalid', '2022-01-01 10:00:00', 0.5),
    (1, 'Alice', '2022-01-01 10:00:00', 100.5),
    (2, 'Bob', '2022-01-02 11:00:00', 200.3),
    (3, 'Charlie', '2022-01-03 12:00:00', 150.7),
    (4, 'David', '2022-01-04 13:00:00', 300.2),
    (5, 'Eve', '2022-01-05 14:00:00', 250.1);

SELECT 'Create Hybrid table with three segment pairs';
DROP TABLE IF EXISTS test_tiered_multi_segment SYNC;

CREATE TABLE test_tiered_multi_segment
(
    `id` UInt32,
    `name` String,
    `event_time` DateTime,
    `value` Float64
)
ENGINE = Hybrid(
    remote('127.0.0.2:9000', currentDatabase(), 'test_tiered_local_data'),
    id <= 2,
    cluster('test_shard_localhost', currentDatabase(), 'test_tiered_local_data'),
    id = 3,
    remoteSecure('127.0.0.1:9440', currentDatabase(), 'test_tiered_local_data'),
    id > 3
);

SELECT 'Count rows across all segments';
SELECT count() FROM test_tiered_multi_segment;
SELECT 'Count rows from segments with id > 4';
SELECT count() FROM test_tiered_multi_segment WHERE id > 4;
SELECT 'Count rows where value > 200';
SELECT count() FROM test_tiered_multi_segment WHERE value > 200;
SELECT 'Count rows named Alice';
SELECT count() AS alice_rows FROM test_tiered_multi_segment WHERE name = 'Alice';

SELECT 'Select rows ordered by value descending (id > 2)';
SELECT id, name, value FROM test_tiered_multi_segment WHERE id > 2 ORDER BY value DESC;
SELECT 'Limit results ordered by id';
SELECT * FROM test_tiered_multi_segment ORDER BY id LIMIT 3;
SELECT 'Explain plan for filter on value';
EXPLAIN SELECT * FROM test_tiered_multi_segment WHERE value > 150 SETTINGS prefer_localhost_replica=0, enable_analyzer=0;
EXPLAIN SELECT * FROM test_tiered_multi_segment WHERE value > 150 SETTINGS prefer_localhost_replica=0, enable_analyzer=1;
EXPLAIN SELECT * FROM test_tiered_multi_segment WHERE value > 150 SETTINGS prefer_localhost_replica=1, enable_analyzer=0;
EXPLAIN SELECT * FROM test_tiered_multi_segment WHERE value > 150 SETTINGS prefer_localhost_replica=1, enable_analyzer=1;

SELECT 'Aggregate values across name when filtering by event_time';
SELECT
    name,
    count() AS count,
    avg(value) AS avg_value
FROM test_tiered_multi_segment
WHERE event_time >= '2022-01-02'
GROUP BY name
ORDER BY avg_value DESC;

SELECT 'Verify additional_table_filters works consistently (legacy analyser)';
SELECT id, name, value
FROM test_tiered_multi_segment
WHERE id < 3
ORDER BY id
SETTINGS additional_table_filters = {'test_tiered_multi_segment' : 'id > 1'}, allow_experimental_analyzer = 0;

SELECT 'Verify additional_table_filters works consistently (new analyser)';
SELECT id, name, value
FROM test_tiered_multi_segment
WHERE id < 3
ORDER BY id
SETTINGS additional_table_filters = {'test_tiered_multi_segment' : 'id > 1'}, allow_experimental_analyzer = 1;


SELECT 'Clean up Hybrid table with three segment pairs';
DROP TABLE IF EXISTS test_tiered_multi_segment SYNC;
SELECT 'Clean up local helper table';
DROP TABLE IF EXISTS test_tiered_local_data SYNC;

---------------------------------

-- Test Hybrid engine predicate filtering functionality

SELECT 'Drop predicate filtering fixtures if they exist';
DROP TABLE IF EXISTS test_tiered_watermark_after SYNC;
DROP TABLE IF EXISTS test_tiered_watermark_before SYNC;
DROP TABLE IF EXISTS test_tiered_watermark SYNC;

SELECT 'Create local tables representing before/after watermark partitions';
CREATE TABLE test_tiered_watermark_after
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt64
)
ENGINE = MergeTree()
ORDER BY id;

SELECT 'Create second local table with different value type';
CREATE TABLE test_tiered_watermark_before
(
    `id` Int32,
    `name` Nullable(String),
    `date` Date,
    `value` Decimal128(0)
)
ENGINE = MergeTree()
ORDER BY id;

SELECT 'Insert rows before watermark into both tables';
INSERT INTO test_tiered_watermark_after VALUES
    (11, 'Alice', '2025-08-15', 100),
    (12, 'Bob', '2025-08-20', 200),
    (13, 'Charlie', '2025-08-25', 300);
INSERT INTO test_tiered_watermark_before VALUES
    (21, 'Alice', '2025-08-15', 100),
    (22, 'Bob', '2025-08-20', 200),
    (23, 'Charlie', '2025-08-25', 300);

SELECT 'Insert rows after watermark into both tables';
INSERT INTO test_tiered_watermark_after VALUES
    (14, 'David', '2025-09-05', 400),
    (15, 'Eve', '2025-09-10', 500),
    (16, 'Frank', '2025-09-15', 600);
INSERT INTO test_tiered_watermark_before VALUES
    (24, 'David', '2025-09-05', 400),
    (25, 'Eve', '2025-09-10', 500),
    (26, 'Frank', '2025-09-15', 600);


SELECT 'Create Hybrid table with analyzer disabled during reads';
CREATE TABLE test_tiered_watermark
(
    `id` UInt32,
    `name` String,
    `date` Date,
    `value` UInt32
)
ENGINE = Hybrid(
    remote('127.0.0.1:9000', currentDatabase(), 'test_tiered_watermark_after'),
    date >= '2025-09-01',
    remote('127.0.0.1:9000', currentDatabase(), 'test_tiered_watermark_before'),
    date < '2025-09-01'
);

SELECT 'Insert row via Hybrid table (should go to first segment)';
INSERT INTO test_tiered_watermark SETTINGS distributed_foreground_insert = 1
VALUES (17, 'John', '2025-09-25', 400);

SELECT 'Verify that inserted row landed in first table';
SELECT * FROM test_tiered_watermark_after WHERE id = 17 ORDER BY id;
SELECT 'Verify that second table did not receive the inserted row';
SELECT count() FROM test_tiered_watermark_before WHERE id = 17;


SELECT 'Read predicate-filtered data with analyzer disabled and no localhost preference';
SELECT * FROM test_tiered_watermark ORDER BY id SETTINGS enable_analyzer = 0, prefer_localhost_replica = 0;
SELECT 'Read predicate-filtered data with analyzer enabled and no localhost preference';
SELECT * FROM test_tiered_watermark ORDER BY id SETTINGS enable_analyzer = 1, prefer_localhost_replica = 0;
SELECT 'Read predicate-filtered data with analyzer disabled and prefer localhost replica';
SELECT * FROM test_tiered_watermark ORDER BY id SETTINGS enable_analyzer = 0, prefer_localhost_replica = 1;
SELECT 'Read predicate-filtered data with analyzer enabled and prefer localhost replica';
SELECT * FROM test_tiered_watermark ORDER BY id SETTINGS enable_analyzer = 1, prefer_localhost_replica = 1;

-- other combinations of settings work, but give a bit different content in the query_log
-- See the problem around is_initial_query described in https://github.com/Altinity/ClickHouse/issues/1077
SELECT 'Check if the subqueries were recorded in query_log (hybrid_table_auto_cast_columns = 0)';

SELECT * FROM test_tiered_watermark ORDER BY id DESC SETTINGS enable_analyzer = 1,  hybrid_table_auto_cast_columns = 0, prefer_localhost_replica = 0, log_queries=1, serialize_query_plan=0, log_comment = 'test_tiered_watermark1', max_threads=1 FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT
    type,
    query_id = initial_query_id AS is_initial_query2,
    arraySort(arrayMap(x -> replaceAll(x, currentDatabase(), 'db'), tables)) as tbl,
    replaceAll(query, currentDatabase(), 'db') as qry,
    log_comment
FROM system.query_log
WHERE
 event_time > now() - 300 AND type = 'QueryFinish' AND
 initial_query_id IN (
    SELECT initial_query_id
    FROM system.query_log
    WHERE
        event_time > now() - 300
        and log_comment = 'test_tiered_watermark1'
        and current_database = currentDatabase()
        and query_id = initial_query_id )
ORDER BY tbl, event_time_microseconds
FORMAT Vertical;

SELECT 'Check if the subqueries were recorded in query_log (hybrid_table_auto_cast_columns = 1)';

SELECT * FROM test_tiered_watermark ORDER BY id DESC SETTINGS enable_analyzer = 1, hybrid_table_auto_cast_columns = 1, prefer_localhost_replica = 0, log_queries=1, serialize_query_plan=0, log_comment = 'test_tiered_watermark2', max_threads=1 FORMAT Null;
SYSTEM FLUSH LOGS;
SELECT
    type,
    query_id = initial_query_id AS is_initial_query2,
    arraySort(arrayMap(x -> replaceAll(x, currentDatabase(), 'db'), tables)) as tbl,
    replaceAll(query, currentDatabase(), 'db') as qry,
    log_comment
FROM system.query_log
WHERE
 event_time > now() - 300 AND type = 'QueryFinish' AND
 initial_query_id IN (
    SELECT initial_query_id
    FROM system.query_log
    WHERE
        event_time > now() - 300
        and log_comment = 'test_tiered_watermark2'
        and current_database = currentDatabase()
        and query_id = initial_query_id )
ORDER BY tbl, event_time_microseconds
FORMAT Vertical;


SELECT 'Clean up predicate filtering tables';
DROP TABLE IF EXISTS test_tiered_watermark SYNC;
DROP TABLE IF EXISTS test_tiered_watermark_after SYNC;
DROP TABLE IF EXISTS test_tiered_watermark_before SYNC;

-- TODO: - addressed by 03644_hybrid_auto_cast.sql
-- Code: 70. DB::Exception: Received from localhost:9000. DB::Exception: Conversion from AggregateFunction(sum, Decimal(38, 0)) to AggregateFunction(sum, UInt32) is not supported: while converting source column `sum(__table1.value)` to destination column `sum(__table1.value)`. (CANNOT_CONVERT_TYPE)
-- SELECT sum(value) FROM test_tiered_watermark;

-- TODO:
-- Code: 47. DB::Exception: Received from localhost:9000. DB::Exception: Received from 127.0.0.2:9000. DB::Exception: Identifier '__table1._database' cannot be resolved from table with name __table1. In scope SELECT __table1._database AS _database, __table1._table AS row_count FROM default.test_tiered_watermark_after AS __table1 WHERE __table1.date >= '2025-09-01'. Maybe you meant: ['__table1._table']. (UNKNOWN_IDENTIFIER)
-- SELECT _database, _table, count() AS row_count FROM test_tiered_watermark GROUP BY _database, _table ORDER BY _database, _table;

-- Other things which may need attention:
-- complex combinations? (overview / over Merge)
-- prefer_localhost_replica
-- threads versus local subquery pipeline part
-- ALTER support

-- TODO
-- SELECT _table_index, count() AS row_count FROM test_debug_tiered GROUP BY _table_index ORDER BY _table_index;

-- TODO
--   1. Integration tests (similar to tests/queries/0_stateless)
--   - Base SELECT with date split: part in Distributed, part in S3 -> results should match a manual UNION ALL (with correct ORDER BY/aggregation).
--   - GROUP BY / ORDER BY / LIMIT: confirm the stage is selected correctly, finalization happens at the top, rows_before_limit_at_least is correct (createLocalPlan already keeps LIMIT).
--   - JOIN: with a small table on the initiator; check GLOBAL JOIN scenarios. Ensure remote segments behave the same as remote shard subqueries created through createLocalPlan.
--   - skipUnusedShards: with analyzer ensure segment conditions are respected (where FILTER DAG is available).
--   - Constants: hostName()/now() in SELECT across several segments -> ensure no discrepancies.
--   - EXPLAIN PLAN/PIPELINE: show child plans for segments and remote plans.
--   - Subqueries in logs.
-- - Different column sets/types: supertype in snapshot, converting actions on read.
--   - Object columns: same as Distributed — use ColumnsDescriptionByShardNum for segments if needed (optional for local segments; already implemented for Distributed).

-- Condition with dictGet('a1_watermarks_dict', ...)

-- access rights check


-- TODO:
-- test for distributed_aggregation_memory_efficient & enable_memory_bound_merging_of_aggregation_results
-- to avoid UNKNOWN_AGGREGATED_DATA_VARIANT when mixing different aggregation variants
-- from remote shards (with memory_bound) and local segments (without memory_bound)
