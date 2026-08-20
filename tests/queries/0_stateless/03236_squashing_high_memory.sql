-- Tags: no-fasttest, no-asan, no-tsan, no-msan, no-ubsan, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-flaky-check
-- reason: test requires too many rows to read
-- no-flaky-check: a single run takes about 12 seconds, but the repeated concurrent runs of the
-- flaky check contend for the same machine and push it over the 180 seconds per-test limit
-- reason for no-replicated-database: every replica re-executes the populating SELECT, and the
-- resulting minutes-long DDL entry blocks the single-threaded global DDLWorker loop, so concurrent
-- tests exceed distributed_ddl_task_timeout while creating or dropping their databases

SET max_rows_to_read = '501G';
SET enable_lazy_columns_replication = 0;

DROP TABLE IF EXISTS id_values;

DROP TABLE IF EXISTS test_table;

CREATE TABLE id_values ENGINE MergeTree ORDER BY id1 AS
    SELECT arrayJoin(range(500000)) AS id1, arrayJoin(range(1000)) AS id2;

SET max_memory_usage = '1G';
SET query_plan_join_swap_table = 'false';

CREATE TABLE test_table ENGINE MergeTree ORDER BY id AS
SELECT id_values.id1             AS id,
    string_values.string_val1 AS string_val1,
    string_values.string_val2 AS string_val2
FROM id_values
        JOIN (SELECT arrayJoin(range(10)) AS id1,
                    'qwe'                AS string_val1,
                    'asd'                AS string_val2) AS string_values
            ON id_values.id1 = string_values.id1
    SETTINGS join_algorithm = 'hash';

DROP TABLE IF EXISTS id_values;
DROP TABLE IF EXISTS test_table;
