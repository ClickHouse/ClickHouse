-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- With distributed_plan_execute_locally = 1 the initiator pipeline reads the result from an
-- in-memory exchange. If that reader blocked the pipeline thread instead of yielding, the source
-- that starts the producing tasks could never run and the query hung. Whether that happens depends
-- on processor scheduling order, so the queries are repeated several times.

DROP TABLE IF EXISTS m_dlk;
DROP TABLE IF EXISTS d_dlk_1;
DROP TABLE IF EXISTS d_dlk_2;
DROP TABLE IF EXISTS base_dlk_1;
DROP TABLE IF EXISTS base_dlk_2;

CREATE TABLE base_dlk_1 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
CREATE TABLE base_dlk_2 (key Int, value Int) ENGINE = MergeTree ORDER BY key;
INSERT INTO base_dlk_1 SELECT number, number FROM numbers(10000);
INSERT INTO base_dlk_2 SELECT number, number FROM numbers(5000);

CREATE TABLE d_dlk_1 AS base_dlk_1 ENGINE = Distributed(test_shard_localhost, currentDatabase(), base_dlk_1);
CREATE TABLE d_dlk_2 AS base_dlk_2 ENGINE = Distributed(test_shard_localhost, currentDatabase(), base_dlk_2);
CREATE TABLE m_dlk ENGINE = Merge(currentDatabase(), '^d_dlk_(1|2)$');

-- max_rows_to_group_by must be 0: make_distributed_plan rejects aggregation with a non-zero limit.
-- prefer_localhost_replica must be 1: with 0 the Distributed child ships its plan to the localhost
-- replica over the classic protocol, which cannot deserialize distributed-plan steps.
-- distributed_plan_max_rows_to_broadcast = 0 forces shuffle aggregation and bucketed reads, so the
-- child plans deterministically contain exchanges.
-- The bucket counts are the number of tasks per stage, and each task runs on its own thread with its
-- own pipeline. Keep them at the minimum that still produces exchanges: at the default of 8 a single
-- query starts 34 tasks (the `Merge` table plans each underlying table separately), which takes tens
-- of seconds on a loaded machine.
SET make_distributed_plan = 1, enable_parallel_replicas = 0, distributed_plan_execute_locally = 1,
    max_rows_to_group_by = 0, prefer_localhost_replica = 1, distributed_plan_max_rows_to_broadcast = 0,
    distributed_plan_default_shuffle_join_bucket_count = 2, distributed_plan_default_reader_bucket_count = 2,
    max_threads = 1, distributed_aggregation_memory_efficient = 0;

SELECT count(_table) FROM m_dlk WHERE _table = 'base_dlk_1' GROUP BY _table;
SELECT count(_table) FROM m_dlk WHERE _table = 'base_dlk_2' GROUP BY _table;
SELECT count(_table) FROM m_dlk WHERE _table = 'base_dlk_1' GROUP BY _table;
SELECT count(_table) FROM m_dlk WHERE _table = 'base_dlk_2' GROUP BY _table;

DROP TABLE m_dlk;
DROP TABLE d_dlk_1;
DROP TABLE d_dlk_2;
DROP TABLE base_dlk_1;
DROP TABLE base_dlk_2;
