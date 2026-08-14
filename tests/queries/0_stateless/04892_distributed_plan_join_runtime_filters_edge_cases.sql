-- Tags: no-old-analyzer

CREATE TABLE tiny (tid UInt64) ENGINE = MergeTree ORDER BY tid;
CREATE TABLE mid (tid UInt64) ENGINE = MergeTree ORDER BY tid;
CREATE TABLE huge (hid UInt64, hid2 UInt64, s String) ENGINE = MergeTree ORDER BY hid;
CREATE TABLE big_nullable (bid Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE small_nullable (sid Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tiny SELECT number * 100 FROM numbers(100);
INSERT INTO mid SELECT number * 10 FROM numbers(10000);
INSERT INTO huge SELECT number, number, toString(number) FROM numbers(1000000);
INSERT INTO big_nullable SELECT number, number FROM numbers(100000);
INSERT INTO small_nullable SELECT number * 100 FROM numbers(100);

SET enable_analyzer = 1, enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_parallel_replicas = 0;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_max_rows_to_broadcast = 0;
SET explain_query_plan_default = 'legacy';
SET max_rows_to_group_by = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- Admission of transported filters depends on which relation ends up at each apply site, so pin
-- the join order and the estimate source against test-level randomization.
SET query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_limit = 10, use_hash_table_stats_for_join_reordering = 0, use_statistics = 0;
SET distributed_plan_join_runtime_filters = 1;

SELECT '-- tiny probe side, huge build side: the filter cannot arrive in time, rows pass unfiltered';
SELECT count() FROM tiny, huge WHERE tid = hid;

SELECT '-- Nullable key is not transportable, the local filter stays';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN SELECT count() FROM big_nullable, small_nullable WHERE bid = sid
) WHERE explain LIKE '%RuntimeFilter%';
SELECT count() FROM big_nullable, small_nullable WHERE bid = sid;

SELECT '-- persisted exchanges';
SELECT count() FROM huge, tiny WHERE hid = tid SETTINGS distributed_plan_force_exchange_kind = 'Persisted';

SELECT '-- streaming exchanges';
SELECT count() FROM huge, tiny WHERE hid = tid SETTINGS distributed_plan_force_exchange_kind = 'Streaming';

SELECT '-- early close under LIMIT';
SELECT count() FROM (SELECT hid FROM huge, tiny WHERE hid = tid LIMIT 10);

-- Independent join keys (hid vs hid2) keep the two dimension filters from collapsing into a
-- transitive `mid ⋈ tiny` bushy join. `mid` between `tiny` and `huge` keeps every admission
-- decision far from its estimate threshold, so randomized index-analysis jitter cannot flip the plan.
SELECT '-- two joins, each with its own filter';
SELECT REGEXP_REPLACE(trimLeft(explain), '_runtime_filter_\\d+', '_runtime_filter_UNIQ_ID') FROM (
    EXPLAIN SELECT count() FROM huge AS h INNER JOIN mid AS t1 ON h.hid = t1.tid INNER JOIN tiny AS t2 ON h.hid2 = t2.tid
) WHERE explain LIKE '%SendRuntimeFilter%' OR explain LIKE '%ReceiveRuntimeFilter%';
SELECT count() FROM huge AS h INNER JOIN mid AS t1 ON h.hid = t1.tid INNER JOIN tiny AS t2 ON h.hid2 = t2.tid;
