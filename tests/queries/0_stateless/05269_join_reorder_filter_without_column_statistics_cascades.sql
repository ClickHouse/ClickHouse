-- The experimental Cascades optimizer estimates a relation whose PREWHERE reads only columns without statistics as without statistics.

SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'auto';
SET use_hash_table_stats_for_join_reordering = 0;
SET join_algorithm = 'hash';
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP TABLE IF EXISTS t3;

CREATE TABLE t1 (r String, j JSON(k Array(String), sd Bool, ui Bool, rs Array(String), `a.t` String, `a.n` String, `a.ty` String, tid String) MATERIALIZED r, Id String, INDEX i1 j.k TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY (j.sd, j.ui, j.a.ty, j.tid, Id) SETTINGS auto_statistics_types = 'minmax, uniq';
CREATE TABLE t2 (r String, j JSON(dn String, f1 Nullable(DateTime64(3)), f2 Nullable(DateTime64(3)), s Array(String), tid String, k Array(String)) MATERIALIZED r, Id String, INDEX i2 j.k TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY (j.dn, Id) SETTINGS auto_statistics_types = 'minmax, uniq';
CREATE TABLE t3 (r String, j JSON(k Array(String)) MATERIALIZED r, Id String, n Nullable(UInt8), INDEX i3 j.k TYPE set(100) GRANULARITY 1)
ENGINE = ReplacingMergeTree ORDER BY Id SETTINGS auto_statistics_types = 'minmax, uniq';

INSERT INTO t1 (r, Id) SELECT concat('{"k":', if(number % 20 = 0, '["A","B"]', '["B"]'), ',"sd":false,"ui":true,"rs":["s1"],"a":{"t":"x', toString(number % 7), '","n":"n', toString(number % 1000), '","ty":"', if(number % 20 = 0, 'y0', 'y1'), '"},"tid":"z"}'), concat('e', toString(number)) FROM numbers(3000);
INSERT INTO t2 (r, Id) SELECT concat('{"eid":"e', toString((number % 30) * 20), '","k":["C"],"s":["s1"],"st":"o","vid":"v', toString(number % 1500), '","f1":"2024-01-01 00:00:00","rv":["1.', toString(number % 9), '"],"f2":null,"dn":"d', toString(number % 50), '","tid":"z"}'), concat('p', toString(number)) FROM numbers(30000);
INSERT INTO t3 (r, Id, n) SELECT concat('{"k":["D"],"w":{"sev":"', ['l','m','h','c'][number % 4 + 1], '","s2":', toString((number % 100) / 10), ',"s3":', toString((number % 90) / 10), '},"sv":"h"}'), concat('v', toString(number)), if(number % 10 = 0, NULL, 1) FROM numbers(1500);

-- Statistics are materialized explicitly, whatever `materialize_statistics_on_insert` is.
ALTER TABLE t1 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;
ALTER TABLE t2 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;
ALTER TABLE t3 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 2;

-- The plan is built by the Cascades optimizer: its join steps carry a physical annotation.
SELECT count() > 0 FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT x0.Id, x1.Id, x2.Id, x3.Id
    FROM t1 AS x0
    INNER JOIN t2 AS x1 ON (CAST(x1.j.eid, 'Nullable(String)') = x0.Id) AND has(CAST(x1.j.k, 'Array(String)'), 'C') AND hasAny(x1.j.s, ['s0', 'all', 's1']) AND (x1.j.st = 'o')
    INNER JOIN t3 AS x2 ON (CAST(x1.j.vid, 'Nullable(String)') = x2.Id) AND has(CAST(x2.j.k, 'Array(String)'), 'D') AND has(['m', 'h', 'c'], x2.j.w.sev)
    INNER JOIN t1 AS x3 ON (CAST(x1.j.eid, 'Nullable(String)') = x3.Id) AND has(CAST(x3.j.k, 'Array(String)'), 'B') AND (x3.j.sd = false) AND (x3.j.ui = true) AND hasAny(x3.j.rs, ['s0', 'all', 's1'])
    WHERE has(CAST(x0.j.k, 'Array(String)'), 'A') AND (x0.j.sd = false) AND (x0.j.ui = true) AND hasAny(x0.j.rs, ['s0', 's1', 'all'])
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1, enable_join_runtime_filters = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1, use_statistics = 1
)
WHERE explain LIKE '%JoinLogical (%';

-- The join order is the same with and without statistics.
SELECT
(
    SELECT arraySort(groupArray(replaceRegexpAll(trimLeft(explain), '\\[[^\\]]*\\]', ''))) FROM
    (
        EXPLAIN keep_logical_steps = 1, actions = 1
        SELECT x0.Id, x1.Id, x2.Id, x3.Id
        FROM t1 AS x0
        INNER JOIN t2 AS x1 ON (CAST(x1.j.eid, 'Nullable(String)') = x0.Id) AND has(CAST(x1.j.k, 'Array(String)'), 'C') AND hasAny(x1.j.s, ['s0', 'all', 's1']) AND (x1.j.st = 'o')
        INNER JOIN t3 AS x2 ON (CAST(x1.j.vid, 'Nullable(String)') = x2.Id) AND has(CAST(x2.j.k, 'Array(String)'), 'D') AND has(['m', 'h', 'c'], x2.j.w.sev)
        INNER JOIN t1 AS x3 ON (CAST(x1.j.eid, 'Nullable(String)') = x3.Id) AND has(CAST(x3.j.k, 'Array(String)'), 'B') AND (x3.j.sd = false) AND (x3.j.ui = true) AND hasAny(x3.j.rs, ['s0', 'all', 's1'])
        WHERE has(CAST(x0.j.k, 'Array(String)'), 'A') AND (x0.j.sd = false) AND (x0.j.ui = true) AND hasAny(x0.j.rs, ['s0', 's1', 'all'])
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1, enable_join_runtime_filters = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1, use_statistics = 1
    )
    WHERE explain LIKE '%Join:%'
)
=
(
    SELECT arraySort(groupArray(replaceRegexpAll(trimLeft(explain), '\\[[^\\]]*\\]', ''))) FROM
    (
        EXPLAIN keep_logical_steps = 1, actions = 1
        SELECT x0.Id, x1.Id, x2.Id, x3.Id
        FROM t1 AS x0
        INNER JOIN t2 AS x1 ON (CAST(x1.j.eid, 'Nullable(String)') = x0.Id) AND has(CAST(x1.j.k, 'Array(String)'), 'C') AND hasAny(x1.j.s, ['s0', 'all', 's1']) AND (x1.j.st = 'o')
        INNER JOIN t3 AS x2 ON (CAST(x1.j.vid, 'Nullable(String)') = x2.Id) AND has(CAST(x2.j.k, 'Array(String)'), 'D') AND has(['m', 'h', 'c'], x2.j.w.sev)
        INNER JOIN t1 AS x3 ON (CAST(x1.j.eid, 'Nullable(String)') = x3.Id) AND has(CAST(x3.j.k, 'Array(String)'), 'B') AND (x3.j.sd = false) AND (x3.j.ui = true) AND hasAny(x3.j.rs, ['s0', 'all', 's1'])
        WHERE has(CAST(x0.j.k, 'Array(String)'), 'A') AND (x0.j.sd = false) AND (x0.j.ui = true) AND hasAny(x0.j.rs, ['s0', 's1', 'all'])
        SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1, enable_join_runtime_filters = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, move_all_conditions_to_prewhere = 1, use_statistics = 0
    )
    WHERE explain LIKE '%Join:%'
);

DROP TABLE t1;
DROP TABLE t2;
DROP TABLE t3;
