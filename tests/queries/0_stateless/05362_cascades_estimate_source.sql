-- A Cascades plan reports where each estimate comes from, as the other planners do: `statistics`
-- for a table with column statistics, `primary_index` for one without them, `hint` for a table in
-- the stat hints. Without this every Cascades step showed a bare row count. Only the reads are
-- checked: the rest of the plan varies with settings the runner randomizes.
SET allow_experimental_statistics = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET param__internal_join_table_stat_hints = '{"t_src_hinted": {"cardinality": 500}}';

DROP TABLE IF EXISTS t_src_fact;
DROP TABLE IF EXISTS t_src_plain;
DROP TABLE IF EXISTS t_src_hinted;

CREATE TABLE t_src_fact (k UInt64 STATISTICS(uniq, basic), v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = '';
CREATE TABLE t_src_plain (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_src_hinted (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_src_fact SELECT number % 100, number FROM numbers(10000);
INSERT INTO t_src_plain SELECT number FROM numbers(100);
INSERT INTO t_src_hinted SELECT number FROM numbers(100);

SELECT DISTINCT regexpExtract(explain, 'ReadFromMergeTree \\((?:[A-Za-z]+ )?([^)]*)\\)', 1) AS table, regexpExtract(explain, 'source: ([a-z_]+)', 1) AS source
FROM (
    EXPLAIN estimates = 1
    SELECT count() FROM t_src_fact AS f JOIN t_src_plain AS p ON f.k = p.k JOIN t_src_hinted AS h ON f.k = h.k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1
)
WHERE explain LIKE '%ReadFromMergeTree%'
ORDER BY table;

DROP TABLE t_src_fact;
DROP TABLE t_src_plain;
DROP TABLE t_src_hinted;
