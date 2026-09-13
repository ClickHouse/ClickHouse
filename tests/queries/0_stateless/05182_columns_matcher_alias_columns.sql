-- A `COLUMNS` matcher matches `ALIAS` and `MATERIALIZED` columns when they are enabled with
-- `asterisk_include_alias_columns` / `asterisk_include_materialized_columns`, the same way as `*`.
-- Without them, a table whose interface consists of alias columns (such as the `bucketed` schema
-- of `system.metric_log`) has nothing to match under the old analyzer, and, for example,
-- `arraySum([COLUMNS('...')])` fails with `ILLEGAL_TYPE_OF_ARGUMENT` on the empty array.
-- The new analyzer always matches them, so only the queries with both settings enabled have the
-- same result under both analyzers.

SET asterisk_include_alias_columns = 1, asterisk_include_materialized_columns = 1;

DROP TABLE IF EXISTS t_columns_matcher;

CREATE TABLE t_columns_matcher
(
    key UInt64,
    metrics Map(String, Int64),
    metric_ordinary Int64,
    metric_materialized Int64 MATERIALIZED key,
    metric_alias Int64 ALIAS metrics['metric_alias']
)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_columns_matcher (key, metrics, metric_ordinary) VALUES (1, {'metric_alias': 10}, 100);

-- 100 (ordinary) + 10 (alias) + 1 (materialized)
SELECT arraySum([COLUMNS('^metric_') APPLY sum]) FROM t_columns_matcher;

-- The same through a qualified matcher.
SELECT arraySum([t.COLUMNS('^metric_') APPLY sum]) FROM t_columns_matcher AS t;

-- All the three columns are matched: ordinary, materialized and alias.
SELECT length([COLUMNS('^metric_')]) FROM t_columns_matcher;

DROP TABLE t_columns_matcher;
