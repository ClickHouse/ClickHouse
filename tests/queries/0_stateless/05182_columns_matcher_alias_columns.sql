-- A `COLUMNS` matcher matches `ALIAS` and `MATERIALIZED` columns when they are enabled with
-- `asterisk_include_alias_columns` / `asterisk_include_materialized_columns`, the same way as `*`.
-- A table whose interface consists of alias columns (such as the `bucketed` schema of
-- `system.metric_log`) relies on it: otherwise, for example, `arraySum([COLUMNS('...')])` has an
-- empty array to sum and fails with `ILLEGAL_TYPE_OF_ARGUMENT`.

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

-- Both planners give the same result when the settings are enabled.
SELECT arraySum([COLUMNS('^metric_') APPLY sum]) FROM t_columns_matcher SETTINGS enable_analyzer = 0;
SELECT arraySum([COLUMNS('^metric_') APPLY sum]) FROM t_columns_matcher SETTINGS enable_analyzer = 1;
SELECT arraySum([t.COLUMNS('^metric_') APPLY sum]) FROM t_columns_matcher AS t SETTINGS enable_analyzer = 0;
SELECT arraySum([t.COLUMNS('^metric_') APPLY sum]) FROM t_columns_matcher AS t SETTINGS enable_analyzer = 1;

-- A qualified asterisk also includes the enabled `ALIAS` and `MATERIALIZED` columns, with both planners.
SELECT t.* FROM t_columns_matcher AS t FORMAT TSVWithNames SETTINGS enable_analyzer = 0;
SELECT t.* FROM t_columns_matcher AS t FORMAT TSVWithNames SETTINGS enable_analyzer = 1;

DROP TABLE t_columns_matcher;
