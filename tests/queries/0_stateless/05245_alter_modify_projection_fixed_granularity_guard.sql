-- Tags: no-replicated-database
-- Tag no-replicated-database: `ALTER`s of replicated and non-replicated types cannot be mixed in one query
-- A single `ALTER` may both add a projection `index_granularity` override and switch the table to fixed
-- granularity, and must be rejected up front. Split out of 04757_alter_modify_projection_settings.sql,
-- whose other cases keep running on `Replicated` databases.

DROP TABLE IF EXISTS t_modify_projection;

CREATE TABLE t_modify_projection
(
    k UInt64,
    v UInt64,
    PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 1024)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

ALTER TABLE t_modify_projection MODIFY PROJECTION p (SELECT v ORDER BY v) WITH SETTINGS (index_granularity = 256), MODIFY SETTING index_granularity_bytes = 0; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE t_modify_projection;
