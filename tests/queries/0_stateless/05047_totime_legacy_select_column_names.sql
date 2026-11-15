-- The legacy `toTime` rewrite must not change the automatic output column names of a persisted
-- SELECT: materialized views and views match columns by name against the stored column list.

DROP TABLE IF EXISTS mv_totime_names;
DROP TABLE IF EXISTS v_totime_names;
DROP TABLE IF EXISTS t_totime_names_src;

SET use_legacy_to_time = 1;

CREATE TABLE t_totime_names_src (c0 DateTime('UTC')) ENGINE = MergeTree ORDER BY tuple();

-- An unaliased expression keeps its automatic name through an alias in the rewritten text.
CREATE MATERIALIZED VIEW mv_totime_names ENGINE = MergeTree ORDER BY tuple() AS SELECT toTime(c0) FROM t_totime_names_src;
INSERT INTO t_totime_names_src VALUES ('2020-01-02 03:04:05');
SELECT 'mv', * FROM mv_totime_names;

CREATE VIEW v_totime_names AS SELECT toTime(c0) FROM t_totime_names_src;
SELECT 'view', * FROM v_totime_names;

-- The rewritten replacement query must keep matching the inner table column by name.
ALTER TABLE mv_totime_names MODIFY QUERY SELECT toTime(c0) FROM t_totime_names_src;
INSERT INTO t_totime_names_src VALUES ('2020-01-02 03:04:06');
SELECT 'after_modify_query', * FROM mv_totime_names ORDER BY ALL;

-- The reloaded definitions resolve identically under the default setting.
DETACH TABLE mv_totime_names;
DETACH TABLE v_totime_names;
SET use_legacy_to_time = 0;
ATTACH TABLE mv_totime_names;
ATTACH TABLE v_totime_names;

INSERT INTO t_totime_names_src VALUES ('2020-01-02 03:04:07');
SELECT 'reloaded_mv', * FROM mv_totime_names ORDER BY ALL;
SELECT 'reloaded_view', * FROM v_totime_names ORDER BY ALL;

DROP TABLE mv_totime_names;
DROP TABLE v_totime_names;
DROP TABLE t_totime_names_src;
