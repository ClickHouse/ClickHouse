-- `CREATE TABLE ... AS src` copies the source column expressions verbatim, and the legacy `toTime`
-- rewrite runs on the query afterwards. The live table must be built from the rewritten text, so the
-- same insert has to produce the same value before and after a reload.

SET allow_experimental_time_time64_type = 1;

DROP TABLE IF EXISTS t_copy_expr_src;
DROP TABLE IF EXISTS t_copy_expr_dst;

CREATE TABLE t_copy_expr_src (c0 DateTime('UTC'), mat UInt32 MATERIALIZED toTime(c0), def UInt32 DEFAULT toTime(c0), ali UInt32 ALIAS toTime(c0))
ENGINE = MergeTree ORDER BY c0;

SET use_legacy_to_time = 1;
CREATE TABLE t_copy_expr_dst AS t_copy_expr_src;
SET use_legacy_to_time = 0;

SELECT 'stored', name, default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 't_copy_expr_dst' AND default_expression != '' ORDER BY name;

INSERT INTO t_copy_expr_dst (c0) VALUES ('2020-01-02 03:04:05');
SELECT 'before_reload', mat, def, ali FROM t_copy_expr_dst;

DETACH TABLE t_copy_expr_dst;
ATTACH TABLE t_copy_expr_dst;

INSERT INTO t_copy_expr_dst (c0) VALUES ('2020-01-02 03:04:05');
SELECT 'after_reload', mat, def, ali, count() FROM t_copy_expr_dst GROUP BY mat, def, ali ORDER BY mat;

DROP TABLE t_copy_expr_dst;
DROP TABLE t_copy_expr_src;
