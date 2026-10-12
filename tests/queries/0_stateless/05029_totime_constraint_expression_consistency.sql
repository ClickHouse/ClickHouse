-- Most engines take the constraints computed before the legacy `toTime` rewrite verbatim, while the
-- metadata written to disk comes from the rewritten query. The constraint must therefore be enforced
-- with the same spelling before and after a reload: the same row cannot be accepted now and rejected
-- after a restart.

SET allow_experimental_time_time64_type = 1;

DROP TABLE IF EXISTS t_constraint_expr;

SET use_legacy_to_time = 1;
CREATE TABLE t_constraint_expr (c0 DateTime('UTC'), CONSTRAINT c CHECK toUInt32(toTime(c0)) < 50000) ENGINE = Log;
SET use_legacy_to_time = 0;

SELECT 'stored', extract(create_table_query, 'toTime\\w*') FROM system.tables
WHERE database = currentDatabase() AND name = 't_constraint_expr';

-- `toTime` gives 3600 for this timestamp and `toTimeWithFixedDate` gives 90000, so the threshold
-- separates the two spellings.
INSERT INTO t_constraint_expr VALUES ('2020-01-02 01:00:00'); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'before_reload', count() FROM t_constraint_expr;

DETACH TABLE t_constraint_expr;
ATTACH TABLE t_constraint_expr;

INSERT INTO t_constraint_expr VALUES ('2020-01-02 01:00:00'); -- { serverError VIOLATED_CONSTRAINT }
SELECT 'after_reload', count() FROM t_constraint_expr;

DROP TABLE t_constraint_expr;
