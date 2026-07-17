-- Single-column partition/sorting key stored as `a` (older versions) vs `tuple(a)` (newer versions)
-- must be treated as equal by REPLACE PARTITION. Regression for the backward-incompatible reject
-- introduced when key comparison used the raw definition AST instead of the normalized key expression.

DROP TABLE IF EXISTS t_bare;
DROP TABLE IF EXISTS t_tuple;

CREATE TABLE t_bare (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY a ORDER BY v;
CREATE TABLE t_tuple (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY tuple(a) ORDER BY v;

INSERT INTO t_tuple VALUES (1, 10), (1, 11);
ALTER TABLE t_bare REPLACE PARTITION 1 FROM t_tuple;
SELECT 'partition a<-tuple(a)', count() FROM t_bare;

DROP TABLE t_bare;
DROP TABLE t_tuple;

-- Same normalization for the sorting key.
CREATE TABLE t_bare (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY tuple() ORDER BY a;
CREATE TABLE t_tuple (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY tuple() ORDER BY tuple(a);

INSERT INTO t_tuple VALUES (1, 10);
ALTER TABLE t_bare REPLACE PARTITION tuple() FROM t_tuple;
SELECT 'ordering a<-tuple(a)', count() FROM t_bare;

DROP TABLE t_bare;
DROP TABLE t_tuple;

-- Genuinely different keys are still rejected.
CREATE TABLE t_bare (a UInt32, b UInt32, v UInt32) ENGINE = MergeTree PARTITION BY a ORDER BY v;
CREATE TABLE t_other (a UInt32, b UInt32, v UInt32) ENGINE = MergeTree PARTITION BY b ORDER BY v;
INSERT INTO t_other VALUES (1, 2, 10);
ALTER TABLE t_bare REPLACE PARTITION 2 FROM t_other; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_bare;
DROP TABLE t_other;
