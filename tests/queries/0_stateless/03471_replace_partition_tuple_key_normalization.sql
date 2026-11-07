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

-- Reverse sorting key: `ORDER BY a` and `ORDER BY a DESC` describe different layouts and must stay
-- rejected. The normalization only unwraps tuple(...); it must not drop the sort direction.
CREATE TABLE t_asc (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY tuple() ORDER BY a;
CREATE TABLE t_desc (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY tuple() ORDER BY a DESC;
INSERT INTO t_desc VALUES (1, 10);
ALTER TABLE t_asc REPLACE PARTITION tuple() FROM t_desc; -- { serverError BAD_ARGUMENTS }
SELECT 'ordering asc<-desc rejected', count() FROM t_asc;

DROP TABLE t_asc;
DROP TABLE t_desc;

-- Redundant parentheses (`PARTITION BY (a)`) are the same key as `PARTITION BY a`. #92340 started
-- preserving them in stored metadata, breaking ATTACH/REPLACE PARTITION between a table created by
-- an older version (canonical `a`) and a newer one (`(a)`). All three keys must normalize equally.
CREATE TABLE t_plain (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY a PRIMARY KEY a ORDER BY (a, b) SAMPLE BY a;
CREATE TABLE t_paren (a UInt32, b UInt32) ENGINE = MergeTree PARTITION BY (a) PRIMARY KEY (a) ORDER BY (a, b) SAMPLE BY (a);
INSERT INTO t_paren VALUES (1, 1), (1, 2), (2, 1);
ALTER TABLE t_plain ATTACH PARTITION 1 FROM t_paren;
SELECT 'partition/primary a<-(a)', count() FROM t_plain;

DROP TABLE t_plain;
DROP TABLE t_paren;

-- Same for a parenthesized single-element sorting key.
CREATE TABLE t_plain (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY a ORDER BY a;
CREATE TABLE t_paren (a UInt32, v UInt32) ENGINE = MergeTree PARTITION BY (a) ORDER BY (a);
INSERT INTO t_paren VALUES (1, 10), (1, 11);
ALTER TABLE t_plain ATTACH PARTITION 1 FROM t_paren;
SELECT 'ordering a<-(a)', count() FROM t_plain;

DROP TABLE t_plain;
DROP TABLE t_paren;
