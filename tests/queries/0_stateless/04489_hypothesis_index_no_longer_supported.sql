-- Issue #89797: the `hypothesis` skip index type was removed, so it can no longer
-- be created or used. Creating or adding one must throw ILLEGAL_INDEX.

-- The exact table from the issue can no longer be created.
SET allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t0 (c0 LowCardinality(UInt128), INDEX i0 90 % c0 TYPE hypothesis, c1 Bool, c2 Map(Int64, Int64)) ENGINE = MergeTree() PRIMARY KEY tuple(); -- { serverError ILLEGAL_INDEX }

-- A minimal hypothesis index is rejected too, independent of column type.
CREATE TABLE t_create (a UInt64, INDEX i0 90 % a TYPE hypothesis) ENGINE = MergeTree() PRIMARY KEY tuple(); -- { serverError ILLEGAL_INDEX }

-- ALTER ADD INDEX and CREATE INDEX are rejected as well.
CREATE TABLE t_alter (a UInt64) ENGINE = MergeTree() PRIMARY KEY tuple();
ALTER TABLE t_alter ADD INDEX i0 90 % a TYPE hypothesis; -- { serverError ILLEGAL_INDEX }
CREATE INDEX i0 ON t_alter (90 % a) TYPE hypothesis; -- { serverError ILLEGAL_INDEX }

DROP TABLE t_alter;
