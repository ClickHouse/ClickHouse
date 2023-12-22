# There was a wrong, harmful feature, leading to bugs and data corruption.
# This feature is removed, but we take care to maintain compatibility on the syntax level, so now it works as a no-op.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8, PRIMARY KEY x) ENGINE = ReplacingMergeTree;
OPTIMIZE TABLE t CLEANUP;
DROP TABLE t;
