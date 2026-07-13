-- Regression test for a LOGICAL_ERROR ("Arguments of 'minus'/'plus' have incorrect data types")
-- during primary-key index analysis when a monotonic arithmetic function is applied over a
-- LowCardinality key column exposed through a Merge table whose header declares the plain type.
-- KeyCondition::applyFunction executed the monotonic function on the raw LowCardinality index
-- column while the function was resolved against the plain key type, aborting in debug/sanitizer
-- builds. The function must be applied to the full (non-LowCardinality) representation, as the
-- sibling applyFunctionChainToColumn already does.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_04350_lc;
DROP TABLE IF EXISTS t_04350_plain;
DROP TABLE IF EXISTS t_04350_merge;

CREATE TABLE t_04350_lc (k LowCardinality(UInt32), v String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_04350_plain (k UInt32, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04350_lc SELECT number, toString(number) FROM numbers(100000);
INSERT INTO t_04350_plain SELECT number, toString(number) FROM numbers(100000);

-- Merge header declares the plain type; sources mix LowCardinality and plain key columns.
CREATE TABLE t_04350_merge (k UInt32, v String)
    ENGINE = Merge(currentDatabase(), 't_04350_lc|t_04350_plain');

-- minus over the LowCardinality key inside KeyCondition (previously aborted).
SELECT count() FROM t_04350_merge WHERE 3 = minus(materialize(materialize(65536)), k);
-- plus over the key.
SELECT count() FROM t_04350_merge WHERE 100003 = (k + materialize(3));
-- FINAL path.
SELECT DISTINCT count() FROM t_04350_merge FINAL WHERE (3 = minus(materialize(materialize(65536)), k)) AND notEmpty(v);

-- Correctness: PK pruning over the LowCardinality key must match the plain result.
SELECT count() FROM t_04350_merge WHERE (65536 - k) BETWEEN 3 AND 5003;

DROP TABLE t_04350_lc;
DROP TABLE t_04350_plain;
DROP TABLE t_04350_merge;
