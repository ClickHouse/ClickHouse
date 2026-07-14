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

-- Second path to the same class of crash: the sparse primary-key analysis. When the key column is
-- NOT loaded in the in-memory index (dropped as a useless suffix) but is bounded by the part's
-- partition minmax, it is analysed as a constant coordinate whose type comes from the raw key type.
-- For the LowCardinality source that raw type is LowCardinality, but the monotonic function chain was
-- built against the stripped type, so the sparse KeyCondition::checkInHyperrectangle caller fed a
-- LowCardinality type into a chain built on the plain type. With a CAST wrapper (here the implicit
-- UInt8->Bool cast) the dictionary-unpack step is elided, and applyFunctionForField builds a
-- LowCardinality const column that the inner cast wrapper then rejects with a Bad cast LOGICAL_ERROR.
-- The sparse caller must strip LowCardinality like the dense one.
DROP TABLE IF EXISTS t_04350_lc2;
DROP TABLE IF EXISTS t_04350_merge2;

-- Leading key column `a` is unique, so the useless suffix key column `b` is dropped from the
-- in-memory index; `PARTITION BY b` gives `b` a partition-minmax bound (constant coordinate).
CREATE TABLE t_04350_lc2 (a UInt64, b LowCardinality(Bool))
    ENGINE = MergeTree ORDER BY (a, b) PARTITION BY b
    SETTINGS index_granularity = 1, allow_nullable_key = 1,
             primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns = 0.5;
INSERT INTO t_04350_lc2 SELECT number, number % 2 = 0 FROM numbers(1000);

-- Merge header declares the plain Bool type over the LowCardinality(Bool) source.
CREATE TABLE t_04350_merge2 (a UInt64, b Bool)
    ENGINE = Merge(currentDatabase(), 't_04350_lc2');

-- CAST wrapper over the LowCardinality key reached via the sparse constant-coordinate path
-- (previously a Bad cast LOGICAL_ERROR).
SELECT count() FROM t_04350_merge2 WHERE b < toLowCardinality(toNullable(7));

DROP TABLE t_04350_lc2;
DROP TABLE t_04350_merge2;
