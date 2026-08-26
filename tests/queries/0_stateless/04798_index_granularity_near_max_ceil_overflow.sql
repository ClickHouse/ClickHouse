-- Tags: no-replicated-database, no-shared-merge-tree

-- A granularity close to the maximum of UInt64 must still return the rows that were written, both for
-- the data marks of a part with non-adaptive granularity and for the granules of a secondary index.

DROP TABLE IF EXISTS t_granularity_near_max;

-- Non-adaptive granularity stores wide parts only, so both wide-part thresholds must be 0.
CREATE TABLE t_granularity_near_max (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity_bytes = 0, index_granularity = 18446744073709551615, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO t_granularity_near_max SELECT number FROM numbers(10);

SELECT 'written', count(), sum(x) FROM t_granularity_near_max SETTINGS optimize_trivial_count_query = 0;
SELECT 'read', x FROM t_granularity_near_max ORDER BY x;

-- Reloading the table must see the same rows the insert wrote.
DETACH TABLE t_granularity_near_max;
ATTACH TABLE t_granularity_near_max;

SELECT 'reloaded', count(), sum(x) FROM t_granularity_near_max SETTINGS optimize_trivial_count_query = 0;

DROP TABLE t_granularity_near_max;

DROP TABLE IF EXISTS t_skip_granularity_near_max;

-- An index GRANULARITY counts marks, so the part needs more than one mark for the rounding to matter.
CREATE TABLE t_skip_granularity_near_max (x UInt64, y UInt64, INDEX i y TYPE minmax GRANULARITY 18446744073709551615)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity = 64;

INSERT INTO t_skip_granularity_near_max SELECT number, number FROM numbers(4096);

SELECT 'skip index, no filter', count(), sum(y) FROM t_skip_granularity_near_max;
SELECT 'skip index, filtered', count(), sum(y) FROM t_skip_granularity_near_max WHERE y = 500 SETTINGS force_data_skipping_indices = 'i';

DROP TABLE t_skip_granularity_near_max;

DROP TABLE IF EXISTS t_skip_granularity_one;

CREATE TABLE t_skip_granularity_one (x UInt64, y UInt64, INDEX i y TYPE minmax GRANULARITY 1)
ENGINE = MergeTree() ORDER BY x
SETTINGS index_granularity = 64;

INSERT INTO t_skip_granularity_one SELECT number, number FROM numbers(4096);

SELECT 'skip index granularity 1, filtered', count(), sum(y) FROM t_skip_granularity_one WHERE y = 500 SETTINGS force_data_skipping_indices = 'i';

DROP TABLE t_skip_granularity_one;
