-- A `String` column of a table that can produce automatically `LowCardinality`-encoded parts is not
-- dictionary-decoded into the substreams of its data type, so `length` / `notEmpty` must not be
-- rewritten to the `.size` subcolumn. The decision must not depend on whether an encoded part exists
-- right now: such a part can be committed while the query is being analyzed and still belong to the
-- parts the query reads, which would make the query throw depending on insert and merge timing.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_auto_lc_subcolumn_guard;
CREATE TABLE t_auto_lc_subcolumn_guard
(
    id UInt64,
    s String STATISTICS(uniq),
    arr Array(String)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 1000,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    min_bytes_for_wide_part = 0;

-- No part exists yet, so no column is encoded, but the table can still write an encoded part.
SELECT 'empty table, rewrite of a String column is skipped';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_subcolumn_guard) WHERE explain LIKE '%s.size%';

SELECT 'empty table, rewrite of an Array column still fires';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(arr) FROM t_auto_lc_subcolumn_guard) WHERE explain LIKE '%arr.size0%';

INSERT INTO t_auto_lc_subcolumn_guard SELECT number, 'v_' || toString(number % 10), ['a', 'b'] FROM numbers(2000);

SELECT 'encoded part, correctness';
SELECT sum(length(s)), countIf(notEmpty(s)), sum(length(arr)) FROM t_auto_lc_subcolumn_guard;

-- Once the feature is off no new encoded part can appear, but the existing one is still encoded.
ALTER TABLE t_auto_lc_subcolumn_guard MODIFY SETTING max_uniq_number_for_low_cardinality = 0;

SELECT 'feature disabled but the part is still encoded, rewrite is skipped';
SELECT count() FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_subcolumn_guard) WHERE explain LIKE '%s.size%';
SELECT sum(length(s)), countIf(notEmpty(s)) FROM t_auto_lc_subcolumn_guard;

DROP TABLE t_auto_lc_subcolumn_guard;

-- Without the feature the rewrite fires as usual.
DROP TABLE IF EXISTS t_auto_lc_subcolumn_guard_off;
CREATE TABLE t_auto_lc_subcolumn_guard_off
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_auto_lc_subcolumn_guard_off SELECT number, 'v_' || toString(number % 10) FROM numbers(2000);

SELECT 'feature never enabled, rewrite fires';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT length(s) FROM t_auto_lc_subcolumn_guard_off) WHERE explain LIKE '%s.size%';
SELECT sum(length(s)), countIf(notEmpty(s)) FROM t_auto_lc_subcolumn_guard_off;

DROP TABLE t_auto_lc_subcolumn_guard_off;
