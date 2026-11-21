DROP TABLE IF EXISTS t_rpn_pos;

CREATE TABLE t_rpn_pos
(
    id UInt32,
    a UInt8,
    x UInt8,
    y UInt8,
    INDEX iax (a, x) TYPE minmax GRANULARITY 2,
    INDEX iy y TYPE minmax GRANULARITY 2
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;

INSERT INTO t_rpn_pos VALUES (0, 0, 1, 1), (1, 200, 1, 1), (2, 0, 1, 7), (3, 200, 1, 1);

-- `intDiv(a, toInt8(-1))` reinterprets `a` through a signed cast, so it is not monotonic over an `a`
-- range that crosses 128, and the minmax index on (a, x) cannot decide that atom for either index
-- granule here. Row 2 matches only through the `y = 7` arm, which the index on y does decide.
SELECT count() FROM t_rpn_pos WHERE intDiv(a, toInt8(-1)) < 1 AND (x = 5 OR y = 7)
SETTINGS use_skip_indexes = 0;

SELECT count() FROM t_rpn_pos WHERE intDiv(a, toInt8(-1)) < 1 AND (x = 5 OR y = 7)
SETTINGS use_skip_indexes = 1, use_skip_indexes_for_disjunctions = 1;

-- The counts above are only meaningful if the merged-disjunction path actually ran: with it off no
-- granule is pruned and row-level filtering returns the same 1. `<Combined skip indexes>` appears in
-- EXPLAIN only while that path is on, so these two probes are its positive and negative control.
SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_rpn_pos WHERE intDiv(a, toInt8(-1)) < 1 AND (x = 5 OR y = 7)
    SETTINGS use_skip_indexes = 1, use_skip_indexes_for_disjunctions = 1,
             use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0,
             parallel_replicas_local_plan = 1, explain_query_plan_default = 'legacy'
) WHERE explain ILIKE '%<Combined skip indexes>%';

SELECT count() > 0 FROM (
    EXPLAIN indexes = 1
    SELECT count() FROM t_rpn_pos WHERE intDiv(a, toInt8(-1)) < 1 AND (x = 5 OR y = 7)
    SETTINGS use_skip_indexes = 1, use_skip_indexes_for_disjunctions = 0,
             use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0,
             parallel_replicas_local_plan = 1, explain_query_plan_default = 'legacy'
) WHERE explain ILIKE '%<Combined skip indexes>%';

DROP TABLE t_rpn_pos;

-- An atom the index cannot evaluate (a function of the column), followed by a constant disjunct that
-- the next index reports as false.
DROP TABLE IF EXISTS t_skip_or0;
CREATE TABLE t_skip_or0
(
    id UInt32,
    e Enum8('a' = 1, 'b' = 2, 'c' = 3),
    INDEX ie e TYPE minmax GRANULARITY 1,
    INDEX ii id TYPE minmax GRANULARITY 1,
    INDEX ise e TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_skip_or0 SELECT number, ['a', 'b', 'c'][number % 3 + 1] FROM numbers(1000);

SELECT count() FROM t_skip_or0 WHERE toString(e) < 'c' AND (0 OR id != 5)
SETTINGS use_skip_indexes = 1, use_skip_indexes_for_disjunctions = 1;
SELECT count() FROM t_skip_or0 WHERE e < 'c' AND (toString(id) != '5' OR 0)
SETTINGS use_skip_indexes = 1, use_skip_indexes_for_disjunctions = 1;
-- The same through a set index on e.
SELECT count() FROM t_skip_or0 WHERE toString(e) < 'c' AND (0 OR id != 5)
SETTINGS use_skip_indexes = 1, use_skip_indexes_for_disjunctions = 1, ignore_data_skipping_indices = 'ie';
DROP TABLE t_skip_or0;

-- Implicit minmax indexes; the comparison of a UInt128 column with a Nullable(Int256) constant.
DROP TABLE IF EXISTS t_rpn_implicit;
CREATE TABLE t_rpn_implicit (v UInt128, w UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 1;
INSERT INTO t_rpn_implicit SELECT number, number % 3 + 1 FROM numbers(100);
SELECT count() FROM t_rpn_implicit WHERE w > 0 AND (toNullable(toInt256(-1)) < v OR 0)
SETTINGS use_skip_indexes = 1, use_skip_indexes_for_disjunctions = 1;
DROP TABLE t_rpn_implicit;
