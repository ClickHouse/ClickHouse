-- Tags: no-random-merge-tree-settings
-- Test: exercises `KeyCondition::extractAtomFromTree` Case2 (`pointInPolygon` with hole rings, num_args > 2).
-- Covers: src/Storages/MergeTree/KeyCondition.cpp:3600-3604 — `if (func_name == "pointInPolygon") return analyze_point_in_polygon()`
-- inside the `else` (num_args > 2) block. PR's own test 03031 only exercises Case1 (num_args == 2, no holes).


DROP TABLE IF EXISTS minmax_pip_holes;

CREATE TABLE minmax_pip_holes
(
    x UInt32,
    y UInt32,
    INDEX mm_x_y (x, y) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY x
SETTINGS index_granularity = 3, add_minmax_index_for_numeric_columns = 0;

INSERT INTO minmax_pip_holes VALUES (4, 4), (6, 6), (8, 8), (12, 12), (14, 14), (16, 16);

-- `pointInPolygon` with a hole. Outer ring covers only mark1 (x,y in [4..8]).
-- Mark2 (x,y in [12..16]) does not intersect outer ring -> minmax should drop it.
-- (6,6) is inside the hole -> excluded by the function, not by the index.
-- Without Case2 path, predicate is unknown: no marks pruned, all 6 rows scanned.
SELECT * FROM minmax_pip_holes
WHERE pointInPolygon((x, y), [(3., 3.), (9., 3.), (9., 9.), (3., 9.)], [(5., 5.), (7., 5.), (7., 7.), (5., 7.)])
ORDER BY x;

-- EXPLAIN ESTIMATE shows: parts, rows, marks. Selecting `rows` confirms only mark1 (3 rows) is read.
SELECT rows FROM
(
    EXPLAIN ESTIMATE
    SELECT * FROM minmax_pip_holes
    WHERE pointInPolygon((x, y), [(3., 3.), (9., 3.), (9., 9.), (3., 9.)], [(5., 5.), (7., 5.), (7., 7.), (5., 7.)])
);

DROP TABLE minmax_pip_holes;
