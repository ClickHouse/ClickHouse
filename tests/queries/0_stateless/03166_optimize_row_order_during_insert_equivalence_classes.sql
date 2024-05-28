-- Checks that RowOptimizer correctly selects the order for columns according to cardinality in each equivalence class obtained using SortDescription.
-- There are two columns in the SortDescription: {flag, money} in this order.
-- So there are 5 equivalence classes: {9.81, 9}, {2.7, 1}, {42, 1}, {3.14, Null}, {42, Null}.
-- For the first three of them cardinalities of the other 2 columns are equal, so they are sorted in order {0, 1} in these classes.
-- In the fourth class cardinalities: {name : 2, timestamp : 3}, so they are sorted in order {name, timestamp} in this class.
-- In the fifth class cardinalities: {name : 3, timestamp : 2}, so they are sorted in order {timestamp, name} in this class.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (name FixedString(2), timestamp Float32, money Float64, flag Nullable(Int32)) ENGINE = MergeTree ORDER BY (flag, money) SETTINGS allow_experimental_optimized_row_order = True, allow_nullable_key = True;
INSERT INTO tab VALUES ('AB', 0, 42, Null), ('AB', 0, 42, Null), ('A', 1, 42, Null), ('AB', 1, 9.81, 0), ('B', 0, 42, Null), ('B', -1, 3.14, Null), ('B', 1, 2.7, 1), ('B', 0, 42, 1), ('A', 1, 42, 1), ('B', 1, 42, Null), ('B', 0, 2.7, 1), ('A', 0, 2.7, 1), ('B', 2, 3.14, Null), ('A', 0, 3.14, Null), ('A', 1, 2.7, 1), ('A', 1, 42, Null);

SELECT * FROM tab SETTINGS max_threads=1;

DROP TABLE tab;
