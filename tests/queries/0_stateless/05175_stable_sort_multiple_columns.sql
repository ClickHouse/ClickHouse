-- Sorting a block by several key columns must be stable: rows with equal keys keep the order
-- they were inserted in. A radix sort inside `updatePermutation` used to inherit the partial
-- ordering left behind by `trySort` when it gave up, which broke stability for equal ranges of
-- 256 rows or more.

DROP TABLE IF EXISTS t_stable_sort;

CREATE TABLE t_stable_sort (type Int64, id Int64, num Int64) ENGINE = MergeTree ORDER BY (type, id);

-- Each (type, id) pair appears twice, and every `type` group holds 1250 rows, so the sort of the
-- second key column runs on equal ranges above the radix sort threshold.
INSERT INTO t_stable_sort SELECT number % 40000 % 64, number % 40000, number FROM numbers(80000);

SELECT countIf(nums != arraySort(nums)) AS unstable_keys
FROM (SELECT groupArray(num) AS nums FROM t_stable_sort GROUP BY type, id)
SETTINGS max_threads = 1;

DROP TABLE t_stable_sort;

-- The same for a `Decimal` key column, which has its own radix sort path.

DROP TABLE IF EXISTS t_stable_sort_decimal;

CREATE TABLE t_stable_sort_decimal (type Int64, id Decimal64(2), num Int64) ENGINE = MergeTree ORDER BY (type, id);

INSERT INTO t_stable_sort_decimal SELECT number % 40000 % 64, number % 40000, number FROM numbers(80000);

SELECT countIf(nums != arraySort(nums)) AS unstable_keys
FROM (SELECT groupArray(num) AS nums FROM t_stable_sort_decimal GROUP BY type, id)
SETTINGS max_threads = 1;

DROP TABLE t_stable_sort_decimal;

-- The same instability made `CollapsingMergeTree` collapse rows that must not be collapsed. Here
-- every cancelling row precedes the state row it is paired with, so both rows have to survive.

DROP TABLE IF EXISTS t_stable_sort_collapsing;
DROP TABLE IF EXISTS t_stable_sort_source;

CREATE TABLE t_stable_sort_collapsing (type Int64, id Int64, status Int8, sign Int8)
ENGINE = CollapsingMergeTree(sign) ORDER BY (type, id);

CREATE TABLE t_stable_sort_source (type Int64, id Int64, status Int8, sign Int8)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_stable_sort_source SELECT number % 64, number, 10, -1 FROM numbers(40000);
INSERT INTO t_stable_sort_source SELECT number % 64, number, 20, 1 FROM numbers(40000);

INSERT INTO t_stable_sort_collapsing SELECT * FROM t_stable_sort_source ORDER BY sign
SETTINGS optimize_on_insert = 1;

SELECT count() AS rows_left, countIf(sign = 1) AS state_rows, countIf(sign = -1) AS cancel_rows
FROM t_stable_sort_collapsing;

DROP TABLE t_stable_sort_collapsing;
DROP TABLE t_stable_sort_source;
