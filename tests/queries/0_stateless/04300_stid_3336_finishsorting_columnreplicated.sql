-- Regression test for STID 3336-2f87:
--   Logical error: Bad cast from type DB::ColumnReplicated to DB::ColumnString
--     in `FinishSortingTransform::consume` -> `less(...)` -> `ColumnString::doCompareAt`.
--
-- `arrayJoin(mapValues(val))` produces a `ColumnReplicated` for the `key` column when
-- the input row's map has 2+ elements; chunks where the map has 1 element materialize
-- `key` to a plain column. Because `compactReplicatedColumns` decides per-chunk, the
-- binary search in `FinishSortingTransform::consume` could end up comparing a plain
-- `ColumnString` against a `ColumnReplicated` and `assert_cast` would fail.
--
-- Originally surfaced by `03221_variant_logical_error.sql`.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_stid_3336;

CREATE TABLE t_stid_3336(
    key String,
    val Map(String, Variant(String, Int32, DateTime64(3, 'UTC')))
) ENGINE = MergeTree ORDER BY key;

-- A mix of maps with size 1 (force materialization of `key`) and size >=2 (keep
-- `key` as `ColumnReplicated` in the chunk).
INSERT INTO t_stid_3336 VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10.11'});
INSERT INTO t_stid_3336 VALUES ('a', {'x':'xx'});
INSERT INTO t_stid_3336 VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10'});
INSERT INTO t_stid_3336 VALUES ('a', {'a':'b', 'b':1, 'c': '2020-01-01'});
INSERT INTO t_stid_3336 VALUES ('z', {'a':'a'});
INSERT INTO t_stid_3336 VALUES ('a', {'a': Null, 'c': Null});

-- `ORDER BY ALL` keeps the `key` column as the sort prefix (the MergeTree sort key),
-- so the planner uses `FinishSortingTransform` rather than the full
-- `PartialSortingTransform`. The output ordering is irrelevant; we only need the query
-- to not crash with `Bad cast from type DB::ColumnReplicated to DB::ColumnString`.
SELECT key, arrayJoin(mapValues(val)) FROM t_stid_3336 ORDER BY ALL FORMAT Null;

DROP TABLE t_stid_3336;
