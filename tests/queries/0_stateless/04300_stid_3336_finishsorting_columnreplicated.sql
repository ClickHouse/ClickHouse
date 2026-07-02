-- Documentation test for STID 3336-2f87:
--   `Logical error: Bad cast from type DB::ColumnReplicated to DB::ColumnString`
--     in `FinishSortingTransform::consume` -> `less` -> `ColumnString::doCompareAt`.
--
-- Captured in CI on sanitizer/parallel runs (three unrelated PRs in a ~16h window),
-- but the exception does not reproduce on master HEAD on a debug build. This test
-- pins the failing query shape so any future reproduction can be located by name.

SET allow_experimental_variant_type = 1;
SET allow_suspicious_types_in_order_by = 1;

DROP TABLE IF EXISTS t_stid_3336;

CREATE TABLE t_stid_3336(
    key String,
    val Map(String, Variant(String, Int32, DateTime64(3, 'UTC')))
) ENGINE = MergeTree ORDER BY key;

-- Mix maps of size 1 (force materialization of `key`) and size >= 2 (keep
-- `key` as `ColumnReplicated` in the chunk).
INSERT INTO t_stid_3336 VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10.11'});
INSERT INTO t_stid_3336 VALUES ('a', {'x':'xx'});
INSERT INTO t_stid_3336 VALUES ('a', {'a':'a', 'b':1, 'c': '2020-01-01 10:10:10'});
INSERT INTO t_stid_3336 VALUES ('a', {'a':'b', 'b':1, 'c': '2020-01-01'});
INSERT INTO t_stid_3336 VALUES ('z', {'a':'a'});
INSERT INTO t_stid_3336 VALUES ('a', {'a': Null, 'c': Null});

-- `ORDER BY ALL` keeps the `key` column as the sort prefix (MergeTree sort key),
-- so the planner uses `FinishSortingTransform` rather than the full
-- `PartialSortingTransform`. Output ordering is irrelevant; we only need the
-- query to complete without `Bad cast from type DB::ColumnReplicated to DB::ColumnString`.
SELECT key, arrayJoin(mapValues(val)) FROM t_stid_3336 ORDER BY ALL FORMAT Null;

DROP TABLE t_stid_3336;
