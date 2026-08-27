-- Regression test: Dynamic column with LowCardinality variant and enable_join_runtime_filters
-- caused LOGICAL_ERROR "Bad cast from type DB::ColumnVector<int> to DB::ColumnLowCardinality"
-- because convertToFullIfNeeded recursively stripped LowCardinality from Dynamic's internal
-- variant columns without updating variant_info type metadata, causing column/type mismatches
-- in Set::appendSetElements when serializing values into shared variant storage.
-- max_threads=1 is needed to reliably trigger the runtime filter code path.
-- https://github.com/ClickHouse/ClickHouse/issues/97847
DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (`c0` Dynamic(max_types=1)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t0;
INSERT INTO t0 SETTINGS allow_suspicious_low_cardinality_types=1 SELECT 'str_' || toString(number) FROM numbers(100);
INSERT INTO t0 SETTINGS allow_suspicious_low_cardinality_types=1 VALUES (1::LowCardinality(Int32)), (2::LowCardinality(Int32));
SELECT count() FROM (
    SELECT t0.c0 FROM (SELECT NULL AS c0) AS v0 RIGHT JOIN t0 USING (c0)
    SETTINGS allow_dynamic_type_in_join_keys=1, enable_join_runtime_filters=1, max_threads=1
);
DROP TABLE t0;
