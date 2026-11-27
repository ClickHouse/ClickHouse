SET allow_reserved_column_names = 1;
CREATE TABLE override_test__fuzz_45 (`_part` Float32) ENGINE = MergeTree ORDER BY tuple() AS SELECT 1;
SELECT _part FROM override_test__fuzz_45 GROUP BY materialize(6), 1;
