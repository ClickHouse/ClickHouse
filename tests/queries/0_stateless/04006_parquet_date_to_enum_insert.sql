-- Tags: no-fasttest
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/79022
-- Inserting a Parquet file with a Date column into an Enum column should fail,
-- not silently produce invalid enum values.

DROP TABLE IF EXISTS t_enum_parquet;

CREATE TABLE t_enum_parquet (c0 Enum('a' = 1)) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY (c0);

INSERT INTO TABLE FUNCTION file('04006_parquet_date_to_enum_{currentDatabase}.parquet', 'Parquet', 'c0 Date') SELECT toDate('2000-01-01');
INSERT INTO t_enum_parquet SELECT * FROM file('04006_parquet_date_to_enum_{currentDatabase}.parquet', 'Parquet'); -- { serverError CANNOT_CONVERT_TYPE }

-- Also test Arrow format
INSERT INTO TABLE FUNCTION file('04006_arrow_date_to_enum_{currentDatabase}.arrow', 'Arrow', 'c0 Date') SELECT toDate('2000-01-01');
INSERT INTO t_enum_parquet SELECT * FROM file('04006_arrow_date_to_enum_{currentDatabase}.arrow', 'Arrow'); -- { serverError CANNOT_CONVERT_TYPE }

DROP TABLE t_enum_parquet;
