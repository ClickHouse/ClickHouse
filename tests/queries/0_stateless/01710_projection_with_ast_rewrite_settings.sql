DROP TABLE IF EXISTS aggregate_functions_null_for_empty;

CREATE TABLE aggregate_functions_null_for_empty (`x` UInt32, `y` UInt64, PROJECTION p (SELECT sum(y))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO aggregate_functions_null_for_empty SELECT number, number * 2 FROM numbers(8192 * 10) SETTINGS aggregate_functions_null_for_empty = true;

SELECT count() FROM aggregate_functions_null_for_empty;

DROP TABLE aggregate_functions_null_for_empty;

DROP TABLE IF EXISTS transform_null_in;

CREATE TABLE transform_null_in (`x` UInt32, `y` UInt64, PROJECTION p (SELECT sum(y in (1,2,3)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO transform_null_in SELECT number, number * 2 FROM numbers(8192 * 10) SETTINGS transform_null_in = true;

SELECT count() FROM transform_null_in;

DROP TABLE transform_null_in;

DROP TABLE IF EXISTS legacy_column_name_of_tuple_literal;

CREATE TABLE legacy_column_name_of_tuple_literal (`x` UInt32, `y` UInt64, PROJECTION p (SELECT sum(y in (1,2,3)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO legacy_column_name_of_tuple_literal SELECT number, number * 2 FROM numbers(8192 * 10) SETTINGS legacy_column_name_of_tuple_literal = true;

SELECT count() FROM legacy_column_name_of_tuple_literal;

DROP TABLE legacy_column_name_of_tuple_literal;
