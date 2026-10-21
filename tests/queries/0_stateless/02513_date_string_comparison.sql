CREATE TABLE datetime_date_table (
    col_date Date,
    col_datetime DateTime,
    col_datetime64 DateTime64(3),
    col_date_string String,
    col_datetime_string String,
    col_datetime64_string DateTime64,
    col_date_lc LowCardinality(String),
    col_datetime_lc LowCardinality(String),
    col_datetime64_lc LowCardinality(String),
    PRIMARY KEY col_date
) ENGINE = MergeTree;

INSERT INTO datetime_date_table VALUES ('2020-03-04', '2020-03-04 10:23:45', '2020-03-04 10:23:45.123', '2020-03-04', '2020-03-04 10:23:45', '2020-03-04 10:23:45.123', '2020-03-04', '2020-03-04 10:23:45', '2020-03-04 10:23:45.123');
INSERT INTO datetime_date_table VALUES ('2020-03-05', '2020-03-05 12:23:45', '2020-03-05 12:23:45.123', '2020-03-05', '2020-03-05 12:23:45', '2020-03-05 12:23:45.123', '2020-03-05', '2020-03-05 12:23:45', '2020-03-05 12:23:45.123');
INSERT INTO datetime_date_table VALUES ('2020-04-05', '2020-04-05 00:10:45', '2020-04-05 00:10:45.123', '2020-04-05', '2020-04-05 00:10:45', '2020-04-05 00:10:45.123', '2020-04-05', '2020-04-05 00:10:45', '2020-04-05 00:10:45.123');

SELECT 'Date';
SELECT count() FROM datetime_date_table WHERE col_date > '2020-03-04';
SELECT count() FROM datetime_date_table WHERE col_date > '2020-03-04'::Date;
SELECT count() FROM datetime_date_table WHERE col_date > '2020-03-04 10:20:45'; -- { serverError TYPE_MISMATCH }
SELECT count() FROM datetime_date_table WHERE col_date > '2020-03-04 10:20:45'::DateTime;
SELECT count() FROM datetime_date_table WHERE col_date > '2020-03-04 10:20:45.100'; -- { serverError TYPE_MISMATCH }
SELECT count() FROM datetime_date_table WHERE col_date > '2020-03-04 10:20:45.100'::DateTime64(3);

SELECT 'DateTime';
SELECT count() FROM datetime_date_table WHERE col_datetime > '2020-03-04';
SELECT count() FROM datetime_date_table WHERE col_datetime > '2020-03-04'::Date;
SELECT count() FROM datetime_date_table WHERE col_datetime > '2020-03-04 10:20:45';
SELECT count() FROM datetime_date_table WHERE col_datetime > '2020-03-04 10:20:45'::DateTime;
SELECT count() FROM datetime_date_table WHERE col_datetime > '2020-03-04 10:20:45.100'; -- { serverError TYPE_MISMATCH }
SELECT count() FROM datetime_date_table WHERE col_datetime > '2020-03-04 10:20:45.100'::DateTime64(3);

SELECT 'Date String';
SELECT count() FROM datetime_date_table WHERE col_date_string > '2020-03-04';
SELECT count() FROM datetime_date_table WHERE col_date_string > '2020-03-04'::Date; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_date_string > '2020-03-04 10:20:45';
SELECT count() FROM datetime_date_table WHERE col_date_string > '2020-03-04 10:20:45'::DateTime; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_date_string > '2020-03-04 10:20:45.100';
SELECT count() FROM datetime_date_table WHERE col_date_string > '2020-03-04 10:20:45.100'::DateTime64(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'DateTime String';
SELECT count() FROM datetime_date_table WHERE col_datetime_string > '2020-03-04';
SELECT count() FROM datetime_date_table WHERE col_datetime_string > '2020-03-04'::Date; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_datetime_string > '2020-03-04 10:20:45';
SELECT count() FROM datetime_date_table WHERE col_datetime_string > '2020-03-04 10:20:45'::DateTime; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_datetime_string > '2020-03-04 10:20:45.100';
SELECT count() FROM datetime_date_table WHERE col_datetime_string > '2020-03-04 10:20:45.100'::DateTime64(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Date LC';
SELECT count() FROM datetime_date_table WHERE col_date_lc > '2020-03-04';
SELECT count() FROM datetime_date_table WHERE col_date_lc > '2020-03-04'::Date; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_date_lc > '2020-03-04 10:20:45';
SELECT count() FROM datetime_date_table WHERE col_date_lc > '2020-03-04 10:20:45'::DateTime; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_date_lc > '2020-03-04 10:20:45.100';
SELECT count() FROM datetime_date_table WHERE col_date_lc > '2020-03-04 10:20:45.100'::DateTime64(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'DateTime LC';
SELECT count() FROM datetime_date_table WHERE col_datetime_lc > '2020-03-04';
SELECT count() FROM datetime_date_table WHERE col_datetime_lc > '2020-03-04'::Date;  -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_datetime_lc > '2020-03-04 10:20:45';
SELECT count() FROM datetime_date_table WHERE col_datetime_lc > '2020-03-04 10:20:45'::DateTime; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM datetime_date_table WHERE col_datetime_lc > '2020-03-04 10:20:45.100';
SELECT count() FROM datetime_date_table WHERE col_datetime_lc > '2020-03-04 10:20:45.100'::DateTime64(3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

