-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET s3_truncate_on_insert=1;
INSERT INTO FUNCTION s3(
	s3_conn,
	filename='input/test_03229_s3_filter_handling_input_{_partition_id}',
	format=Parquet,
	structure='a UInt8, b String'
)
PARTITION BY a
SELECT
	a % 8 AS a,
	CASE
		WHEN a % 5 = 1 THEN concat('a', b)
		WHEN a % 5 = 2 THEN concat('b', b)
		ELSE b
	END AS b
FROM generateRandom('a UInt8, b String', 3453451233, 10, 2) LIMIT 100;

INSERT INTO FUNCTION s3(
	s3_conn,
	filename='output/test_03229_s3_filter_handling_output',
	format=Parquet,
	structure='a UInt8, b2 String'
)
SELECT
	a,
	CASE
		WHEN startsWith(b, 'a') THEN concat('1', b)
		WHEN startsWith(b, 'b') THEN concat('2', b)
		ELSE b
	END AS b2
FROM s3(
	s3_conn,
	filename='input/test_03229_s3_filter_handling_input_{1,2,3,4,5,6}*',
	format=Parquet,
	structure='a UInt8, b String'
)
WHERE (startsWith(b, 'a') OR startsWith(b, 'b'));

SELECT count() FROM s3(
	s3_conn,
	filename='output/test_03229_s3_filter_handling_output',
	format=Parquet,
	structure='a UInt8, b2 String'
);

