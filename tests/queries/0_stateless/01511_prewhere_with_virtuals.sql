DROP TABLE IF EXISTS  test_not_found_column_nothing;

CREATE TABLE test_not_found_column_nothing
(
    col001 UInt8,
    col002 UInt8
) Engine=MergeTree ORDER BY tuple() PARTITION BY col001 % 3;

INSERT INTO test_not_found_column_nothing(col001) SELECT number FROM numbers(11);

SELECT _part, count() FROM test_not_found_column_nothing PREWHERE col001 % 3 != 0 GROUP BY _part ORDER BY _part;
SELECT _part FROM test_not_found_column_nothing PREWHERE col001 = 0;

DROP TABLE test_not_found_column_nothing;
