SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.Issue_2231_Invalid_Nested_Columns_Size;
CREATE TABLE test.Issue_2231_Invalid_Nested_Columns_Size (
    Date Date,
    NestedColumn Nested(
        ID    Int32,
        Count Int64
    )
) Engine = MergeTree 
    PARTITION BY tuple()
    ORDER BY Date;

INSERT INTO test.Issue_2231_Invalid_Nested_Columns_Size VALUES (today(), [2,2], [1]), (today(), [2,2], [1, 1]); -- { serverError 190 }

SELECT * FROM test.Issue_2231_Invalid_Nested_Columns_Size;
DROP TABLE test.Issue_2231_Invalid_Nested_Columns_Size;
