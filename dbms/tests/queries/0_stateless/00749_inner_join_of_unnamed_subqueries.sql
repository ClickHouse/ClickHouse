DROP TABLE IF EXISTS test.left_table;
DROP TABLE IF EXISTS test.right_table;

CREATE TABLE test.left_table(APIKey Int32, SomeColumn String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test.left_table VALUES(1, 'somestr');

CREATE TABLE test.right_table(APIKey Int32, EventValueForPostback String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test.right_table VALUES(1, 'hello'), (2, 'WORLD');

SELECT
    APIKey,
    ConversionEventValue
FROM
    test.left_table AS left_table
ALL INNER JOIN
    (
        SELECT *
        FROM
            (
                SELECT
                    APIKey,
                    EventValueForPostback AS ConversionEventValue
                FROM
                    test.right_table AS right_table
            )
            ALL INNER JOIN
            (
                SELECT
                    APIKey
                FROM
                    test.left_table as left_table
                GROUP BY
                    APIKey
            ) USING (APIKey)
    ) USING (APIKey);

DROP TABLE IF EXISTS test.left_table;
DROP TABLE IF EXISTS test.right_table;
