DROP TABLE IF EXISTS left_table;
DROP TABLE IF EXISTS right_table;

CREATE TABLE left_table(APIKey Int32, SomeColumn String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO left_table VALUES(1, 'somestr');

CREATE TABLE right_table(APIKey Int32, EventValueForPostback String) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO right_table VALUES(1, 'hello'), (2, 'WORLD');

SELECT
    APIKey,
    ConversionEventValue
FROM
    left_table AS left_table
ALL INNER JOIN
    (
        SELECT *
        FROM
            (
                SELECT
                    APIKey,
                    EventValueForPostback AS ConversionEventValue
                FROM
                    right_table AS right_table
            )
            ALL INNER JOIN
            (
                SELECT
                    APIKey
                FROM
                    left_table as left_table
                GROUP BY
                    APIKey
            ) USING (APIKey)
    ) USING (APIKey);

DROP TABLE IF EXISTS left_table;
DROP TABLE IF EXISTS right_table;
