DROP TABLE IF EXISTS partition_and_primary_keys_using_same_expression;

CREATE TABLE partition_and_primary_keys_using_same_expression(dt DateTime)
    ENGINE MergeTree PARTITION BY toDate(dt) ORDER BY toDayOfWeek(toDate(dt));

INSERT INTO partition_and_primary_keys_using_same_expression
    VALUES ('2018-02-19 12:00:00');
INSERT INTO partition_and_primary_keys_using_same_expression
    VALUES ('2018-02-20 12:00:00'), ('2018-02-21 12:00:00');

SELECT * FROM partition_and_primary_keys_using_same_expression ORDER BY dt;

SELECT '---';

ALTER TABLE partition_and_primary_keys_using_same_expression DROP PARTITION '2018-02-20';
SELECT * FROM partition_and_primary_keys_using_same_expression ORDER BY dt;

DROP TABLE partition_and_primary_keys_using_same_expression;
