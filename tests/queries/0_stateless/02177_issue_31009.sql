-- Tags: long

CREATE TABLE left ( key UInt32, value String ) ENGINE = MergeTree ORDER BY key;
CREATE TABLE right (  key UInt32, value String ) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO left SELECT number, toString(number) FROM numbers(25367182);
INSERT INTO right SELECT number, toString(number) FROM numbers(23124707);

SET join_algorithm = 'partial_merge';

SELECT key, count(1) AS cnt
FROM (
    SELECT data.key
    FROM ( SELECT key FROM left AS s ) AS data
    LEFT JOIN ( SELECT key FROM right GROUP BY key ) AS promo ON promo.key = data.key
) GROUP BY key HAVING count(1) > 1;

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;
