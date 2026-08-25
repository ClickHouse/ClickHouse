-- Tags: long, no-tsan, no-asan, no-msan, no-debug

SET max_threads=0;

DROP TABLE IF EXISTS left;
DROP TABLE IF EXISTS right;

CREATE TABLE left ( key UInt32, value String ) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
CREATE TABLE right (  key UInt32, value String ) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

SET max_rows_to_read = '50M';

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
