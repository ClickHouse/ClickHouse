DROP TABLE IF EXISTS table_a;
DROP TABLE IF EXISTS table_b;

CREATE TABLE table_a (
    event_id UInt64,
    something String,
    other Nullable(String)
) ENGINE = MergeTree ORDER BY (event_id);

CREATE TABLE table_b (
    event_id UInt64,
    something Nullable(String),
    other String
) ENGINE = MergeTree ORDER BY (event_id);

INSERT INTO table_a VALUES (1, 'foo', 'foo'), (2, 'foo', 'foo'), (3, 'bar', 'bar');
INSERT INTO table_b VALUES (1, 'bar', 'bar'), (2, 'bar', 'bar'), (3, 'test', 'test'), (4, NULL, '');

SELECT s1.other, s2.other, count_a, count_b, toTypeName(s1.other), toTypeName(s2.other) FROM
    ( SELECT other, count() AS count_a FROM table_a GROUP BY other ) s1
ALL FULL JOIN
    ( SELECT other, count() AS count_b FROM table_b GROUP BY other ) s2
ON s1.other = s2.other
ORDER BY s2.other DESC;

SELECT s1.other, s2.other, count_a, count_b, toTypeName(s1.other), toTypeName(s2.other) FROM
    ( SELECT other, count() AS count_a FROM table_a GROUP BY other ) s1
ALL FULL JOIN
    ( SELECT other, count() AS count_b FROM table_b GROUP BY other ) s2
USING other
ORDER BY s2.other DESC;

SELECT s1.something, s2.something, count_a, count_b, toTypeName(s1.something), toTypeName(s2.something) FROM
    ( SELECT something, count() AS count_a FROM table_a GROUP BY something ) s1
ALL FULL JOIN
    ( SELECT something, count() AS count_b FROM table_b GROUP BY something ) s2
ON s1.something = s2.something
ORDER BY count_a DESC;

SELECT s1.something, s2.something, count_a, count_b, toTypeName(s1.something), toTypeName(s2.something) FROM
    ( SELECT something, count() AS count_a FROM table_a GROUP BY something ) s1
ALL RIGHT JOIN
    ( SELECT something, count() AS count_b FROM table_b GROUP BY something ) s2
USING (something)
ORDER BY count_a DESC;

SET joined_subquery_requires_alias = 0;

SELECT something, count_a, count_b, toTypeName(something) FROM
    ( SELECT something, count() AS count_a FROM table_a GROUP BY something )
ALL FULL JOIN
    ( SELECT something, count() AS count_b FROM table_b GROUP BY something )
USING (something)
ORDER BY count_a DESC;

DROP TABLE table_a;
DROP TABLE table_b;
