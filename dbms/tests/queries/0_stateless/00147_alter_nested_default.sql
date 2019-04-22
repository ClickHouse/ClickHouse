DROP TABLE IF EXISTS alter;

CREATE TABLE alter (d Date DEFAULT toDate('2015-01-01'), n Nested(x String)) ENGINE = MergeTree(d, d, 8192);

INSERT INTO alter (`n.x`) VALUES (['Hello', 'World']);

SELECT * FROM alter;
SELECT * FROM alter ARRAY JOIN n;
SELECT * FROM alter ARRAY JOIN n WHERE n.x LIKE '%Hello%';

ALTER TABLE alter ADD COLUMN n.y Array(UInt64);

SELECT * FROM alter;
SELECT * FROM alter ARRAY JOIN n;
SELECT * FROM alter ARRAY JOIN n WHERE n.x LIKE '%Hello%';

INSERT INTO alter (`n.x`) VALUES (['Hello2', 'World2']);

SELECT * FROM alter ORDER BY n.x;
SELECT * FROM alter ARRAY JOIN n ORDER BY n.x;
SELECT * FROM alter ARRAY JOIN n WHERE n.x LIKE '%Hello%' ORDER BY n.x;

OPTIMIZE TABLE alter;

SELECT * FROM alter;
SELECT * FROM alter ARRAY JOIN n;
SELECT * FROM alter ARRAY JOIN n WHERE n.x LIKE '%Hello%';

DROP TABLE alter;
