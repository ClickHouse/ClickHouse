DROP TABLE IF EXISTS t;
CREATE TABLE t (item_id UInt64, price_sold Float32, date Date) ENGINE MergeTree ORDER BY item_id;

SELECT item_id
FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) l
FULL JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) r
USING (item_id);

SELECT id
FROM (SELECT item_id AS id FROM t GROUP BY id WITH TOTALS) l
FULL JOIN (SELECT item_id AS id FROM t GROUP BY id WITH TOTALS) r
USING (id);

SELECT item_id
FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) l
INNER JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) r
USING (item_id);

SELECT id
FROM (SELECT item_id AS id FROM t GROUP BY id WITH TOTALS) l
INNER JOIN (SELECT item_id AS id FROM t GROUP BY id WITH TOTALS) r
USING (id);

SELECT id, yago, recent
FROM (
    SELECT item_id AS id, SUM(price_sold) AS recent
    FROM t WHERE (date BETWEEN '2019-12-16' AND '2020-03-08')
    GROUP BY id WITH TOTALS
) ll
FULL JOIN
(
    SELECT item_id AS id, SUM(price_sold) AS yago
    FROM t WHERE (date BETWEEN '2018-12-17' AND '2019-03-10')
    GROUP BY id WITH TOTALS
) rr
USING (id);

SELECT id, yago
FROM ( SELECT item_id AS id FROM t GROUP BY id ) AS ll
FULL OUTER JOIN ( SELECT item_id AS id, arrayJoin([111, 222, 333, 444]), SUM(price_sold) AS yago FROM t GROUP BY id WITH TOTALS ) AS rr
USING (id);

SELECT id, yago
FROM ( SELECT item_id AS id, arrayJoin([111, 222, 333]) FROM t GROUP BY id WITH TOTALS ) AS ll
FULL OUTER JOIN ( SELECT item_id AS id, SUM(price_sold) AS yago FROM t GROUP BY id ) AS rr
USING (id);

SELECT id, yago
FROM ( SELECT item_id AS id, arrayJoin(emptyArrayInt32()) FROM t GROUP BY id WITH TOTALS ) AS ll
FULL OUTER JOIN ( SELECT item_id AS id, SUM(price_sold) AS yago FROM t GROUP BY id ) AS rr
USING (id);

SELECT id, yago
FROM ( SELECT item_id AS id FROM t GROUP BY id ) AS ll
FULL OUTER JOIN ( SELECT item_id AS id, arrayJoin(emptyArrayInt32()), SUM(price_sold) AS yago FROM t GROUP BY id WITH TOTALS ) AS rr
USING (id);

SELECT id, yago
FROM ( SELECT item_id AS id, arrayJoin([111, 222, 333]) FROM t GROUP BY id WITH TOTALS ) AS ll
FULL OUTER JOIN ( SELECT item_id AS id, arrayJoin([111, 222, 333, 444]), SUM(price_sold) AS yago FROM t GROUP BY id WITH TOTALS ) AS rr
USING (id);

INSERT INTO t VALUES (1, 100, '1970-01-01'), (1, 200, '1970-01-02');

SELECT *
FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) l
LEFT JOIN (SELECT item_id FROM t ) r
ON l.item_id = r.item_id;

SELECT *
FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) l
RIGHT JOIN (SELECT item_id FROM t ) r
ON l.item_id = r.item_id;

SELECT *
FROM (SELECT item_id FROM t) l
LEFT JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS ) r
ON l.item_id = r.item_id;

SELECT *
FROM (SELECT item_id FROM t) l
RIGHT JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS ) r
ON l.item_id = r.item_id;

SELECT *
FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) l
LEFT JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS ) r
ON l.item_id = r.item_id;

SELECT *
FROM (SELECT item_id, 'foo' AS key, 1 AS val FROM t GROUP BY item_id WITH TOTALS) l
LEFT JOIN (SELECT item_id, sum(price_sold) AS val FROM t GROUP BY item_id WITH TOTALS ) r
ON l.item_id = r.item_id;

SELECT *
FROM (SELECT * FROM t GROUP BY item_id, price_sold, date WITH TOTALS) l
LEFT JOIN (SELECT * FROM t GROUP BY item_id, price_sold, date WITH TOTALS ) r
ON l.item_id = r.item_id;

DROP TABLE t;
