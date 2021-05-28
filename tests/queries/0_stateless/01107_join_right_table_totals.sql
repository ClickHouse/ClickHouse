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

DROP TABLE t;
