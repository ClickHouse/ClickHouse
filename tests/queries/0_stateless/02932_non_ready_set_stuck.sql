CREATE TABLE tab (item_id UInt64, price_sold Nullable(Float32), date Date) ENGINE = MergeTree ORDER BY item_id;
SELECT * FROM (SELECT item_id FROM tab GROUP BY item_id WITH TOTALS ORDER BY '922337203.6854775806' IN (SELECT NULL)) AS l RIGHT JOIN (SELECT item_id FROM tab) AS r ON l.item_id = r.item_id WHERE NULL;
