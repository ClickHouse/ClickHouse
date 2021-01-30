DROP TABLE IF EXISTS t;
CREATE TABLE t (`item_id` UInt64, `price_sold` Float32, `date` Date) ENGINE = MergeTree ORDER BY item_id;
SELECT item_id FROM (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) AS l FULL OUTER JOIN (SELECT item_id FROM t GROUP BY item_id WITH TOTALS) AS r USING (item_id);
DROP TABLE t;
