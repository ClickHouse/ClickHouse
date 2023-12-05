CREATE TABLE tab__fuzz_44 (`item_id` Array(LowCardinality(Date)), `price_sold` UUID, `date` UInt8) ENGINE = MergeTree ORDER BY item_id;
CREATE TABLE tab__fuzz_40 (`item_id` Array(Date), `price_sold` Array(LowCardinality(Date)), `date` Nullable(UUID)) ENGINE = MergeTree ORDER BY item_id;


SELECT * FROM (SELECT item_id FROM tab__fuzz_44 GROUP BY item_id WITH TOTALS ORDER BY '922337203.6854775806' IN (SELECT NULL) ASC NULLS LAST) AS l RIGHT JOIN (SELECT item_id FROM tab__fuzz_40) AS r ON l.item_id = r.item_id format Null;
