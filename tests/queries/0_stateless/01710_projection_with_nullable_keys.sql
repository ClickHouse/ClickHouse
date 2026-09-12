-- Disable force_primary_key_reverse_order: SHOW CREATE output contains ORDER BY which changes with forced DESC
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS sales;

CREATE TABLE sales (DATE_SOLD DateTime64(3, 'UTC'), PRODUCT_ID Nullable(String)) Engine MergeTree() PARTITION BY toYYYYMM(DATE_SOLD) ORDER BY DATE_SOLD;

ALTER TABLE sales ADD PROJECTION test (SELECT toInt64(COUNT(*)) GROUP BY PRODUCT_ID, DATE_SOLD);

SHOW CREATE sales;

DROP TABLE sales;
