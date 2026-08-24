-- add_minmax_index_for_numeric_columns=0: Changes the plan and rows read
DROP TABLE IF EXISTS rmt1;

SET use_skip_indexes=1;
SET use_skip_indexes_if_final=1;
SET use_skip_indexes_if_final_exact_mode=1;


CREATE TABLE rmt1
(
    id UInt32,
    val UInt32,
    INDEX vidx val TYPE minmax
) Engine = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 64, add_minmax_index_for_numeric_columns=0;


SYSTEM STOP MERGES rmt1;

-- insert primary key id = 1..10000
INSERT INTO rmt1 SELECT number+1, number+1 FROM numbers(10000);

-- insert primary key id = 10001..20000
INSERT INTO rmt1 SELECT 10001+number, 10001+number FROM numbers(10000);

-- specially crafted mutation granule with primary key range that intersects with almost all granules
INSERT INTO rmt1 VALUES (5, 88888888), (18500, 99999999);


-- Verify granules selected for the next 5 queries
SELECT splitByChar('/',trimLeft(explain))[1] FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM rmt1 FINAL WHERE id = 25 AND val = 88888888)
WHERE explain like '%Granules:%';

SELECT splitByChar('/',trimLeft(explain))[1] FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM rmt1 FINAL WHERE id < 1000 AND val = 88888888)
WHERE explain like '%Granules:%';

-- PK selects more granules but intersection will be lesser
SELECT splitByChar('/',trimLeft(explain))[1] FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM rmt1 FINAL WHERE id > 18000 AND id < 19500 AND val = 99999999)
WHERE explain like '%Granules:%';

SELECT splitByChar('/',trimLeft(explain))[1] FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM rmt1 FINAL WHERE id IN (100, 500, 12000, 18000) AND val = 88888888)
WHERE explain like '%Granules:%';

SELECT splitByChar('/',trimLeft(explain))[1] FROM (
    EXPLAIN indexes=1 SELECT count(*) FROM rmt1 FINAL WHERE (id BETWEEN 650 AND 900 OR id BETWEEN 8000 AND 9000 OR id BETWEEN 12000 AND 13000 AND id BETWEEN 16000 AND 16500) AND val = 88888888)
WHERE explain like '%Granules:%';

-- execute the queries to verify ranges are correctly added
SELECT count(*) FROM rmt1 FINAL WHERE id = 25 AND val = 88888888;
SELECT count(*) FROM rmt1 FINAL WHERE id < 1000 AND val = 88888888;
SELECT count(*) FROM rmt1 FINAL WHERE id > 18000 AND id < 19500 AND val = 99999999;
SELECT count(*) FROM rmt1 FINAL WHERE id IN (100, 500, 12000, 18000) AND val = 88888888;
SELECT count(*) FROM rmt1 FINAL WHERE (id BETWEEN 650 AND 900 OR id BETWEEN 8000 AND 9000 OR id BETWEEN 12000 AND 13000 AND id BETWEEN 16000 AND 16500) AND val = 88888888;

DROP TABLE rmt1;
