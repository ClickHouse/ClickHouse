SET allow_statistics_optimize = 0;
SET use_skip_indexes_on_data_read = 0; -- for correct row count estimation in join order planning
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS sales;

SET enable_analyzer = 1;

CREATE TABLE sales (
    id Int32,
    date Date,
    amount Decimal(10, 2),
    product_id Int32
) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

INSERT INTO sales SELECT number, '2024-05-05' + INTERVAL intDiv(number, 1000) DAY , (number + 1) % 100, number % 100_000 FROM numbers(1_000_000);

CREATE TABLE products (id Int32, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO products SELECT number, 'product ' || toString(number) FROM numbers(100_000);

SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_limit = 2;

SELECT * FROM products, sales
WHERE sales.product_id = products.id AND date = '2024-05-07'
SETTINGS log_comment = '03279_join_choose_build_table_no_idx' FORMAT Null;

SELECT * FROM sales, products
WHERE sales.product_id = products.id AND date = '2024-05-07'
SETTINGS log_comment = '03279_join_choose_build_table_no_idx' FORMAT Null;

SET mutations_sync = 2;
ALTER TABLE sales ADD INDEX date_idx date TYPE minmax GRANULARITY 1;
ALTER TABLE sales MATERIALIZE INDEX date_idx;

SELECT * FROM products, sales
WHERE sales.product_id = products.id AND date = '2024-05-07'
SETTINGS log_comment = '03279_join_choose_build_table_idx' FORMAT Null;

SELECT * FROM sales, products
WHERE sales.product_id = products.id AND date = '2024-05-07'
SETTINGS log_comment = '03279_join_choose_build_table_idx' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- condtitions are pushed down, but no filter by index applied
-- build table is as it's written in query

SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 100 AND 2000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 90_000 AND 110_000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 1000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinResultRowCount'])),
    Settings['query_plan_join_swap_table'],
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND query like '%products, sales%'
AND log_comment = '03279_join_choose_build_table_no_idx'
ORDER BY event_time DESC
LIMIT 1;

SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 90_000 AND 110_000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 100 AND 2000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 1000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinResultRowCount'])),
    Settings['query_plan_join_swap_table'],
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND query like '%sales, products%'
AND log_comment = '03279_join_choose_build_table_no_idx'
ORDER BY event_time DESC
LIMIT 1;

-- after adding index, optimizer can choose best table order

SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 100 AND 2000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 90_000 AND 110_000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 1000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinResultRowCount'])),
    Settings['query_plan_join_swap_table'],
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND query like '%products, sales%'
AND log_comment = '03279_join_choose_build_table_idx'
ORDER BY event_time DESC
LIMIT 1;

SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 100 AND 2000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 90_000 AND 110_000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 1000, 'ok', format('fail({}): {}', query_id, ProfileEvents['JoinResultRowCount'])),
    Settings['query_plan_join_swap_table'],
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND query like '%sales, products%'
AND log_comment = '03279_join_choose_build_table_idx'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS sales;
