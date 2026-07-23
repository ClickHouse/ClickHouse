-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

DROP TABLE IF EXISTS t_read_in_order_limit;

CREATE TABLE t_read_in_order_limit (
    Id String,
    Document JSON(TypedField String),
    Payload String DEFAULT repeat('x', 200)
) ENGINE = MergeTree
ORDER BY Id
SETTINGS index_granularity = 8192,
    min_bytes_for_wide_part = '128G',
    write_marks_for_substreams_in_compact_parts = 1;

SYSTEM STOP MERGES t_read_in_order_limit;

INSERT INTO t_read_in_order_limit (Id, Document) SELECT
    leftPad(toString(number * 2), 10, '0'),
    concat('{"ScopeIds": ["aaa", "all"], "TypedField": "v', toString(number), '"}')
FROM numbers(50_000);

INSERT INTO t_read_in_order_limit (Id, Document) SELECT
    leftPad(toString(number * 2 + 1), 10, '0'),
    concat('{"ScopeIds": ["aaa", "all"], "TypedField": "v', toString(number), '"}')
FROM numbers(50_000);

INSERT INTO t_read_in_order_limit (Id, Document) SELECT
    leftPad(toString(number * 3), 10, '0'),
    concat('{"ScopeIds": ["aaa", "all"], "TypedField": "v', toString(number), '"}')
FROM numbers(50_000);

INSERT INTO t_read_in_order_limit (Id, Document) SELECT
    leftPad(toString(number * 3), 10, '1'),
    concat('{"ScopeIds": ["aaa", "all"], "TypedField": "v', toString(number), '"}')
FROM numbers(50_000);

SET query_plan_optimize_lazy_materialization = 0;

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

SELECT * FROM t_read_in_order_limit ORDER BY Id ASC LIMIT 101
FORMAT Null SETTINGS log_comment = 'test_04201_no_filter';

SELECT * FROM t_read_in_order_limit ORDER BY Id ASC LIMIT 101
FORMAT Null SETTINGS log_comment = 'test_04201_no_filter_vrow', read_in_order_use_virtual_row = 1;

SELECT * FROM t_read_in_order_limit
WHERE hasAny(Document.ScopeIds, ['aaa', 'all'])
ORDER BY Id ASC LIMIT 101
FORMAT Null SETTINGS log_comment = 'test_04201_with_filter', read_in_order_use_virtual_row = 0;


SELECT * FROM t_read_in_order_limit
WHERE hasAny(Document.ScopeIds, ['aaa', 'all'])
ORDER BY Id ASC LIMIT 101
FORMAT Null SETTINGS log_comment = 'test_04201_with_filter_vrow', read_in_order_use_virtual_row = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    if(read_rows <= 8192 * expected_granules, 'Ok', format('Fail: {} rows read in query {}', read_rows, query_id)),
    if(Settings['read_in_order_use_virtual_row'] == '1', 3, 4) as expected_granules
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday()
  AND event_time >= (SELECT ts FROM start_ts)
  AND log_comment like 'test_04201%'
  AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_read_in_order_limit;
