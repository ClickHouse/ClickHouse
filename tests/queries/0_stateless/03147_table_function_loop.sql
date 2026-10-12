-- Tags: no-parallel
-- Disable force_primary_key_reverse_order: tests loop table function, output depends on MergeTree table ordering
SET force_primary_key_reverse_order = 0;

SELECT * FROM loop(numbers(3)) LIMIT 10;
SELECT * FROM loop (numbers(3)) LIMIT 10 settings max_block_size = 1;

CREATE TABLE t (n Int8) ENGINE=MergeTree ORDER BY n;

SELECT * FROM loop(t) LIMIT 15; -- { serverError TOO_MANY_RETRIES_TO_FETCH_PARTS }

INSERT INTO t SELECT * FROM numbers(10);

SELECT * FROM loop({CLICKHOUSE_DATABASE:Identifier}.t) LIMIT 15;
SELECT * FROM loop(t) LIMIT 15;
SELECT * FROM loop({CLICKHOUSE_DATABASE:Identifier}, t) LIMIT 15;

SELECT * FROM loop('', '') -- { serverError UNKNOWN_TABLE }
