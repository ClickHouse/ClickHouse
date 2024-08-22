-- Tags: no-parallel

SELECT * FROM loop(numbers(3)) LIMIT 10;
SELECT * FROM loop (numbers(3)) LIMIT 10 settings max_block_size = 1;

CREATE TABLE t (n Int8) ENGINE=MergeTree ORDER BY n;
INSERT INTO t SELECT * FROM numbers(10);

SELECT * FROM loop({CLICKHOUSE_DATABASE:Identifier}.t) LIMIT 15;
SELECT * FROM loop(t) LIMIT 15;
SELECT * FROM loop({CLICKHOUSE_DATABASE:Identifier}, t) LIMIT 15;

SELECT * FROM loop('', '') -- { serverError UNKNOWN_TABLE }
