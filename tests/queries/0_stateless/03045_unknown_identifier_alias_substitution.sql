-- https://github.com/ClickHouse/ClickHouse/issues/23053
SET enable_analyzer=1;
DROP TABLE IF EXISTS repl_tbl;

CREATE TEMPORARY TABLE repl_tbl
(
    `key` UInt32,
    `val_1` UInt32,
    `val_2` String,
    `val_3` String,
    `val_4` String,
    `val_5` UUID,
    `ts` DateTime
)
ENGINE = ReplacingMergeTree(ts)
ORDER BY `key`;
set prefer_column_name_to_alias = 1;
INSERT INTO repl_tbl (key) SELECT number FROM numbers(10);
WITH 10 as k SELECT k as key, * FROM repl_tbl WHERE key = k;

DROP TABLE IF EXISTS repl_tbl;
