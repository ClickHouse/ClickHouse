-- Tags: no-stress
-- ^ no-stress: the Stress test wraps every query with the server-side AST fuzzer.
--   The fuzzer mutates `numbers(9)` in the INSERT into a much larger argument,
--   and combined with `index_granularity = 1` plus `INDEX idx (value1) TYPE set(10)`
--   each inserted row triggers a skip-index update, so the per-row cost explodes
--   into many minutes — the `max_execution_time` cap inside `executeASTFuzzerQueries`
--   marks the query as cancelled but the writer thread never observes it, tripping
--   the post-test hung-check.

DROP TABLE IF EXISTS t;

CREATE TABLE t
(
  key1 UInt64,
  value1 String,
  value2 String,
  INDEX idx (value1) TYPE set(10) GRANULARITY 1
)
ENGINE MergeTree ORDER BY key1 SETTINGS index_granularity = 1;

INSERT INTO t SELECT toDate('2019-10-01') + number % 3, toString(number), toString(number) from numbers(9);

SYSTEM STOP MERGES t;

SET alter_sync = 0;

ALTER TABLE t RENAME COLUMN value1 TO value11;

-- Index works without mutation applied.
SELECT * FROM t WHERE value11 = '000' SETTINGS max_rows_to_read = 0;

SYSTEM START MERGES t;

-- Another ALTER to wait for.
ALTER TABLE t RENAME COLUMN value11 TO value12 SETTINGS mutations_sync = 2;

-- Index works with mutation applied.
SELECT * FROM t WHERE value12 = '000' SETTINGS max_rows_to_read = 0;

DROP TABLE t;
