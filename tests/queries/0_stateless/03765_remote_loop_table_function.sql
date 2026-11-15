-- Tags: no-fasttest
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/77030
-- remote() wrapping loop() used to fail because TableFunctionLoop::getActualTableStructure
-- returned empty ColumnsDescription, causing EMPTY_LIST_OF_COLUMNS_PASSED or a logical error.

DROP TABLE IF EXISTS t0_03765;

CREATE TABLE t0_03765 (c0 Int) ENGINE = Memory;

-- Empty table: should get TOO_MANY_RETRIES_TO_FETCH_PARTS (same as loop() alone with empty tables)
SELECT 1 FROM remote('localhost', loop(currentDatabase(), 't0_03765')) tx; -- { serverError TOO_MANY_RETRIES_TO_FETCH_PARTS }

INSERT INTO t0_03765 VALUES (1),(2),(3);

-- With data: selecting a constant should work
SELECT 1 FROM remote('127.0.0.1', loop(currentDatabase(), 't0_03765')) LIMIT 3;

-- With data: selecting actual columns should work
SELECT * FROM remote('127.0.0.1', loop(currentDatabase(), 't0_03765')) LIMIT 5;

DROP TABLE t0_03765 SYNC;
