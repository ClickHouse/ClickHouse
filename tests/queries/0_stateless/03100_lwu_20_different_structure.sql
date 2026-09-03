DROP TABLE IF EXISTS testing;
SET enable_lightweight_update = 1;

CREATE TABLE testing
(
    a String,
    b String,
    c Int32,
    d Int32,
    e Int32,
)
ENGINE = MergeTree PRIMARY KEY (a)
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO testing SELECT number, number, number, number, number % 2 FROM numbers(5);

-- { echoOn }

OPTIMIZE TABLE testing FINAL;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- update all columns used by proj_1
UPDATE testing SET c = c+1, d = d+2 WHERE 1;

SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- update only one column
UPDATE testing SET d = d-1 WHERE 1;

SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- update only another one column
UPDATE testing SET c = c-1 WHERE 1;

SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- { echoOff }

DROP TABLE testing;
