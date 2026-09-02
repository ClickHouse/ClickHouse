DROP TABLE IF EXISTS testing;

CREATE TABLE testing
(
    a String,
    b String,
    c Int32,
    d Int32,
    e Int32,
    PROJECTION proj_1
    (
        SELECT c ORDER BY d
    ),
    PROJECTION proj_2
    (
        SELECT c ORDER BY e, d
    )
)
ENGINE = MergeTree() PRIMARY KEY (a) SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO testing SELECT number, number, number, number, number%2 FROM numbers(5);

-- { echoOn }

OPTIMIZE TABLE testing FINAL;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- update all columns used by proj_1
ALTER TABLE testing UPDATE c = c+1, d = d+2 WHERE True SETTINGS mutations_sync=2;

SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;


-- update only one column
ALTER TABLE testing UPDATE d = d-1 WHERE True SETTINGS mutations_sync=2;

SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;


-- update only another one column
ALTER TABLE testing UPDATE c = c-1 WHERE True SETTINGS mutations_sync=2;

SELECT * FROM system.mutations WHERE database = currentDatabase() AND table = 'testing' AND not is_done;

SELECT c FROM testing ORDER BY d;
SELECT c FROM testing ORDER BY e, d;

-- { echoOff }

DROP TABLE testing;
