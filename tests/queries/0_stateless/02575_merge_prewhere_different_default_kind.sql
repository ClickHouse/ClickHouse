-- Prohibit PREWHERE when Merge and MergeTree has different default type of the column

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE m
(
    a String,
    date Date,
    f UInt8
)
ENGINE = Merge(currentDatabase(), '^(t1|t2)$');

CREATE TABLE t1
(
    a String,
    date Date,
    f UInt8 ALIAS 0
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;
INSERT INTO t1 (a) VALUES ('OK');

-- { echoOn }
-- for pure PREWHERE it is not addressed yet.
SELECT * FROM m PREWHERE a = 'OK';
SELECT * FROM m PREWHERE f = 0; -- { serverError ILLEGAL_PREWHERE }
SELECT * FROM m WHERE f = 0 SETTINGS optimize_move_to_prewhere=0;
SELECT * FROM m WHERE f = 0 SETTINGS optimize_move_to_prewhere=1;
-- { echoOff }

CREATE TABLE t2
(
    a String,
    date Date,
    f UInt8,
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;
INSERT INTO t2 (a) VALUES ('OK');

-- { echoOn }
SELECT * FROM m WHERE f = 0 SETTINGS optimize_move_to_prewhere=1;
-- { echoOff }
