-- Allow PREWHERE when Merge has DEFAULT and MergeTree has MATERIALIZED

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE m
(
    `a` String,
    `f` UInt8 DEFAULT 0
)
ENGINE = Merge(currentDatabase(), '^(t1|t2)$');

CREATE TABLE t1
(
    a String,
    f UInt8 MATERIALIZED 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;
INSERT INTO t1 (a) VALUES ('OK');

CREATE TABLE t2
(
    a String,
    f UInt8 DEFAULT 2
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192;
INSERT INTO t2 (a) VALUES ('OK');

-- { echoOn }
SELECT * FROM m PREWHERE a = 'OK' ORDER BY a, f;
SELECT * FROM m PREWHERE f = 1 ORDER BY a, f;
SELECT * FROM m WHERE f = 0 SETTINGS optimize_move_to_prewhere=0;
SELECT * FROM m WHERE f = 0 SETTINGS optimize_move_to_prewhere=1;
-- { echoOff }

DROP TABLE m;
DROP TABLE t1;
DROP TABLE t2;
