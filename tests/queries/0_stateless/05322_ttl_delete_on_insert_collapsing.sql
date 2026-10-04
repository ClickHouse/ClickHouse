DROP TABLE IF EXISTS ttl_collapse_insert;
DROP TABLE IF EXISTS ttl_collapse_merge;

-- The live state row is inserted before the expired cancel row.
-- They share one sorting key, and the collapse removes both.
-- A live row with no cancel stays.
-- One path sets optimize_on_insert = 1.
-- The other path sets optimize_on_insert = 0, then runs OPTIMIZE TABLE ... FINAL.

CREATE TABLE ttl_collapse_insert
(
    k UInt32,
    ts DateTime,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY k
TTL ts + INTERVAL 1 DAY;

CREATE TABLE ttl_collapse_merge
(
    k UInt32,
    ts DateTime,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY k
TTL ts + INTERVAL 1 DAY;

-- Merges stay stopped so a later merge cannot change the insert result.
SYSTEM STOP MERGES ttl_collapse_insert;

SET optimize_on_insert = 1;

INSERT INTO ttl_collapse_insert VALUES
    (1, now() + INTERVAL 1 YEAR, 1),
    (1, '2000-01-01 00:00:00', -1),
    (2, now() + INTERVAL 1 YEAR, 1);

SYSTEM STOP MERGES ttl_collapse_merge;

SET optimize_on_insert = 0;

INSERT INTO ttl_collapse_merge VALUES
    (1, now() + INTERVAL 1 YEAR, 1),
    (1, '2000-01-01 00:00:00', -1),
    (2, now() + INTERVAL 1 YEAR, 1);

SYSTEM START MERGES ttl_collapse_merge;

OPTIMIZE TABLE ttl_collapse_merge FINAL;

SELECT 'on_insert', k, sign FROM ttl_collapse_insert ORDER BY k, sign;
SELECT 'after_optimize', k, sign FROM ttl_collapse_merge ORDER BY k, sign;

DROP TABLE ttl_collapse_insert;
DROP TABLE ttl_collapse_merge;
