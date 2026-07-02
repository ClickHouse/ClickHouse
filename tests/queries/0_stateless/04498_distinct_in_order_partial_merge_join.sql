DROP TABLE IF EXISTS t1_04498;
DROP TABLE IF EXISTS t2_04498;

CREATE TABLE t1_04498 (x UInt32, y UInt64) ENGINE = MergeTree ORDER BY (x, y);
CREATE TABLE t2_04498 (x UInt32, y UInt64) ENGINE = MergeTree ORDER BY (x, y);

-- Freeze merges so the two inserts below stay as two separate right-side parts.
-- The bug needs the right side split into more than one block; a background merge
-- collapsing them would let partial_merge read a single block and pass spuriously.
SYSTEM STOP MERGES t2_04498;

INSERT INTO t1_04498 VALUES (0,0),(1,10),(2,20),(3,30),(4,40);
INSERT INTO t2_04498 VALUES (2,21),(2,22),(4,41);
INSERT INTO t2_04498 VALUES (0,0),(4,42),(5,50);

SET optimize_distinct_in_order = 1;

SET join_algorithm = 'partial_merge';

-- intDiv(t2.y, 2147483647) maps every t2 row to key 0, so this INNER JOIN matches the single
-- t1 row (0,0) against all 6 distinct t2 rows: DISTINCT must return exactly these 6 rows.
SELECT DISTINCT t1_04498.*, t2_04498.*
FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
ORDER BY ALL;

SELECT count() FROM (
    SELECT DISTINCT t1_04498.*, t2_04498.*
    FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
);

SET join_algorithm = 'prefer_partial_merge';
SELECT count() FROM (
    SELECT DISTINCT t1_04498.*, t2_04498.*
    FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
);

-- full_sorting_merge re-sorts the left side by the join key, so it must not carry the left
-- sort property either. It must return the same 6 distinct rows.
SET join_algorithm = 'full_sorting_merge';
SELECT count() FROM (
    SELECT DISTINCT t1_04498.*, t2_04498.*
    FROM t1_04498 INNER JOIN t2_04498 ON intDiv(t2_04498.y, 2147483647) = toUInt64(t1_04498.x)
);

DROP TABLE t1_04498;
DROP TABLE t2_04498;
