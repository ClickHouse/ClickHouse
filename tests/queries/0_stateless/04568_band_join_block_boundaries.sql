-- Tags: no-old-analyzer

-- Walks crossing many block boundaries: with a small `max_block_size` the interval side
-- arrives as many blocks, and a wide interval several blocks back from the probe position
-- must still be found while the thin-interval blocks between it and the probe are skipped
-- in O(1) via the block directory (their own max `hi` cannot admit the point).

-- Keep the written join order so the checks below exercise the orientation as written
-- instead of whatever the join order optimizer prefers.
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'band_join,hash';
SET cross_to_inner_join_rewrite = 0;
SET max_block_size = 100;

DROP TABLE IF EXISTS bb_p;
DROP TABLE IF EXISTS bb_i;

CREATE TABLE bb_p (id UInt32, t Int64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bb_i (id UInt32, lo Int64, hi Int64) ENGINE = MergeTree ORDER BY id;

-- Thin intervals [x, x+2] everywhere; every 500th is wide, [x, x+4000], so it stays
-- admissible for points dozens of blocks after the block that holds it.
INSERT INTO bb_p SELECT number, (number * 53) % 10000 FROM numbers(200);
INSERT INTO bb_i SELECT number, number, number + if(number % 500 = 0, 4000, 2) FROM numbers(10000);

SELECT 'plan', count() > 0 FROM (EXPLAIN SELECT count() FROM bb_p p JOIN bb_i i ON p.t >= i.lo AND p.t <= i.hi) WHERE explain LIKE '%BandJoin%';

SELECT 'skip blocks',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bb_p p JOIN bb_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bb_p p, bb_i i WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok,
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bb_p p JOIN bb_i i ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bb_p p JOIN bb_i i ON p.t >= i.lo AND p.t <= i.hi SETTINGS join_algorithm = 'ie_join') AS ie_parity,
    (SELECT count() FROM bb_p p JOIN bb_i i ON p.t >= i.lo AND p.t <= i.hi) AS cnt;

SELECT 'strict brackets',
    (SELECT arraySort(groupArray((p.id, i.id))) FROM bb_p p JOIN bb_i i ON p.t > i.lo AND p.t < i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id))) FROM bb_p p, bb_i i WHERE p.t > i.lo AND p.t < i.hi) AS oracle_ok,
    (SELECT count() FROM bb_p p JOIN bb_i i ON p.t > i.lo AND p.t < i.hi) AS cnt;

-- The same data over the generic comparator path (String keys): the directory skipping and
-- the boundary crossings must behave identically
SELECT 'generic path',
    (SELECT arraySort(groupArray((p.id, i.id)))
     FROM (SELECT id, leftPad(toString(t), 5, '0') AS t FROM bb_p) p
     JOIN (SELECT id, leftPad(toString(lo), 5, '0') AS lo, leftPad(toString(hi), 5, '0') AS hi FROM bb_i) i
     ON p.t >= i.lo AND p.t <= i.hi)
        = (SELECT arraySort(groupArray((p.id, i.id)))
           FROM (SELECT id, leftPad(toString(t), 5, '0') AS t FROM bb_p) p, (SELECT id, leftPad(toString(lo), 5, '0') AS lo, leftPad(toString(hi), 5, '0') AS hi FROM bb_i) i
           WHERE p.t >= i.lo AND p.t <= i.hi) AS oracle_ok;

-- A wide interval in the very first block covers everything: the across-blocks running max
-- keeps every walk alive down to block 0
SELECT 'global interval', count()
FROM bb_p p
JOIN (SELECT id, if(id = 0, 0, lo) AS lo, if(id = 0, 100000, hi) AS hi FROM bb_i) i
ON p.t >= i.lo AND p.t <= i.hi;

DROP TABLE bb_p;
DROP TABLE bb_i;
