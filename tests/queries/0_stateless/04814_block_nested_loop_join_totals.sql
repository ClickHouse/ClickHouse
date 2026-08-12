-- Tags: no-old-analyzer

-- `WITH TOTALS` on either side of a block nested loop join. The totals row of the probe side is
-- joined against the build side's, and a side without totals of its own contributes its columns'
-- defaults - the same row a hash join produces for the equivalent equi join. Build-side totals are
-- stored by the single build stream that owns the totals port, which still stores every build row.

SET join_algorithm = 'direct,parallel_hash,hash';
SET max_threads = 3;
-- A swapped join would put the probe side on the right and exchange the two totals rows.
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS bnl_totals_l;
DROP TABLE IF EXISTS bnl_totals_r;

CREATE TABLE bnl_totals_l (id Int32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE bnl_totals_r (id Int32, y Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO bnl_totals_l VALUES (1, 1), (2, 2);
INSERT INTO bnl_totals_r VALUES (1, 3), (2, 4);

-- The joined totals row itself, with no probe row to match.
SELECT * FROM (SELECT id, x FROM bnl_totals_l WHERE 0) l
LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_totals_r GROUP BY id WITH TOTALS) r ON l.x < r.y;
SELECT * FROM (SELECT id, x FROM bnl_totals_l WHERE 0) l
LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_totals_r GROUP BY id WITH TOTALS) r ON l.id = r.id;
SELECT * FROM (SELECT id, sum(x) AS x FROM bnl_totals_l WHERE 0 GROUP BY id WITH TOTALS) l
LEFT JOIN bnl_totals_r r ON l.x < r.y;
SELECT * FROM (SELECT id, sum(x) AS x FROM bnl_totals_l WHERE 0 GROUP BY id WITH TOTALS) l
LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_totals_r GROUP BY id WITH TOTALS) r ON l.x < r.y;

-- Build-side totals with a probe side that does have rows: the joined rows come through next to the
-- totals row.
SELECT * FROM bnl_totals_l l
LEFT JOIN (SELECT id, sum(y) AS y FROM bnl_totals_r GROUP BY id WITH TOTALS) r ON l.x < r.y
ORDER BY ALL;

DROP TABLE bnl_totals_l;
DROP TABLE bnl_totals_r;
