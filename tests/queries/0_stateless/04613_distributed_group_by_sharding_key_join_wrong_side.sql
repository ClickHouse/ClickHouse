-- Tags: distributed, long
-- long: many distributed JOIN self-checks are slow under the debug flaky check's 180s cap.
-- Regression for optimize_distributed_group_by_sharding_key returning duplicate unmerged
-- groups when GROUP BY references a column from the other side of a distributed JOIN that
-- merely shares the sharding key's name (issue #111087, silent wrong result).

DROP TABLE IF EXISTS bug_l;
DROP TABLE IF EXISTS bug_r;
DROP TABLE IF EXISTS bug_dl;
DROP TABLE IF EXISTS bug_dr;

CREATE TABLE bug_l (k UInt32, g UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE bug_r (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO bug_l SELECT number, number % 3 FROM numbers(40);
INSERT INTO bug_r SELECT number FROM numbers(40);

CREATE TABLE bug_dl (k UInt32, g UInt32) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), bug_l, k);
CREATE TABLE bug_dr (k UInt32) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), bug_r, k);

SET distributed_product_mode = 'global', optimize_skip_unused_shards = 1;

-- GROUP BY r.k is over the RIGHT side of the LEFT JOIN. Unmatched left rows are padded r.k = 0
-- on every shard, so the r.k = 0 group spans shards and the shard-local aggregation shortcut is
-- unsound: it must NOT fire even though r.k shares the left table's sharding key name k.
SELECT 'correct result (GROUP BY r.k, optimize=1)';
SELECT r.k, count(), max(l.k) FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
    GROUP BY r.k ORDER BY ALL LIMIT 4
    SETTINGS optimize_distributed_group_by_sharding_key = 1;

-- Self-check: with the shortcut on, the GROUP BY r.k result must match the non-optimized one.
SELECT 'optimize=1 equals optimize=0 for GROUP BY r.k';
SELECT groupArray((k, c, m)) = (
        SELECT groupArray((k, c, m)) FROM (
            SELECT r.k AS k, count() AS c, max(l.k) AS m FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
            GROUP BY r.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT r.k AS k, count() AS c, max(l.k) AS m FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
    GROUP BY r.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1);

-- GROUP BY l.k is over the distributed table's actual sharding key, so the shortcut is still
-- allowed. Data is inserted directly into both shards' local tables, so a shard-local shortcut
-- returns unmerged per-shard groups (more rows) than the fully merged result: this proves the
-- optimization still fires for the legitimate case and was not disabled by the fix.
SELECT 'optimization still fires for GROUP BY l.k (sharding key)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0));

-- The shortcut is also unsound when the distributed table is itself on an outer-join padded side,
-- even when GROUP BY is over its OWN sharding key: unmatched rows default the key to 0 on every
-- shard, so that group spans shards. RIGHT JOIN pads the left (bug_dl) side.
SELECT 'RIGHT JOIN GROUP BY l.k (bug_dl padded), optimize=1 equals optimize=0';
SELECT groupArray((k, c, m)) = (
        SELECT groupArray((k, c, m)) FROM (
            SELECT l.k AS k, count() AS c, max(r.k) AS m FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, count() AS c, max(r.k) AS m FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1);

-- FULL JOIN pads both sides.
SELECT 'FULL JOIN GROUP BY l.k (bug_dl padded), optimize=1 equals optimize=0';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT l.k AS k, count() AS c FROM bug_dl AS l FULL JOIN bug_dr AS r ON l.k = r.k AND l.k > 29 AND r.k > 29
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, count() AS c FROM bug_dl AS l FULL JOIN bug_dr AS r ON l.k = r.k AND l.k > 29 AND r.k > 29
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1);

-- The padded table can also sit under a CROSS/comma join: `bug_dl CROSS JOIN one RIGHT JOIN bug_dr`
-- builds JoinNode(RIGHT){CrossJoinNode[bug_dl, one], bug_dr}, so bug_dl is on the padded left side even
-- though it is reached through a CrossJoinNode. The shortcut must still not fire.
SET joined_subquery_requires_alias = 0;
SELECT 'CROSS JOIN under RIGHT JOIN GROUP BY l.k (bug_dl padded), optimize=1 equals optimize=0';
SELECT groupArray((k, c, m)) = (
        SELECT groupArray((k, c, m)) FROM (
            SELECT l.k AS k, count() AS c, max(r.k) AS m FROM bug_dl AS l CROSS JOIN system.one AS s RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, count() AS c, max(r.k) AS m FROM bug_dl AS l CROSS JOIN system.one AS s RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1);

-- A plain CROSS JOIN with no outer join does not pad, so GROUP BY the sharding key must still take the
-- shortcut (fires): the shard-local result has more rows than the merged one.
SELECT 'optimization still fires for plain CROSS JOIN GROUP BY l.k (sharding key)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l CROSS JOIN system.one AS s
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l CROSS JOIN system.one AS s
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0));

DROP TABLE bug_dl;
DROP TABLE bug_dr;
DROP TABLE bug_l;
DROP TABLE bug_r;
