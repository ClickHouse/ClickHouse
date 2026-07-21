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

-- RIGHT/FULL pad the left side only when unmatched rows are emitted. SEMI keeps only matched rows, so
-- RIGHT SEMI JOIN emits no default-padded l.k and GROUP BY the sharding key is still shard-local: the
-- shortcut must keep firing (shard-local result has more rows than the merged one). Checked for both
-- the analyzer (allow_experimental_analyzer = 1) and the old AST path (allow_experimental_analyzer = 0).
SELECT 'optimization still fires for RIGHT SEMI JOIN GROUP BY l.k (analyzer)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
SETTINGS allow_experimental_analyzer = 1;

SELECT 'optimization still fires for RIGHT SEMI JOIN GROUP BY l.k (old analyzer)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
SETTINGS allow_experimental_analyzer = 0;

-- LEFT SEMI keeps only matched left rows, no padding either, so the shortcut also still fires.
SELECT 'optimization still fires for LEFT SEMI JOIN GROUP BY l.k (analyzer)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
SETTINGS allow_experimental_analyzer = 1;

SELECT 'optimization still fires for LEFT SEMI JOIN GROUP BY l.k (old analyzer)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
SETTINGS allow_experimental_analyzer = 0;

-- ANTI is the opposite of SEMI: RIGHT ANTI keeps the right rows with no left match, so l.k is defaulted
-- to 0 on every shard exactly like a plain RIGHT JOIN. The shortcut must stay disabled (only Semi is
-- excluded from the padded-side guard, not Anti). Checked for both analyzer paths.
SELECT 'RIGHT ANTI JOIN GROUP BY l.k (bug_dl padded), optimize=1 equals optimize=0 (analyzer)';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT ANTI JOIN bug_dr AS r ON l.k = r.k AND l.k > 1000
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT ANTI JOIN bug_dr AS r ON l.k = r.k AND l.k > 1000
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

SELECT 'RIGHT ANTI JOIN GROUP BY l.k (bug_dl padded), optimize=1 equals optimize=0 (old analyzer)';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT ANTI JOIN bug_dr AS r ON l.k = r.k AND l.k > 1000
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT ANTI JOIN bug_dr AS r ON l.k = r.k AND l.k > 1000
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 0;

-- The join-sensitive shortcut is shared by DISTINCT and LIMIT BY, not just GROUP BY: the analyzer path
-- reaches the same guard from the projection (DISTINCT) and LIMIT BY nodes, and the old AST path from
-- select.select() and select.limitBy(). So the padded-side wrong result must be blocked for those too.
-- DISTINCT over the padded side's own sharding key (bug_dl on the RIGHT JOIN padded left) takes the
-- shortcut on every shard, defaulting l.k to 0 per shard, so without the guard it returns each key
-- twice instead of once. Checked for both analyzer paths.
SELECT 'DISTINCT l.k over RIGHT JOIN (bug_dl padded), optimize=1 equals optimize=0 (analyzer)';
SELECT groupArray(k) = (
        SELECT groupArray(k) FROM (
            SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

SELECT 'DISTINCT l.k over RIGHT JOIN (bug_dl padded), optimize=1 equals optimize=0 (old analyzer)';
SELECT groupArray(k) = (
        SELECT groupArray(k) FROM (
            SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 0;

-- LIMIT BY l.k over the same padded side keeps one row per key. Without the guard the shortcut runs it
-- per shard, so each key survives on every shard and appears twice. Checked for both analyzer paths.
SELECT 'LIMIT 1 BY l.k over RIGHT JOIN (bug_dl padded), optimize=1 equals optimize=0 (analyzer)';
SELECT groupArray((k, g)) = (
        SELECT groupArray((k, g)) FROM (
            SELECT l.k AS k, l.g AS g FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            ORDER BY l.k, l.g LIMIT 1 BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, l.g AS g FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    ORDER BY l.k, l.g LIMIT 1 BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

SELECT 'LIMIT 1 BY l.k over RIGHT JOIN (bug_dl padded), optimize=1 equals optimize=0 (old analyzer)';
SELECT groupArray((k, g)) = (
        SELECT groupArray((k, g)) FROM (
            SELECT l.k AS k, l.g AS g FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            ORDER BY l.k, l.g LIMIT 1 BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, l.g AS g FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    ORDER BY l.k, l.g LIMIT 1 BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 0;

-- DISTINCT over the foreign-column-name shape: r.k is the RIGHT side of a LEFT JOIN and merely shares
-- the sharding key name. Unmatched left rows pad r.k = 0 on every shard, so the shortcut is unsound and
-- DISTINCT would keep the duplicated 0 group. In the old AST path r.k is a qualified identifier that
-- does not match the sharding key name, so that path never took the shortcut here; only the analyzer
-- path (which resolves r.k to the sharding-key expression) needs the guard, so this case is analyzer-only.
SELECT 'DISTINCT r.k over LEFT JOIN (foreign column name), optimize=1 equals optimize=0 (analyzer)';
SELECT groupArray(k) = (
        SELECT groupArray(k) FROM (
            SELECT DISTINCT r.k AS k FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
            ORDER BY r.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT DISTINCT r.k AS k FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
    ORDER BY r.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

DROP TABLE bug_dl;
DROP TABLE bug_dr;
DROP TABLE bug_l;
DROP TABLE bug_r;
