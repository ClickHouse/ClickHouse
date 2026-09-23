-- Tags: distributed, long
-- long: many distributed JOIN self-checks are slow under the debug flaky check's 180s cap.
-- Regression for optimize_distributed_group_by_sharding_key returning duplicate unmerged
-- groups when GROUP BY references a column from the other side of a distributed JOIN that
-- merely shares the sharding key's name (issue #111087, silent wrong result).

DROP TABLE IF EXISTS bug_l;
DROP TABLE IF EXISTS bug_r;
DROP TABLE IF EXISTS bug_dl;
DROP TABLE IF EXISTS bug_dr;
DROP TABLE IF EXISTS bug_sk;
DROP TABLE IF EXISTS bug_dsk;

CREATE TABLE bug_l (k UInt32, g UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE bug_r (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO bug_l SELECT number, number % 3 FROM numbers(40);
INSERT INTO bug_r SELECT number FROM numbers(40);

CREATE TABLE bug_dl (k UInt32, g UInt32) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), bug_l, k);
CREATE TABLE bug_dr (k UInt32) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), bug_r, k);

-- Second fixture, for issue #111274: the sharding key is an EXPRESSION over k, and the query
-- self-joins the distributed table on a column unrelated to it.
CREATE TABLE bug_sk (k UInt32, g UInt16, n Int64, lc String) ENGINE = MergeTree ORDER BY k;
INSERT INTO bug_sk SELECT number, number % 5, number, toString(number % 4) FROM numbers(60);
CREATE TABLE bug_dsk (k UInt32, g UInt16, n Int64, lc String) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), bug_sk, intHash64(k));

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

-- FULL JOIN pads both sides. Two conditions keep this arm about padding: the join must run on the
-- shards, since a broadcast right side has the grouping column resolved to it instead, and the ON
-- condition must not equate the sharding key, for the same reason. Relax either and the arm still
-- passes without any padded-side check at all.
SELECT 'FULL JOIN GROUP BY l.k (bug_dl padded, shard-local join), optimize=1 equals optimize=0';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT l.k AS k, count() AS c FROM bug_dl AS l FULL JOIN bug_dr AS r ON l.g = r.k % 3 AND r.k > 29
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0, distributed_product_mode = 'local'))
FROM (
    SELECT l.k AS k, count() AS c FROM bug_dl AS l FULL JOIN bug_dr AS r ON l.g = r.k % 3 AND r.k > 29
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1, distributed_product_mode = 'local');

-- The padded table can also sit under a CROSS/comma join: in `bug_dl CROSS JOIN one RIGHT JOIN bug_dr`
-- the RIGHT JOIN pads everything reached through the cross join, so bug_dl is still on a padded side
-- and the shortcut must not fire.
SET joined_subquery_requires_alias = 0;
SELECT 'CROSS JOIN under RIGHT JOIN GROUP BY l.k (bug_dl padded, shard-local join), optimize=1 equals optimize=0';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT l.k AS k, count() AS c FROM bug_dl AS l CROSS JOIN system.one AS s RIGHT JOIN bug_dr AS r ON l.g = r.k % 3 AND r.k > 29
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0, distributed_product_mode = 'local'))
FROM (
    SELECT l.k AS k, count() AS c FROM bug_dl AS l CROSS JOIN system.one AS s RIGHT JOIN bug_dr AS r ON l.g = r.k % 3 AND r.k > 29
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1, distributed_product_mode = 'local');

-- A plain CROSS JOIN with no outer join does not pad, so GROUP BY the sharding key must still take the
-- shortcut (fires): the shard-local result has more rows than the merged one.
SELECT 'optimization still fires for plain CROSS JOIN GROUP BY l.k (sharding key)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l CROSS JOIN system.one AS s
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l CROSS JOIN system.one AS s
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0));

-- RIGHT SEMI emits no default-padded l.k, so the padded-side guard exempts it by strictness. This
-- shape is still blocked, by the provenance check: the analyzer resolves `l.k` to the join's right
-- column, which from bug_dl's side is a foreign column. Blocking is conservative rather than
-- required here, since that column is bug_dr's own sharding key, but it is not unsound, and without
-- any guard this shape returns duplicate unmerged groups.
SELECT 'RIGHT SEMI JOIN GROUP BY l.k, optimize=1 equals optimize=0';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

-- LEFT SEMI keeps only matched left rows and pads nothing, and here `l.k` stays bug_dl's own column,
-- so the shortcut must keep firing (shard-local result has more rows than the merged one). This is
-- what shows the SEMI exemption was not over-broadened into a blanket block on SEMI joins.
SELECT 'optimization still fires for LEFT SEMI JOIN GROUP BY l.k';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l LEFT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
SETTINGS allow_experimental_analyzer = 1;

-- The same exemption for RIGHT SEMI, where the kind alone would say "padded". A shard-local join
-- keeps l.k as bug_dl's own column, so the strictness carve-out is the only thing letting the
-- shortcut fire here; drop it and this count stops growing.
SELECT 'optimization still fires for RIGHT SEMI JOIN GROUP BY l.k (shard-local join)';
SELECT
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1, distributed_product_mode = 'local'))
    >
    (SELECT count() FROM (SELECT l.k FROM bug_dl AS l RIGHT SEMI JOIN bug_dr AS r ON l.k = r.k
        GROUP BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0, distributed_product_mode = 'local'));

-- ANTI is the opposite of SEMI: RIGHT ANTI keeps the right rows with no left match, so l.k is defaulted
-- to 0 on every shard exactly like a plain RIGHT JOIN. The shortcut must stay disabled (only Semi is
-- excluded from the padded-side guard, not Anti).
SELECT 'RIGHT ANTI JOIN GROUP BY l.k (bug_dl padded), optimize=1 equals optimize=0';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT ANTI JOIN bug_dr AS r ON l.k = r.k AND l.k > 1000
            GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, count() AS c FROM bug_dl AS l RIGHT ANTI JOIN bug_dr AS r ON l.k = r.k AND l.k > 1000
    GROUP BY l.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

-- The join-sensitive shortcut is shared by DISTINCT and LIMIT BY, not just GROUP BY: the same guard is
-- reached from the projection (DISTINCT) and LIMIT BY nodes. So the padded-side wrong result must be
-- blocked for those too. DISTINCT over the padded side's own sharding key (bug_dl on the RIGHT JOIN
-- padded left) takes the shortcut on every shard, defaulting l.k to 0 per shard, so without the guard
-- it returns each key twice instead of once.
SELECT 'DISTINCT l.k over RIGHT JOIN (bug_dl padded), optimize=1 equals optimize=0';
SELECT groupArray(k) = (
        SELECT groupArray(k) FROM (
            SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

-- The same DISTINCT call site, on the shape where the padded-side check is the blocker rather than
-- the foreign-column one, so the two guards are covered separately here too.
SELECT 'DISTINCT l.k over RIGHT JOIN (bug_dl padded, shard-local join), optimize=1 equals optimize=0';
SELECT groupArray(k) = (
        SELECT groupArray(k) FROM (
            SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.g = r.k % 3 AND r.k > 29
            ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0, distributed_product_mode = 'local'))
FROM (
    SELECT DISTINCT l.k AS k FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.g = r.k % 3 AND r.k > 29
    ORDER BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1, distributed_product_mode = 'local');

-- LIMIT BY l.k over the same padded side keeps one row per key. Without the guard the shortcut runs it
-- per shard, so each key survives on every shard and appears twice.
SELECT 'LIMIT 1 BY l.k over RIGHT JOIN (bug_dl padded), optimize=1 equals optimize=0';
SELECT groupArray((k, g)) = (
        SELECT groupArray((k, g)) FROM (
            SELECT l.k AS k, l.g AS g FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
            ORDER BY l.k, l.g LIMIT 1 BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT l.k AS k, l.g AS g FROM bug_dl AS l RIGHT JOIN bug_dr AS r ON l.k = r.k AND r.k > 29
    ORDER BY l.k, l.g LIMIT 1 BY l.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

-- DISTINCT over the foreign-column-name shape: r.k is the RIGHT side of a LEFT JOIN and merely shares
-- the sharding key name. Unmatched left rows pad r.k = 0 on every shard, so the shortcut is unsound and
-- DISTINCT would keep the duplicated 0 group.
SELECT 'DISTINCT r.k over LEFT JOIN (foreign column name), optimize=1 equals optimize=0';
SELECT groupArray(k) = (
        SELECT groupArray(k) FROM (
            SELECT DISTINCT r.k AS k FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
            ORDER BY r.k SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT DISTINCT r.k AS k FROM bug_dl AS l LEFT JOIN bug_dr AS r ON l.k = r.k AND l.k > 29
    ORDER BY r.k SETTINGS optimize_distributed_group_by_sharding_key = 1)
SETTINGS allow_experimental_analyzer = 1;

-- Outer-join padding is not what makes the shortcut unsound: a foreign grouping column breaks it under
-- an INNER JOIN too. The join condition `l.g = r.k % 3` is not an equality on the sharding key, so each
-- r.k matches left rows whose k (the sharding key) lies on both shards, and that r.k group spans shards.
SELECT 'INNER JOIN GROUP BY r.k, join condition unrelated to the sharding key, optimize=1 equals optimize=0';
SELECT groupArray((k, c)) = (
        SELECT groupArray((k, c)) FROM (
            SELECT r.k AS k, count() AS c FROM bug_dl AS l INNER JOIN bug_dr AS r ON l.g = r.k % 3
            GROUP BY r.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0))
FROM (
    SELECT r.k AS k, count() AS c FROM bug_dl AS l INNER JOIN bug_dr AS r ON l.g = r.k % 3
    GROUP BY r.k ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1);

-- Issue #111274: the grouping expression is the foreign column under an injective wrapper, so
-- matching the sharding key by name alone still accepted it and every group came back duplicated.
-- The sharding key is the expression intHash64(k) and the self-join is on the unrelated g.
-- Both settings that consult the sharding key are pinned on each side rather than left to the test
-- runner's randomisation: with optimize_distributed_group_by_sharding_key = 0 this shape never
-- reaches the shortcut, and the assertion would then hold on an unguarded build too.
SELECT 'GROUP BY negate(r.k) over an expression-sharded self join (#111274), optimize=1 equals optimize=0';
SELECT groupArray((nk, c)) = (
        SELECT groupArray((nk, c)) FROM (
            SELECT negate(r.k) AS nk, countDistinctIf(l.n, r.lc IN ('1', '3')) AS c FROM bug_dsk AS l INNER JOIN bug_dsk AS r ON l.g = r.g
            GROUP BY negate(r.k) ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 0, optimize_skip_unused_shards = 1))
FROM (
    SELECT negate(r.k) AS nk, countDistinctIf(l.n, r.lc IN ('1', '3')) AS c FROM bug_dsk AS l INNER JOIN bug_dsk AS r ON l.g = r.g
    GROUP BY negate(r.k) ORDER BY ALL SETTINGS optimize_distributed_group_by_sharding_key = 1, optimize_skip_unused_shards = 1);

-- The same shape through the other setting that consults the sharding key, which is the one the
-- report toggles.
SELECT 'GROUP BY negate(r.k) over an expression-sharded self join (#111274), skip_unused_shards=1 equals =0';
SELECT groupArray((nk, c)) = (
        SELECT groupArray((nk, c)) FROM (
            SELECT negate(r.k) AS nk, countDistinctIf(l.n, r.lc IN ('1', '3')) AS c FROM bug_dsk AS l INNER JOIN bug_dsk AS r ON l.g = r.g
            GROUP BY negate(r.k) ORDER BY ALL SETTINGS optimize_skip_unused_shards = 0, optimize_distributed_group_by_sharding_key = 1))
FROM (
    SELECT negate(r.k) AS nk, countDistinctIf(l.n, r.lc IN ('1', '3')) AS c FROM bug_dsk AS l INNER JOIN bug_dsk AS r ON l.g = r.g
    GROUP BY negate(r.k) ORDER BY ALL SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1);

DROP TABLE bug_dl;
DROP TABLE bug_dr;
DROP TABLE bug_dsk;
DROP TABLE bug_l;
DROP TABLE bug_r;
DROP TABLE bug_sk;
