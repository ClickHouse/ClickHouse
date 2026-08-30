-- Tags: shard

-- A sharding key must be deterministic for shard pruning to be forced over it, so
-- `runningConcurrency` is refused, with `allow_nondeterministic_optimize_skip_unused_shards` as the
-- escape hatch. The table still loads: the verdict is recorded on construction and only forced
-- pruning consults it. This lives apart from 04872 because `-- Tags: shard` is file-scoped.

DROP TABLE IF EXISTS events_04875;
DROP TABLE IF EXISTS dist_04875;
DROP TABLE IF EXISTS dist_sibling_04875;
DROP TABLE IF EXISTS dist_plain_04875;

CREATE TABLE events_04875 (a UInt64, s DateTime, e DateTime) ENGINE = MergeTree ORDER BY a;
INSERT INTO events_04875 VALUES (1, '2020-01-01 00:00:00', '2020-01-01 00:00:10');

CREATE TABLE dist_04875 AS events_04875
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), events_04875,
                         runningConcurrency(s, e));

-- Sibling: a running function that is already declared non-deterministic. Every row below that
-- refuses for it must refuse identically for `runningConcurrency`, which is what makes those rows
-- statements about the declaration rather than about shard pruning.
CREATE TABLE dist_sibling_04875 AS events_04875
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), events_04875,
                         rowNumberInAllBlocks() + a);

-- Negative control: a deterministic key.
CREATE TABLE dist_plain_04875 AS events_04875
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), events_04875, a);

SET optimize_skip_unused_shards = 1;
SET force_optimize_skip_unused_shards = 1;

-- The WHERE constrains both arguments of the key, so pruning is attempted and the determinism
-- verdict decides the outcome. `min(a)` rather than `count()`: the answer must not depend on how
-- many shards of the configured cluster reply.
SELECT 'carrier', min(a) FROM dist_04875
    WHERE s = '2020-01-01 00:00:00' AND e = '2020-01-01 00:00:10'; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

SELECT 'carrier old analyzer', min(a) FROM dist_04875
    WHERE s = '2020-01-01 00:00:00' AND e = '2020-01-01 00:00:10'
    SETTINGS enable_analyzer = 0; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

SELECT 'hatch', min(a) FROM dist_04875
    WHERE s = '2020-01-01 00:00:00' AND e = '2020-01-01 00:00:10'
    SETTINGS allow_nondeterministic_optimize_skip_unused_shards = 1;

SELECT 'sibling', min(a) FROM dist_sibling_04875 WHERE a = 1; -- { serverError UNABLE_TO_SKIP_UNUSED_SHARDS }

SELECT 'plain', min(a) FROM dist_plain_04875 WHERE a = 1;

DROP TABLE dist_plain_04875;
DROP TABLE dist_sibling_04875;
DROP TABLE dist_04875;
DROP TABLE events_04875;
