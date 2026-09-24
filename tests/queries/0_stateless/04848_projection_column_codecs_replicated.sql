-- Tags: zookeeper

-- { echo ON }

DROP TABLE IF EXISTS t_repl_codecs_r1;
DROP TABLE IF EXISTS t_repl_codecs_r2;

-- `ReplicatedMergeTreeTableMetadata::checkAndFindDiff` compares a replica's definition against the
-- ZooKeeper one, re-parsed and re-serialized, so the replicas below agree only if the codec-argument
-- substitution is deterministic.
CREATE TABLE t_repl_codecs_r1
(
    x Int64,
    y Int64,
    PROJECTION p
    (
        x BIGINT CODEC(Delta, ZSTD(1)),
        y BIGINT
    )
    AS
    (
        SELECT x, y ORDER BY x
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_repl_codecs', 'r1') ORDER BY x
SETTINGS min_bytes_for_wide_part = 0;

-- The second replica declares canonical types and the already-substituted codec. `BIGINT`/`Int64`
-- and `Delta`/`Delta(8)` must converge on the same stored definition, including the type-only `y`,
-- or this `CREATE` fails with a metadata mismatch.
CREATE TABLE t_repl_codecs_r2
(
    x Int64,
    y Int64,
    PROJECTION p
    (
        x Int64 CODEC(Delta(8), ZSTD(1)),
        y Int64
    )
    AS
    (
        SELECT x, y ORDER BY x
    )
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_repl_codecs', 'r2') ORDER BY x
SETTINGS min_bytes_for_wide_part = 0;

-- Both replicas must report the same definition, in the substituted form.
SHOW CREATE TABLE t_repl_codecs_r1;
SHOW CREATE TABLE t_repl_codecs_r2;

SELECT name, codecs FROM system.projections
WHERE database = currentDatabase() AND table LIKE 't_repl_codecs_%'
ORDER BY table;

INSERT INTO t_repl_codecs_r1 SELECT number, number FROM numbers(100000);
SYSTEM SYNC REPLICA t_repl_codecs_r2;

SELECT count() FROM t_repl_codecs_r2;

-- Decode the projection from the fetched part on the second replica.
SELECT sum(x), sum(y) FROM t_repl_codecs_r2
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p';

-- An `ALTER` that leaves projections alone still re-derives each one from its stored definition,
-- whose codec arguments are already substituted. That must not substitute a second time.
ALTER TABLE t_repl_codecs_r1 ADD COLUMN z UInt64;
SYSTEM SYNC REPLICA t_repl_codecs_r2;
SHOW CREATE TABLE t_repl_codecs_r2;

DROP TABLE t_repl_codecs_r1;
DROP TABLE t_repl_codecs_r2;
