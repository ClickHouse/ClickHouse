-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_add_duplicate_name;
DROP TABLE IF EXISTS t_projection_add_duplicate_name_replica;

CREATE TABLE t_projection_add_duplicate_name
    (a UInt64, PROJECTION p (SELECT a ORDER BY a))
ENGINE = MergeTree ORDER BY a;

-- The skipped declaration must not be analyzed or persisted.
ALTER TABLE t_projection_add_duplicate_name ADD PROJECTION IF NOT EXISTS p
    (a CODEC(UnknownCodec)) AS (SELECT a ORDER BY a);
SELECT count() = 1 FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_add_duplicate_name';

-- A plain duplicate reports its name conflict before checking the new codec.
ALTER TABLE t_projection_add_duplicate_name ADD PROJECTION p
    (a CODEC(UnknownCodec)) AS (SELECT a ORDER BY a); -- { serverError ILLEGAL_PROJECTION }
SELECT position(create_table_query, 'UnknownCodec') = 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_add_duplicate_name';

DROP TABLE t_projection_add_duplicate_name;
CREATE TABLE t_projection_add_duplicate_name (a UInt64) ENGINE = MergeTree ORDER BY a;

-- The second command sees the name added by the first command in this `ALTER`.
ALTER TABLE t_projection_add_duplicate_name
    ADD PROJECTION p (SELECT a ORDER BY a),
    ADD PROJECTION IF NOT EXISTS p (a CODEC(UnknownCodec)) AS (SELECT a ORDER BY a);
SELECT count() = 1 FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_add_duplicate_name';
SELECT position(create_table_query, 'UnknownCodec') = 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_add_duplicate_name';

-- A preceding `DROP` makes the name available again in the same statement.
ALTER TABLE t_projection_add_duplicate_name
    DROP PROJECTION p,
    ADD PROJECTION IF NOT EXISTS p (a CODEC(ZSTD)) AS (SELECT a ORDER BY a);
SELECT position(create_table_query, 'CODEC(ZSTD)') > 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_add_duplicate_name';

-- An ignored codec declaration must not trigger the shared-metadata gate when another
-- projection in the same `ALTER` is actually added to a replicated table.
CREATE TABLE t_projection_add_duplicate_name_replica
    (a UInt64, PROJECTION p (SELECT a ORDER BY a))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05325_projection_add_duplicate_name', 'r1') ORDER BY a;
ALTER TABLE t_projection_add_duplicate_name_replica
    ADD PROJECTION IF NOT EXISTS p (a CODEC(UnknownCodec)) AS (SELECT a ORDER BY a),
    ADD PROJECTION q (SELECT a ORDER BY a);
SELECT count() = 2 FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_add_duplicate_name_replica';
SELECT position(create_table_query, 'UnknownCodec') = 0 FROM system.tables
WHERE database = currentDatabase() AND name = 't_projection_add_duplicate_name_replica';

DROP TABLE t_projection_add_duplicate_name_replica;
DROP TABLE t_projection_add_duplicate_name;
