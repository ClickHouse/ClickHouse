-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_projection_codec_order_r2;
DROP TABLE IF EXISTS t_projection_codec_order_r1;
DROP TABLE IF EXISTS t_projection_codec_order_copy;
DROP TABLE IF EXISTS t_projection_codec_order_local;

SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 1;
CREATE TABLE t_projection_codec_order_r1
(
    a UInt64,
    b UInt64,
    PROJECTION p (a CODEC(ZSTD), b CODEC(NONE)) AS (SELECT a, b ORDER BY a)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05323_projection_codec_order', 'r1') ORDER BY a;

-- The replica has the same name-to-codec mapping in a different declaration order.
CREATE TABLE t_projection_codec_order_r2
(
    a UInt64,
    b UInt64,
    PROJECTION p (b CODEC(NONE), a CODEC(ZSTD)) AS (SELECT a, b ORDER BY a)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05323_projection_codec_order', 'r2') ORDER BY a;

INSERT INTO t_projection_codec_order_r1 SELECT number, number + 1 FROM numbers(10);
SYSTEM SYNC REPLICA t_projection_codec_order_r2;
SELECT sum(b) = 55 FROM t_projection_codec_order_r2;
SELECT position(create_table_query, 'CODEC(NONE)') > 0
    AND position(create_table_query, 'CODEC(ZSTD)') > position(create_table_query, 'CODEC(NONE)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_order_r2';

DETACH TABLE t_projection_codec_order_r2;
ATTACH TABLE t_projection_codec_order_r2;
SELECT position(create_table_query, 'CODEC(NONE)') > 0
    AND position(create_table_query, 'CODEC(ZSTD)') > position(create_table_query, 'CODEC(NONE)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_order_r2';

SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 0;
CREATE TABLE t_projection_codec_order_local (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a;
ALTER TABLE t_projection_codec_order_local ADD PROJECTION p
    (a CODEC(ZSTD), b CODEC(NONE)) AS (SELECT a, b ORDER BY a);

-- Restating the same writer codecs in another order may change only the settings.
ALTER TABLE t_projection_codec_order_local MODIFY PROJECTION p
    (b CODEC(NONE), a CODEC(ZSTD)) AS (SELECT a, b ORDER BY a)
    WITH SETTINGS (index_granularity = 128);
SELECT position(create_table_query, 'CODEC(ZSTD)') > 0
    AND position(create_table_query, 'CODEC(NONE)') > position(create_table_query, 'CODEC(ZSTD)')
    AND position(create_table_query, 'index_granularity = 128') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_order_local';

-- Changing the codec assigned to either name must still be rejected.
ALTER TABLE t_projection_codec_order_local MODIFY PROJECTION p
    (a CODEC(NONE), b CODEC(ZSTD)) AS (SELECT a, b ORDER BY a)
    WITH SETTINGS (index_granularity = 256); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_projection_codec_order_copy AS t_projection_codec_order_local
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_projection_codec_order_local SELECT number, number + 1 FROM numbers(10);
INSERT INTO t_projection_codec_order_copy SELECT * FROM t_projection_codec_order_local;
SELECT position(create_table_query, 'CODEC(ZSTD)') > 0
    AND position(create_table_query, 'CODEC(NONE)') > position(create_table_query, 'CODEC(ZSTD)')
    AND position(create_table_query, 'index_granularity = 128') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 't_projection_codec_order_copy';
SELECT countDistinct(table) = 2 FROM system.projection_parts_columns
WHERE database = currentDatabase()
    AND table IN ('t_projection_codec_order_local', 't_projection_codec_order_copy')
    AND name = 'p' AND active AND column = 'a';

DROP TABLE t_projection_codec_order_copy;
DROP TABLE t_projection_codec_order_local;
DROP TABLE t_projection_codec_order_r2;
DROP TABLE t_projection_codec_order_r1;
