-- Tags: zookeeper, no-replicated-database, no-ordinary-database, need-query-parameters

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier} FORMAT Null;

SET distributed_ddl_output_mode = 'throw';
SET distributed_ddl_entry_format_version = 5;
SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 0;

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier}
ENGINE = Replicated('/clickhouse/test/projection_codec_rdb/{database}_1', 'shard1', 'replica1') FORMAT Null;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t_denied
    (k UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = MergeTree ORDER BY k FORMAT Null; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM system.tables
WHERE database = currentDatabase() AND name = 't_denied';

SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 1;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t
    (k UInt64, v UInt64, PROJECTION p (v CODEC(ZSTD)) AS (SELECT k, v ORDER BY k))
ENGINE = MergeTree ORDER BY k FORMAT Null;

-- A replica loading accepted Keeper metadata must not need the initiating session's opt-in.
SET allow_projection_column_codecs_in_replicated_or_distributed_ddl = 0;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}
ENGINE = Replicated('/clickhouse/test/projection_codec_rdb/{database}', 'shard1', 'replica2') FORMAT Null;
-- Both local databases share table UUIDs, so release the first table's data directory
-- before the second replica replays its metadata.
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier} SYNC;
SYSTEM SYNC DATABASE REPLICA {CLICKHOUSE_DATABASE_1:Identifier};
SELECT count() = 1 FROM system.projections
WHERE database = {CLICKHOUSE_DATABASE_1:String} AND table = 't' AND name = 'p';
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1, 2);
SELECT sum(v) = 2 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} FORMAT Null;
