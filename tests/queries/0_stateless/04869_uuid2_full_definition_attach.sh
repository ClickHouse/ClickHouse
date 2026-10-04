#!/usr/bin/env bash
# A full-definition `ATTACH TABLE` is create-like user input, so under `uuid_type_version = 2` it must
# materialize a bare `UUID` (including persisted type-string carriers such as `DEFAULT CAST(..., 'UUID')`)
# exactly like CREATE does, both locally and through the modern `ATTACH ... ON CLUSTER` path.
# The short `ATTACH TABLE t` syntax replays stored metadata and must keep the persisted types.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate random table UUIDs to avoid collisions in Atomic databases.
TABLE_UUID_LOCAL=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
TABLE_UUID_CLUSTER=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
TABLE_UUID_V1=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")

# `SET send_logs_level = 'fatal'` suppresses the "full table definition is not recommended" warning
# (the harness already passes --send_logs_level on the client command line).
$CLICKHOUSE_CLIENT -q "
SET send_logs_level = 'fatal';
SELECT '-- Full-definition ATTACH materializes bare UUID as UUID2, like CREATE';
SET uuid_type_version = 2;
DROP TABLE IF EXISTS t_uuid2_attach;
ATTACH TABLE t_uuid2_attach UUID '${TABLE_UUID_LOCAL}'
(
    id UUID,
    id_explicit UUID2,
    id_legacy UUID1,
    arr Array(UUID),
    s String,
    d UUID DEFAULT CAST(s, 'UUID')
) ENGINE = Memory;
SHOW CREATE TABLE t_uuid2_attach;
DROP TABLE t_uuid2_attach;
"

$CLICKHOUSE_CLIENT -q "
SET send_logs_level = 'fatal';
SELECT '-- ATTACH ... ON CLUSTER (modern DDL format) materializes on the initiator';
SET uuid_type_version = 2;
SET distributed_ddl_output_mode = 'none';
DROP TABLE IF EXISTS t_uuid2_attach_cluster ON CLUSTER test_shard_localhost SYNC;
ATTACH TABLE t_uuid2_attach_cluster UUID '${TABLE_UUID_CLUSTER}' ON CLUSTER test_shard_localhost
(
    id UUID,
    s String,
    d UUID DEFAULT CAST(s, 'UUID')
) ENGINE = Memory;
SHOW CREATE TABLE t_uuid2_attach_cluster;
DROP TABLE t_uuid2_attach_cluster ON CLUSTER test_shard_localhost SYNC;
"

$CLICKHOUSE_CLIENT -q "
SET send_logs_level = 'fatal';
SELECT '-- Short ATTACH replays stored metadata and does not rewrite types';
DROP TABLE IF EXISTS t_uuid2_reattach;
SET uuid_type_version = 1;
CREATE TABLE t_uuid2_reattach (id UUID, d UUID DEFAULT CAST('00000000-0000-0000-0000-000000000000', 'UUID')) ENGINE = Memory;
SET uuid_type_version = 2;
DETACH TABLE t_uuid2_reattach;
ATTACH TABLE t_uuid2_reattach;
SHOW CREATE TABLE t_uuid2_reattach;
DROP TABLE t_uuid2_reattach;
"

$CLICKHOUSE_CLIENT -q "
SET send_logs_level = 'fatal';
SELECT '-- Under version 1 a full-definition ATTACH keeps the historical UUID';
SET uuid_type_version = 1;
DROP TABLE IF EXISTS t_uuid1_attach;
ATTACH TABLE t_uuid1_attach UUID '${TABLE_UUID_V1}' (id UUID, d UUID DEFAULT CAST(toString(id), 'UUID')) ENGINE = Memory;
SHOW CREATE TABLE t_uuid1_attach;
DROP TABLE t_uuid1_attach;
"
