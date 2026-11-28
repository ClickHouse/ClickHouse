#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# With `uuid_type_version = 2`, a bare `UUID` must be materialized into a persisted `CREATE FUNCTION` definition and
# into the predicate of a lightweight `DELETE`, which is persisted as a mutation and replayed later.

# SQL user-defined functions are server-global rather than per-database, so the names have to embed the test database.
function_uuid1="${CLICKHOUSE_DATABASE}_05355_to_uuid1"
function_uuid2="${CLICKHOUSE_DATABASE}_05355_to_uuid2"

$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $function_uuid1;
DROP FUNCTION IF EXISTS $function_uuid2;
DROP TABLE IF EXISTS t_uuid2_lightweight_delete;

SELECT 'CREATE FUNCTION';
SET uuid_type_version = 1;
CREATE FUNCTION $function_uuid1 AS s -> CAST(s, 'UUID');
SET uuid_type_version = 2;
CREATE FUNCTION $function_uuid2 AS s -> CAST(s, 'UUID');
SELECT replaceAll(create_query, currentDatabase(), 'db') FROM system.functions WHERE name IN ('$function_uuid1', '$function_uuid2') ORDER BY name;
SET uuid_type_version = 1;
SELECT toTypeName($function_uuid1('61f0c404-5cb3-11e7-907b-a6006ad3dba0')), toTypeName($function_uuid2('61f0c404-5cb3-11e7-907b-a6006ad3dba0'));

SELECT 'lightweight DELETE';
SET uuid_type_version = 2;
CREATE TABLE t_uuid2_lightweight_delete (k UInt8, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_uuid2_lightweight_delete VALUES (1, '61f0c404-5cb3-11e7-907b-a6006ad3dba0'), (2, '00000000-0000-0000-0000-000000000001');
DELETE FROM t_uuid2_lightweight_delete WHERE CAST(s, 'UUID') = toUUID2('61f0c404-5cb3-11e7-907b-a6006ad3dba0') SETTINGS lightweight_delete_mode = 'alter_update';
SELECT command FROM system.mutations WHERE database = currentDatabase() AND table = 't_uuid2_lightweight_delete';
SELECT k FROM t_uuid2_lightweight_delete ORDER BY k;

DROP TABLE t_uuid2_lightweight_delete;
DROP FUNCTION $function_uuid1;
DROP FUNCTION $function_uuid2;
"
