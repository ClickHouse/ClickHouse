#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Legacy stored metadata can contain typed paths that have no `JSON` text deserialization.
for schema in 'JSON(x AggregateFunction(sumMap, Array(UInt64), Array(UInt64)))' 'JSON(x Map(UInt64, String))'; do
    table_uuid=$($CLICKHOUSE_CLIENT --query "SELECT generateUUIDv4()")
    $CLICKHOUSE_CLIENT --send_logs_level error --multiquery --query "
        ATTACH TABLE json_stored_metadata UUID '$table_uuid'
        (id UInt8, j $schema)
        ENGINE = MergeTree ORDER BY tuple();
        INSERT INTO json_stored_metadata (id) VALUES (7);
        SELECT id FROM json_stored_metadata;
        DETACH TABLE json_stored_metadata;
        ATTACH TABLE json_stored_metadata;
        SELECT id FROM json_stored_metadata;
        DROP TABLE json_stored_metadata SYNC;
    "
done
