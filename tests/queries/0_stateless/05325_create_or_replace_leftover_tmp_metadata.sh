#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# On a disk without atomic renames (`plain_rewritable` object storage), `CREATE OR REPLACE` renames
# the metadata file of its temporary table by copying it and removing the source. If the server is
# killed in between, two metadata files refer to the same table, and the server must still start.

DATA_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_data"
rm -rf "$DATA_PATH"

$CLICKHOUSE_LOCAL --path "$DATA_PATH" --query "
    CREATE DATABASE db ENGINE = Atomic;
    CREATE OR REPLACE TABLE db.t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO db.t VALUES (1), (2), (3);
"

# Simulate the interrupted rename: the source file of the rename is left in place.
METADATA_DIR="$DATA_PATH/metadata/db"
cp "$METADATA_DIR/t.sql" "$METADATA_DIR/_tmp_replace_0123456789abcdef_abcdefghijklmnop.sql"

$CLICKHOUSE_LOCAL --path "$DATA_PATH" --query "
    SELECT name FROM system.tables WHERE database = 'db' ORDER BY name;
    SELECT sum(x) FROM db.t;
"

ls "$METADATA_DIR"

rm -rf "$DATA_PATH"
