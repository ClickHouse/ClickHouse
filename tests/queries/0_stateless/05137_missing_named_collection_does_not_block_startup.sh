#!/usr/bin/env bash
# `clickhouse local` replays the metadata of its `--path` through the same loader as the server,
# so re-entering it on one directory is a restart.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

STORE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$STORE"

$CLICKHOUSE_LOCAL --path "$STORE" --multiline -q "
CREATE NAMED COLLECTION nc_x AS url = 'http://127.0.0.1:1/none', format = 'TSV';
CREATE TABLE t_x (n UInt32) ENGINE = URL(nc_x);
SET check_named_collection_dependencies = 0;
DROP NAMED COLLECTION nc_x;
"

# The database still loads, and the table is listed as a stand-in for the storage that cannot be
# built. `data_paths` and `metadata_version` reach that storage, so they are the columns that must
# not abort the scan.
$CLICKHOUSE_LOCAL --path "$STORE" -q "SELECT name, engine, data_paths, metadata_version FROM system.tables WHERE database = currentDatabase()"

$CLICKHOUSE_LOCAL --path "$STORE" -q "SELECT name, type, data_compressed_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_x'"

# Reading the table reports the missing collection itself, not a load job that failed around it,
# so the first line of the error must be the collection error and not a wrapper of it.
$CLICKHOUSE_LOCAL --path "$STORE" -q "SELECT * FROM t_x" 2>&1 \
    | grep -m1 '^Code: 669' | grep -o -m1 'NAMED_COLLECTION_DOESNT_EXIST'

rm -rf "$STORE"
