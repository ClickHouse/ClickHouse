#!/usr/bin/env bash
# Tags: no-async-insert
# no-async-insert: async inserts with 'implicit_transaction' are not supported

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (s String) ORDER BY ()
SETTINGS table_disk = true,
  disk = disk(
      name = 03362_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/03362/${CLICKHOUSE_DATABASE}/')
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO writer SETTINGS implicit_transaction=1 VALUES ('Hello')";

${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
