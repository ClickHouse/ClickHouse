#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# An explicit `use_fake_transaction=1` on a `cas` disk would silently break the
# atomic manifest/ref publish (per-file autocommit, no commit point for the transaction). The disk
# factory must reject it at CREATE TABLE time with BAD_ARGUMENTS instead of silently corrupting
# writes later -- mirrors the existing missing-`server_root_id` fail-close handling.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_cas_reject_fake_transaction;
CREATE TABLE t_cas_reject_fake_transaction (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    server_root_id = '05015',
    name = '05015_cas_reject_fake_transaction',
    path = '05015_cas_reject_fake_transaction_pool/',
    use_fake_transaction = 1);
" 2>&1 | grep -cm1 "use_fake_transaction. cannot be enabled for metadata type"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE name = 't_cas_reject_fake_transaction'"
