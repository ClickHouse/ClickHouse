#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Transactions are not supported in Replicated and Ordinary databases.
# A non-transactional ALTER TABLE ... DROP PARTITION covers parts of not-yet-committed
# transactions. It used to persist removal_csn before creation_csn was resolved, failing
# version metadata validation with a logical error "creation_csn is not set while removal_csn
# is set". Now it fails close: the operation is rejected with SERIALIZATION_ERROR and the
# uncommitted data stays intact.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "drop table if exists mt"
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"

# Case 1: DROP PARTITION is rejected while an uncommitted part exists; nothing is removed,
# including the committed part in the same partition; the transaction then commits normally.
$CLICKHOUSE_CLIENT -q "insert into mt values (0)"
tx 1 "begin transaction"
tx 1 "insert into mt settings async_insert=0 values (1)"
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'" 2>&1 | grep -oE "SERIALIZATION_ERROR" | head -1
$CLICKHOUSE_CLIENT -q "select 'case 1 before commit', count() from mt"
tx 1 "commit"
$CLICKHOUSE_CLIENT -q "select 'case 1 after commit', count() from mt"

# The committed parts must not be left locked for removal by the failed DROP.
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'"
$CLICKHOUSE_CLIENT -q "select 'case 1 after drop', count() from mt"

# Case 2: same, but the creating transaction rolls back.
tx 2 "begin transaction"
tx 2 "insert into mt settings async_insert=0 values (2)"
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'" 2>&1 | grep -oE "SERIALIZATION_ERROR" | head -1
tx 2 "rollback"
$CLICKHOUSE_CLIENT -q "select 'case 2 after rollback', count() from mt"
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'"
$CLICKHOUSE_CLIENT -q "select 'case 2 after drop', count() from mt"

# Case 3 (control): DROP PARTITION over committed parts still works as before.
$CLICKHOUSE_CLIENT -q "insert into mt values (3)"
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'"
$CLICKHOUSE_CLIENT -q "select 'case 3', count() from mt"

$CLICKHOUSE_CLIENT -q "drop table mt"
