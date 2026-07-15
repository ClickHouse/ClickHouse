#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database
# Transactions are not supported in Replicated and Ordinary databases.
# A non-transactional ALTER TABLE ... DROP PARTITION covers parts of not-yet-committed
# transactions. It must not persist removal_csn before creation_csn is resolved,
# otherwise version metadata validation fails with a logical error
# "creation_csn is not set while removal_csn is set".

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "drop table if exists mt"
$CLICKHOUSE_CLIENT -q "create table mt (n int) engine=MergeTree order by tuple()"

# Case 1: the creating transaction commits after the non-transactional DROP PARTITION.
tx 1 "begin transaction"
tx 1 "insert into mt settings async_insert=0 values (1)"
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'"
tx 1 "commit"
$CLICKHOUSE_CLIENT -q "select 'case 1', count() from mt"

# Case 2: the creating transaction rolls back after the non-transactional DROP PARTITION.
tx 2 "begin transaction"
tx 2 "insert into mt settings async_insert=0 values (2)"
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'"
tx 2 "rollback"
$CLICKHOUSE_CLIENT -q "select 'case 2', count() from mt"

# Case 3 (control): DROP PARTITION over a committed part still works as before.
$CLICKHOUSE_CLIENT -q "insert into mt values (3)"
$CLICKHOUSE_CLIENT -q "alter table mt drop partition id 'all'"
$CLICKHOUSE_CLIENT -q "select 'case 3', count() from mt"

# The table must stay fully usable afterwards.
$CLICKHOUSE_CLIENT -q "insert into mt values (4)"
$CLICKHOUSE_CLIENT -q "select 'case 4', count() from mt"

$CLICKHOUSE_CLIENT -q "drop table mt"
