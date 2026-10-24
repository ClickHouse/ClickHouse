#!/usr/bin/env bash

# Regression test: `BACKUP` of a table with a pending `IN PARTITION` mutation persists the
# resolved partition scope of the mutation commands (the commands are pinned to the
# `IN PARTITION ID` form, exactly as in the on-disk `mutation_*.txt` file), so the scope is not
# lost after a safe partition key type change (e.g. `Enum8 -> Int8`) that makes the original
# `IN PARTITION` literal unparseable.
# `RESTORE` itself does not recreate mutations (restored parts are renamed to fresh block
# numbers and pending mutations are dropped by design), so it must succeed in this state and
# produce the unmutated data.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t_04701;
DROP TABLE IF EXISTS t_04701_restored;

CREATE TABLE t_04701 (p Enum8('a' = 1, 'b' = 2), n Int64)
ENGINE = MergeTree PARTITION BY p ORDER BY tuple();

INSERT INTO t_04701 VALUES ('a', 1), ('b', 2);

SYSTEM STOP MERGES t_04701;

ALTER TABLE t_04701 UPDATE n = n + 100 IN PARTITION 'a' WHERE 1;

ALTER TABLE t_04701 MODIFY COLUMN p Int8 SETTINGS alter_sync = 2;
"

${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t_04701 TO ${backup_name} FORMAT Null"

# The backup entry of the pending mutation carries its resolved partition scope.
backups_disk_root=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name = 'backups'")
grep --no-filename -o "IN PARTITION ID [^ ]*" "${backups_disk_root}/${CLICKHOUSE_TEST_UNIQUE_NAME}/data/${CLICKHOUSE_DATABASE}/t_04701/mutations/"*.txt | tr -d '\\'

${CLICKHOUSE_CLIENT} -m --query "
RESTORE TABLE ${CLICKHOUSE_DATABASE}.t_04701 AS t_04701_restored FROM ${backup_name} FORMAT Null;

SELECT p, n FROM t_04701_restored ORDER BY p, n;

SELECT count() FROM system.mutations
WHERE database = currentDatabase() AND table = 't_04701_restored' AND NOT is_done;

DROP TABLE t_04701_restored;
"

${CLICKHOUSE_CLIENT} -m --query "
SYSTEM START MERGES t_04701;

-- The original table still executes its pending mutation after the type change.
ALTER TABLE t_04701 UPDATE n = n IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 2;

SELECT p, n FROM t_04701 ORDER BY p, n;

DROP TABLE t_04701;
"
