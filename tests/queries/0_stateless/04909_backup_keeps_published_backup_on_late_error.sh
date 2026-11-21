#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: enables a global failpoint
# no-fasttest: the archive destination needs minizip

# A backup that fails AFTER it is published must be reported as failed without being deleted.
# `.backup` (or, for an archive, the finalized archive file) is already readable at the destination
# at that point, and anything incrementally chained onto it would go with it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);
"

# $1 = human-readable destination kind, $2 = BACKUP destination expression, $3 = path under the disk.
run_case()
{
    local kind="$1" dest="$2" rel="$3"

    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT backup_fail_reading_archive_size"

    # The failure happens after publication, so it must be surfaced to the user.
    ${CLICKHOUSE_CLIENT} --query \
        "BACKUP TABLE ${CLICKHOUSE_DATABASE}.t TO $dest SETTINGS id='${CLICKHOUSE_TEST_UNIQUE_NAME}_${kind}'" 2>&1 \
        | grep -o "FAULT_INJECTED" | head -n1

    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT backup_fail_reading_archive_size"

    # ...and the published backup must still be there. RESTORE is the oracle: it only succeeds if the
    # destination was left intact, which also proves the bytes were not merely partially removed.
    ${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE IF EXISTS t_restored;
    RESTORE TABLE ${CLICKHOUSE_DATABASE}.t AS ${CLICKHOUSE_DATABASE}.t_restored FROM $dest
        SETTINGS id='${CLICKHOUSE_TEST_UNIQUE_NAME}_${kind}_restore';
    " > /dev/null

    echo "$kind restored: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_restored")"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_restored"
}

# An archive only becomes readable once it is finalized, and its size is then read back off the
# destination disk. A failure of that read must not take the published archive with it.
run_case "archive" "Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_archive.zip')" ""

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t"
