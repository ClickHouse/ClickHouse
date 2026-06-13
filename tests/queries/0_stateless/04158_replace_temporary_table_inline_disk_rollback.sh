#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-flaky-check, long
#
# Regression for clickhouse-gh[bot] review on PR #103818 (issue #63019), the
# `doCreateOrReplaceTemporaryTable` scope gap.
#
# A bare `REPLACE TEMPORARY TABLE tmp ... SETTINGS disk = disk(name = 'X', ...)` on a name
# that does NOT exist builds the storage (registering the inline custom disk X), then
# `updateExternalTable` throws `UNKNOWN_TABLE` because bare REPLACE requires the target to
# exist. The temporary-table CREATE-or-REPLACE path used to build the storage WITHOUT an
# ambient `CustomDiskRegistrationScope`, so X stayed registered in `DiskSelector` /
# `FileCacheFactory` even though no temporary table was installed. A later valid CREATE with
# the same disk name but different settings was then rejected (settings-hash mismatch) until
# server restart.
#
# The fix installs the ambient scope around `doCreateOrReplaceTemporaryTable` and commits it
# only after `addOrUpdateExternalTable` / `updateExternalTable` succeeds, so the failed bare
# REPLACE rolls the freshly-registered disk back.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DISK_BASE="${CLICKHOUSE_TEST_UNIQUE_NAME}_base"
SHARED_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_shared_cache"
REPLACE_LOG="${CLICKHOUSE_TMP}/04158_replace_${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.base_table"

# Base object-storage disk, committed via a real table so the cache disk below has a
# backing disk to wrap.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${CLICKHOUSE_DATABASE}.base_table (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${DISK_BASE}',
    type = object_storage,
    object_storage_type = local_blob_storage,
    path = './${DISK_BASE}_objstore/');"

# Bare REPLACE TEMPORARY TABLE on a name that does NOT exist. The inline cache disk X
# (settings S1) is registered while building the storage, then updateExternalTable throws
# UNKNOWN_TABLE. With the fix the ambient scope rolls X back.
${CLICKHOUSE_CLIENT} --query "
REPLACE TEMPORARY TABLE tmp_does_not_exist (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '1Mi');" >"${REPLACE_LOG}" 2>&1

echo -n "replace_rejected_unknown_table: "
grep -qE "UNKNOWN_TABLE" "${REPLACE_LOG}" && echo yes || echo no

# The disk name must be free now (no leaked tentative/committed entry). A fresh CREATE
# TEMPORARY with the SAME name X but DIFFERENT settings S2 (max_size = '2Mi') must succeed;
# if the bug had left X committed with S1, the settings-hash check would reject this with
# BAD_ARGUMENTS until restart.
${CLICKHOUSE_CLIENT} --query "
CREATE TEMPORARY TABLE tmp_fresh (a Int32) ENGINE = MergeTree() ORDER BY a
SETTINGS disk = disk(
    name = '${SHARED_NAME}',
    type = cache,
    disk = '${DISK_BASE}',
    path = './${SHARED_NAME}_data/',
    max_size = '2Mi');
INSERT INTO tmp_fresh VALUES (42);
SELECT 'fresh_temp_table', count(), sum(a) FROM tmp_fresh;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.base_table"
rm -f "${REPLACE_LOG}"
