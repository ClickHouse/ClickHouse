#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: enables a global failpoint

# Regression for the '.lock' verification read of a backup writer being served from the userspace
# page cache. Companion to 04763, which covers the mmap cache; this covers the second carrier.
# BackupImpl::checkLockFile re-reads the lock file this process just wrote. The page cache is keyed
# by path plus a file_version that local disks never populate, so after a failed attempt's lock at
# the same path was removed, the retry's read returned the previous attempt's UUID and the retry
# aborted with BACKUP_ALREADY_EXISTS even though no concurrent backup existed.
#
# clickhouse-local is required: page_cache_max_size is a server setting that defaults to 0 and no
# file under tests/config/ sets it, so the stateless server has no page cache to poison.
# Disk(...) is also required: BackupWriterFile::readFile calls createReadBufferFromFileBase
# directly, bypassing the ReadPipeline that owns the page-cache stage, so File(...) cannot reach it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups" "${WORK_DIR}/local"

CONFIG_FILE="${WORK_DIR}/config.yaml"
> "${CONFIG_FILE}" echo "
page_cache_max_size: 134217728
storage_configuration:
  disks:
    backups:
      type: local
      path: \"${WORK_DIR}/backups/\"
backups:
  allowed_disk: backups
"

QUERIES_FILE="${WORK_DIR}/queries.sql"
# A single process is required: the page cache is process-local, so the failing attempt and the
# retry must share one clickhouse-local run. --queries-file is required too: a multi-statement
# --query aborts at the first error, so the retry would never run.
> "${QUERIES_FILE}" echo "
CREATE TABLE t (x Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t VALUES (1), (2), (3);
BACKUP TABLE t TO Disk('backups', 'base') FORMAT Null;

SYSTEM ENABLE FAILPOINT backup_fail_before_writing_metadata;
-- The first attempt fails and its cleanup removes the lock file, leaving its contents in the cache.
BACKUP TABLE t TO Disk('backups', 'inc') SETTINGS base_backup = Disk('backups', 'base'); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT backup_fail_before_writing_metadata;

-- The retry writes a fresh lock file at the same path with the same length. Verifying it must
-- observe the file just written, not the contents cached for the previous attempt.
BACKUP TABLE t TO Disk('backups', 'inc') SETTINGS base_backup = Disk('backups', 'base');

-- The page cache must actually have been populated, otherwise the assertion above would pass for
-- want of a page cache rather than because the fix works.
SELECT if(value > 0, 'page cache populated', 'PAGE CACHE EMPTY') FROM system.metrics WHERE metric = 'PageCacheBytes';
"

${CLICKHOUSE_LOCAL} --config-file "${CONFIG_FILE}" --path "${WORK_DIR}/local" \
    --use_page_cache_for_local_disks 1 \
    --local_filesystem_read_method pread \
    --queries-file "${QUERIES_FILE}" < /dev/null 2>&1 \
    | grep -oE "BACKUP_CREATED|BACKUP_ALREADY_EXISTS|page cache populated|PAGE CACHE EMPTY"

rm -rf "${WORK_DIR}"
