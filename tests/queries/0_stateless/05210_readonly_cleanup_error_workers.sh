#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE readonly_cleanup_error_workers (x UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS table_readonly = 1, cleanup_delay_period = 3600, max_cleanup_delay_period = 3600"

data_dir=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables
    WHERE database = currentDatabase() AND name = 'readonly_cleanup_error_workers'")
bad_dir="${data_dir}tmp_merge_cleanup_error"
mkdir "$bad_dir"
# A non-ENOENT filesystem error must propagate after the writable setting is committed.
ln -s loop "$bad_dir/loop"
touch -d '2000-01-01 UTC' "$bad_dir"
trap 'rm -f "$bad_dir/loop"; rmdir "$bad_dir"; ${CLICKHOUSE_CLIENT} --query "DROP TABLE readonly_cleanup_error_workers SYNC"' EXIT

if ${CLICKHOUSE_CLIENT} --query "ALTER TABLE readonly_cleanup_error_workers MODIFY SETTING table_readonly = 0" \
    2> "$CLICKHOUSE_TMP/readonly_cleanup_error_workers.err"; then
    echo 'Expected cleanup to fail' >&2
    exit 1
fi
grep -q 'Too many levels of symbolic links' "$CLICKHOUSE_TMP/readonly_cleanup_error_workers.err"
rm "$bad_dir/loop"

# The failed cleanup must not strand a writable table without its mutation worker.
${CLICKHOUSE_CLIENT} --multiquery --query "
INSERT INTO readonly_cleanup_error_workers VALUES (0);
ALTER TABLE readonly_cleanup_error_workers UPDATE x = 1 WHERE 1 SETTINGS mutations_sync = 0"
for _ in {1..100}; do
    value=$(${CLICKHOUSE_CLIENT} --query "SELECT x FROM readonly_cleanup_error_workers")
    if [[ "$value" == 1 ]]; then
        echo 'mutation completed after cleanup error'
        exit 0
    fi
    sleep 0.1
done

echo 'Mutation worker did not resume' >&2
exit 1
