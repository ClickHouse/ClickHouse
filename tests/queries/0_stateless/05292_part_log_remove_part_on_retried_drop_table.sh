#!/usr/bin/env bash
# DROP TABLE that fails to remove a part and is retried writes exactly one RemovePart per part.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

# A private instance, so the failpoint cannot fire in other tests.
${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --query "
CREATE DATABASE d ENGINE = Atomic;
CREATE TABLE d.t (x UInt64) ENGINE = MergeTree PARTITION BY x ORDER BY x
SETTINGS disk = disk(type = local_blob_storage, path = '${work_dir}/blobs/'), concurrent_part_removal_threshold = 0;
INSERT INTO d.t VALUES (1);
INSERT INTO d.t VALUES (2);
" -- --custom_local_disks_base_directory="${work_dir}/"

cat > "${work_dir}/config.xml" <<EOF
<clickhouse>
    <custom_local_disks_base_directory>${work_dir}/</custom_local_disks_base_directory>
    <database_catalog_drop_error_cooldown_sec>1</database_catalog_drop_error_cooldown_sec>
    <part_log>
        <database>system</database>
        <table>part_log</table>
    </part_log>
</clickhouse>
EOF

${CLICKHOUSE_LOCAL} --config-file "${work_dir}/config.xml" --path "${work_dir}/db" --query "
SYSTEM ENABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;
DROP TABLE d.t SYNC;
SELECT last_error_message FROM system.errors WHERE name = 'FAULT_INJECTED';
SYSTEM FLUSH LOGS part_log;
SELECT part_name, count() FROM system.part_log
WHERE database = 'd' AND table = 't' AND event_type = 'RemovePart'
GROUP BY part_name ORDER BY part_name;
"

rm -rf "${work_dir}"
