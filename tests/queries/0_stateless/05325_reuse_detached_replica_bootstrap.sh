#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# The test creates two local replicas and stages files in their local data directories.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS bootstrap_destination SYNC;
        DROP TABLE IF EXISTS bootstrap_source SYNC;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE bootstrap_source (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05325_seed', 'source') ORDER BY x;
    SYSTEM STOP MERGES bootstrap_source;
    INSERT INTO bootstrap_source VALUES (111);
    INSERT INTO bootstrap_source VALUES (222);
    INSERT INTO bootstrap_source VALUES (333);
    SYSTEM SYNC REPLICA bootstrap_source;
    SYSTEM STOP REPLICATED SENDS bootstrap_source;
    CREATE TABLE bootstrap_destination (x UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05325_seed', 'destination') ORDER BY x
    SETTINGS max_postpone_time_for_failed_replicated_tasks_ms = 0;
    SYSTEM STOP FETCHES bootstrap_destination;
    SYSTEM STOP MERGES bootstrap_destination;
"

source_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'bootstrap_source'")
destination_path=$(${CLICKHOUSE_CLIENT} --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = 'bootstrap_destination'")
part=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'bootstrap_source' AND active ORDER BY name LIMIT 1")
other_part=$(${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'bootstrap_source' AND active ORDER BY name LIMIT 1 OFFSET 1")

mkdir -p "${destination_path}/detached"
# Publish only a complete candidate, as when downloading a part from a backup.
cp -a "${source_path}/${part}" "${destination_path}/detached/tmp_${part}"
mv "${destination_path}/detached/tmp_${part}" "${destination_path}/detached/${part}"
cp -a "${source_path}/${other_part}" "${destination_path}/detached/tmp_${other_part}"
python3 - "${destination_path}/detached/tmp_${other_part}" <<'PY'
from pathlib import Path
import sys

part_file = next(Path(sys.argv[1]).glob("*.bin"))
with part_file.open("r+b") as file:
    byte = file.read(1)
    assert byte
    file.seek(0)
    file.write(bytes([byte[0] ^ 1]))
PY
mv "${destination_path}/detached/tmp_${other_part}" "${destination_path}/detached/${other_part}"
${CLICKHOUSE_CLIENT} --query "SYSTEM START FETCHES bootstrap_destination"

# The source cannot send any part. The seeded part must become active through local attachment.
attached=0
for _ in $(seq 1 120); do
    attached=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.parts
        WHERE database = currentDatabase() AND table = 'bootstrap_destination' AND active AND name = '${part}'")
    if [[ "$attached" == 1 ]]; then
        break
    fi
    sleep 0.1
done
if [[ "$attached" != 1 ]]; then
    ${CLICKHOUSE_CLIENT} --query "SELECT type, new_part_name, last_exception FROM system.replication_queue WHERE database = currentDatabase() AND table = 'bootstrap_destination'"
    exit 1
fi
echo "attached_without_peer"

# The corrupt candidate and the part missing from the backup must be fetched normally.
${CLICKHOUSE_CLIENT} --multiquery --query "
    SYSTEM START REPLICATED SENDS bootstrap_source;
    SYSTEM SYNC REPLICA bootstrap_destination;
    SELECT 'data', groupArray(x) FROM (SELECT x FROM bootstrap_destination ORDER BY x);
    SELECT 'same_parts',
        (SELECT groupArray(name) FROM (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'bootstrap_source' AND active ORDER BY name))
        =
        (SELECT groupArray(name) FROM (SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'bootstrap_destination' AND active ORDER BY name));
    SELECT 'seed_consumed', count() = 0 FROM system.detached_parts
        WHERE database = currentDatabase() AND table = 'bootstrap_destination' AND name = '${part}';
    SELECT 'corrupt_quarantined', count() = 1 FROM system.detached_parts
        WHERE database = currentDatabase() AND table = 'bootstrap_destination' AND name = 'broken_${other_part}';
"
