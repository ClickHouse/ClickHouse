#!/usr/bin/env bash
# A `Distributed` sharding key containing `arrayJoin` is rejected when the user states it (see
# `05175_reject_array_join_in_row_count_slots`), but a table whose sharding key was stored before that
# check existed still has to load: the sharding key is an engine argument that `ALTER` cannot change, so
# rejecting it while the metadata is read would leave `DROP` as the only way out - and would fail the
# whole database load rather than the one table. A replayed definition (a short `ATTACH TABLE`, the
# tables of an `ATTACH DATABASE`, a `Replicated` database's DDL replay, a `RESTORE`, server startup) is
# therefore accepted, and so is an unrelated `ALTER` of such a table.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# table is created with an ordinary sharding key, its stored definition is then edited into the form an
# older server would have written, and the next start loads it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

# `clickhouse-local` has no clusters of its own, so the one the engine refers to is supplied here.
cat > "${WORKING_DIR}/config.xml" <<'EOF'
<clickhouse>
    <remote_servers>
        <test_shard_localhost>
            <shard><replica><host>127.0.0.1</host><port>9000</port></replica></shard>
        </test_shard_localhost>
    </remote_servers>
</clickhouse>
EOF

LOCAL="${CLICKHOUSE_LOCAL} --path ${WORKING_DIR} --config-file ${WORKING_DIR}/config.xml"

$LOCAL -q "
CREATE DATABASE db;
CREATE TABLE db.dst (k UInt32, arr Array(UInt32)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE db.d (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', 'db', 'dst', k);
"

sed -i "s/, k)\$/, arrayJoin(arr))/" "${WORKING_DIR}/metadata/db/d.sql"
grep -c -F 'arrayJoin(arr)' "${WORKING_DIR}/metadata/db/d.sql"

# The stored definition loads, otherwise the whole database would not.
$LOCAL -q "SELECT engine_full FROM system.tables WHERE database = 'db' AND name = 'd'"

# An unrelated `ALTER` does not revalidate the stored sharding key as if the user had just stated it.
$LOCAL -q "ALTER TABLE db.d ADD COLUMN extra UInt8; SELECT 'altered'"

# Stating it now is still rejected.
$LOCAL -q "
CREATE TABLE db.d2 (k UInt32, arr Array(UInt32)) ENGINE = Distributed('test_shard_localhost', 'db', 'dst', arrayJoin(arr))
" 2>&1 >/dev/null | grep -o -m 1 -F 'ILLEGAL_COLUMN'

rm -rf "${WORKING_DIR}"
