#!/usr/bin/env bash
# A stored sharding key with an `IN` over a non-`Set` table still loads (startup, short `ATTACH`) and survives an
# unrelated `ALTER`; only stating it now is rejected (see `05339_distributed_sharding_key_in_subquery`).
# An insert that needs the stored key, shard pruning forced by it, or the first access to a stored `AS remote(...)`
# table fails with the same error.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

cat > "${WORKING_DIR}/config.xml" <<'XML'
<clickhouse>
    <remote_servers>
        <test_shard_localhost>
            <shard><replica><host>127.0.0.1</host><port>9000</port></replica></shard>
        </test_shard_localhost>
        <test_two_shards_localhost>
            <shard><replica><host>127.0.0.1</host><port>9000</port></replica></shard>
            <shard><replica><host>127.0.0.2</host><port>9000</port></replica></shard>
        </test_two_shards_localhost>
    </remote_servers>
</clickhouse>
XML

LOCAL="${CLICKHOUSE_LOCAL} --path ${WORKING_DIR} --config-file ${WORKING_DIR}/config.xml"

$LOCAL -q "
CREATE DATABASE db;
CREATE TABLE db.dst (k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE db.d (k UInt32) ENGINE = Distributed('test_shard_localhost', 'db', 'dst', k);
CREATE TABLE db.d_two (k UInt32) ENGINE = Distributed('test_two_shards_localhost', 'db', 'dst', k);
CREATE TABLE db.t_tf (k UInt32) AS remote('127.0.0.1', 'db', 'dst', k);
"

sed -i "s/, k)\$/, k IN db.dst)/" "${WORKING_DIR}"/metadata/db/{d,d_two,t_tf}.sql
cat "${WORKING_DIR}"/metadata/db/{d,d_two,t_tf}.sql | grep -c -F 'k IN db.dst'

$LOCAL -q "SELECT engine_full FROM system.tables WHERE database = 'db' AND name = 'd'"
$LOCAL -q "DETACH TABLE db.d; ATTACH TABLE db.d; SELECT 'reattached'"
$LOCAL -q "ALTER TABLE db.d ADD COLUMN extra UInt8; SELECT 'altered'"

$LOCAL -q "INSERT INTO db.d_two VALUES (1)" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_ARGUMENTS'
$LOCAL -q "
SELECT count() FROM db.d_two WHERE k = 1
SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1, allow_nondeterministic_optimize_skip_unused_shards = 1
" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_ARGUMENTS'

$LOCAL -q "EXISTS TABLE db.t_tf"
$LOCAL -q "SELECT count() FROM db.t_tf" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_ARGUMENTS'

$LOCAL -q "
CREATE TABLE db.d2 (k UInt32) ENGINE = Distributed('test_shard_localhost', 'db', 'dst', k IN db.dst)
" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_ARGUMENTS'

rm -rf "${WORKING_DIR}"
