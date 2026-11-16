#!/usr/bin/env bash
# Tags: use_maxminddb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
mkdir -p "$CLICKHOUSE_USER_FILES_UNIQUE"
python3 - "$CUR_DIR/helpers" "$CLICKHOUSE_USER_FILES_UNIQUE" <<'PY'
import pathlib
import sys

sys.path.insert(0, sys.argv[1])
from maxminddb import Value, write_database

record = {
    'label': 'present',
    'numbers': [Value(9, i) for i in range(128)],
    'array': [{'name': 'é' * 128, 'value': Value(5, i)} for i in range(4)],
    'names': {'en': 'a' * 257, 'fr': 'b' * 513},
    'flag': True,
}
other = {'numbers': [], 'array': [], 'names': {}, 'flag': False}
for family, networks in (
    (4, [('8.8.0.0/16', record), ('1.1.1.0/24', other)]),
    (6, [('2001:db8::/32', record), ('2001:db8:1::/48', other)]),
):
    write_database(pathlib.Path(sys.argv[2]) / f'v{family}.mmdb', family, networks)
PY

table="maxminddb_batch_${CLICKHOUSE_TEST_UNIQUE_NAME}"
query()
{
    $CLICKHOUSE_CLIENT --allow_experimental_maxminddb_table_engine 1 -q "$1"
}
cleanup()
{
    query "DROP TABLE IF EXISTS ${table}_4 SYNC; DROP TABLE IF EXISTS ${table}_6 SYNC; DROP TABLE IF EXISTS ${table}_input_4 SYNC; DROP TABLE IF EXISTS ${table}_input_6 SYNC" >/dev/null
    rm -rf "$CLICKHOUSE_USER_FILES_UNIQUE"
}
trap cleanup EXIT

for family in 4 6; do
    query "CREATE TABLE ${table}_${family}
    (ip IPv${family}, label Nullable(String), numbers Array(UInt64), array Array(Tuple(name String, value UInt16)), names Map(String, String), flag Bool)
    ENGINE=MaxMindDB('$CLICKHOUSE_TEST_UNIQUE_NAME/v${family}.mmdb') SETTINGS refresh_interval='0'"
    query "CREATE TABLE ${table}_input_${family} (id UInt64, ip IPv${family}) ENGINE=MergeTree ORDER BY id SETTINGS index_granularity=2048"
    if [[ "$family" == 4 ]]; then
        expression="multiIf(number % 3=0, toIPv4(number % 65536 + 134742016), number % 3=1, toIPv4('1.1.1.1'), toIPv4('9.9.9.9'))"
    else
        expression="multiIf(number % 3=0, toIPv6(concat('2001:db8::', hex(number % 65536))), number % 3=1, toIPv6('2001:db8:1::1'), toIPv6('3001::1'))"
    fi
    query "INSERT INTO ${table}_input_${family} SELECT number, $expression FROM numbers(10000)"
    sql="SELECT count(), countIf(r.flag), sum(length(r.numbers)), sum(length(r.array)), sum(length(r.names)), sum(isNull(r.label)) FROM ${table}_input_${family} AS l LEFT ANY JOIN ${table}_${family} AS r USING ip"
    query "$sql SETTINGS join_algorithm='direct', max_threads=4, max_block_size=8192, enable_parallel_replicas=0"
    checksum="SELECT sum(cityHash64(tuple(r.*))), groupBitXor(cityHash64(tuple(r.*))) FROM ${table}_input_${family} AS l LEFT ANY JOIN ${table}_${family} AS r USING ip"
    uncached="SELECT sum(cityHash64(tuple(r.*))), groupBitXor(cityHash64(tuple(r.*))) FROM (SELECT $expression AS ip FROM numbers(10000)) AS l LEFT ANY JOIN ${table}_${family} AS r USING ip"
    expected=$(query "$uncached SETTINGS join_algorithm='direct', max_threads=1, max_block_size=64, enable_parallel_replicas=0")
    for block in 128 1024 8192; do
        for threads in 1 4; do
            actual=$(query "$checksum SETTINGS join_algorithm='direct', max_threads=$threads, max_block_size=$block, enable_parallel_replicas=0, merge_tree_min_read_task_size=1, merge_tree_min_rows_for_concurrent_read=1, merge_tree_min_bytes_for_concurrent_read=0")
            [[ "$actual" == "$expected" ]]
        done
    done
    echo "IPv${family} payloads match across blocks and threads"
    projection="SELECT sum(cityHash64(r.array.name)), sum(cityHash64(r.names.keys)), sum(cityHash64(r.label)) FROM ${table}_input_${family} AS l LEFT ANY JOIN ${table}_${family} AS r USING ip"
    uncached="SELECT sum(cityHash64(r.array.name)), sum(cityHash64(r.names.keys)), sum(cityHash64(r.label)) FROM (SELECT $expression AS ip FROM numbers(10000)) AS l LEFT ANY JOIN ${table}_${family} AS r USING ip"
    expected=$(query "$uncached SETTINGS join_algorithm='direct', max_threads=1, max_block_size=64, enable_parallel_replicas=0")
    [[ $(query "$projection SETTINGS join_algorithm='direct', max_threads=4, max_block_size=8192, enable_parallel_replicas=0") == "$expected" ]]
    echo "IPv${family} subcolumns match across blocks and threads"
done

query "SELECT count(), uniqExact(ip), sum(length(array)), sum(length(names)) FROM ${table}_4 WHERE ip IN (SELECT toIPv4(number + 134744064) FROM numbers(1000)) SETTINGS max_threads=4, max_block_size=8192, enable_parallel_replicas=0"
