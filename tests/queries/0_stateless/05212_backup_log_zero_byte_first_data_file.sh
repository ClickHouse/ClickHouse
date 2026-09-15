#!/usr/bin/env bash
# Tags: log-engine

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A Log family table decides whether it has anything to back up from the sizes its `sizes.json`
# records: bytes in any data file mean there is data to preserve, and only when no data file occupies
# bytes do the marks decide. The one column that reaches those states through SQL is an aggregate
# state that serializes to nothing, which is the carrier 05211 no longer builds, so they are
# fabricated below instead. The tables that come out of it describe rows in files that do not hold
# them, which is why each arm gets a private `clickhouse-local` path and none of this reaches a
# shared server.

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}/backups"

CONFIG="${WORK_DIR}/config.xml"
cat > "${CONFIG}" <<EOF
<clickhouse>
    <backups>
        <allowed_path>${WORK_DIR}/backups</allowed_path>
    </backups>
</clickhouse>
EOF

query() { ${CLICKHOUSE_LOCAL} --config-file "${CONFIG}" --path "$1" --query "$2"; }

# The files are found, not looked up in a system table: a stateless test may not pair a query for a
# server-side path with a file-modifying command. Each path below holds exactly one Log table when
# this runs, which is what makes the single match the right one.
table_dir() {
    local found
    found=$(find "$1" -name sizes.json)
    if [ "$(echo "${found}" | wc -w)" -ne 1 ]; then
        echo "expected exactly one Log table under $1, found: ${found}" >&2
        return 1
    fi
    dirname "${found}"
}

# The oracle is the backup's own manifest: it is written whether or not the data directory is, so a
# missing entry means the data was dropped rather than the layout moving. A file recorded at zero
# bytes is listed there but never materialised, so no data file of an all-empty column could serve.
backup_holds() {
    if [ ! -f "$1/.backup" ]; then
        echo "no backup manifest in $1, found:" >&2
        find "$1" >&2
        return 1
    fi
    python3 -c "
import re, sys
manifest = open(sys.argv[1]).read()
print(1 if re.search('<name>[^<]*/' + re.escape(sys.argv[2]) + '</name>', manifest) else 0)
" "$1/.backup" "$2"
}

# Truncate a data file and record it as empty, both together, so the table is coherent on the next
# ATTACH: the constructor runs `file_checker.repair()`, which forces every file to its recorded size,
# throwing when the file is shorter and truncating it when it is longer. Prints how many data files
# still record bytes, which is the condition the arms below separate.
fabricate_empty_data_file() {
    local dir=${1%/} file=$2
    if [ ! -f "${dir}/${file}" ]; then
        echo "expected ${file} in ${dir}, found:" >&2
        ls -A "${dir}" >&2
        return 1
    fi
    : > "${dir}/${file}"
    python3 -c "
import json, sys, urllib.parse

sizes_path, name = sys.argv[1], sys.argv[2]
with open(sizes_path) as f:
    sizes = json.load(f)
keys = [key for key in sizes['clickhouse'] if urllib.parse.unquote(key) == name]
if len(keys) != 1:
    sys.exit(f'{name} is not recorded in {sizes_path}: {sorted(sizes[\"clickhouse\"])}')
sizes['clickhouse'][keys[0]]['size'] = '0'
with open(sizes_path, 'w') as f:
    json.dump(sizes, f)

with open(sizes_path) as f:
    written = json.load(f)['clickhouse']
recorded = written[keys[0]]['size']
if recorded != '0':
    sys.exit(f'{name} records {recorded} bytes in {sizes_path} after truncating it')
print(sum(1 for key, entry in written.items()
          if not urllib.parse.unquote(key).endswith('.mrk') and entry['size'] != '0'))
" "${dir}/sizes.json" "${file}"
}

echo '-- a data file that records bytes keeps the table from classifying as empty'
query "${WORK_DIR}/db_a" "
    CREATE TABLE t (s Array(UInt64), k UInt64) ENGINE = Log;
    INSERT INTO t SELECT [], number FROM numbers(100);"
DIR=$(table_dir "${WORK_DIR}/db_a") || exit 1
RECORDING=$(fabricate_empty_data_file "${DIR}" s.size0.bin) || exit 1
printf '%s\t%s\n' 'data files that record bytes' "${RECORDING}"
# A Log table returns no rows at all once its first data file records no bytes, so `t` itself is
# unreadable now and a restore into a fresh table would be too. The target created here already holds
# a row, whose bytes in that same first data file are what make the appended rows readable. Only `k`
# is ever selected: `s` describes 100 rows in an element file that holds none.
query "${WORK_DIR}/db_a" "
    CREATE TABLE tr (s Array(UInt64), k UInt64) ENGINE = Log;
    INSERT INTO tr SELECT [], 0;"
query "${WORK_DIR}/db_a" "SELECT 'restore target sum(k) before', sum(k) FROM tr"
query "${WORK_DIR}/db_a" "BACKUP TABLE t TO File('${WORK_DIR}/backups/a')" | grep -o BACKUP_CREATED
query "${WORK_DIR}/db_a" "RESTORE TABLE t AS tr FROM File('${WORK_DIR}/backups/a') SETTINGS allow_non_empty_tables = 1" | grep -o RESTORED
query "${WORK_DIR}/db_a" "SELECT 'restored sum(k)', sum(k) FROM tr"

echo '-- no data file records bytes, and the marks say there are rows'
query "${WORK_DIR}/db_b" "
    CREATE TABLE u (s Array(UInt64)) ENGINE = Log;
    INSERT INTO u SELECT [] FROM numbers(100);"
DIR=$(table_dir "${WORK_DIR}/db_b") || exit 1
RECORDING=$(fabricate_empty_data_file "${DIR}" s.size0.bin) || exit 1
printf '%s\t%s\n' 'data files that record bytes' "${RECORDING}"
query "${WORK_DIR}/db_b" "BACKUP TABLE u TO File('${WORK_DIR}/backups/b')" | grep -o BACKUP_CREATED
# The restored column would be unreadable, so the oracle is what the backup holds.
printf '%s\t%s\n' 'backup keeps the table data' "$(backup_holds "${WORK_DIR}/backups/b" __marks.mrk)"

# TinyLog has no marks file, so `hasNothingToBackUp` has nothing to fall back on: with the first data
# file recording no bytes, only a later one that does can keep this table's data in the backup.
echo '-- TinyLog keeps no marks, so only a later data file can keep the table'
query "${WORK_DIR}/db_c" "
    CREATE TABLE w (s Array(UInt64), k UInt64) ENGINE = TinyLog;
    INSERT INTO w SELECT [], number FROM numbers(100);"
DIR=$(table_dir "${WORK_DIR}/db_c") || exit 1
RECORDING=$(fabricate_empty_data_file "${DIR}" s.size0.bin) || exit 1
printf '%s\t%s\n' 'data files that record bytes' "${RECORDING}"
query "${WORK_DIR}/db_c" "BACKUP TABLE w TO File('${WORK_DIR}/backups/c')" | grep -o BACKUP_CREATED
printf '%s\t%s\n' 'backup keeps the table data' "$(backup_holds "${WORK_DIR}/backups/c" k.bin)"

# The converse, and the only arm whose answer is that there is nothing to preserve: the second arm's
# fabrication on the engine that keeps no marks. The engine is the sole difference between the two, so
# the marks are the whole reason that table keeps its data and this one does not.
echo '-- TinyLog with no data file recording bytes has nothing left to consult'
query "${WORK_DIR}/db_d" "
    CREATE TABLE x (s Array(UInt64)) ENGINE = TinyLog;
    INSERT INTO x SELECT [] FROM numbers(100);"
DIR=$(table_dir "${WORK_DIR}/db_d") || exit 1
# The same probe for the same file name while it still records bytes, so the zero below is the
# predicate's verdict and not a name that never matches.
query "${WORK_DIR}/db_d" "BACKUP TABLE x TO File('${WORK_DIR}/backups/d_recording')" | grep -o BACKUP_CREATED
printf '%s\t%s\n' 'backup keeps the table data while a data file records bytes' "$(backup_holds "${WORK_DIR}/backups/d_recording" s.size0.bin)"
RECORDING=$(fabricate_empty_data_file "${DIR}" s.size0.bin) || exit 1
printf '%s\t%s\n' 'data files that record bytes' "${RECORDING}"
query "${WORK_DIR}/db_d" "BACKUP TABLE x TO File('${WORK_DIR}/backups/d')" | grep -o BACKUP_CREATED
printf '%s\t%s\n' 'backup keeps the table data' "$(backup_holds "${WORK_DIR}/backups/d" s.size0.bin)"

rm -rf "${WORK_DIR}"
