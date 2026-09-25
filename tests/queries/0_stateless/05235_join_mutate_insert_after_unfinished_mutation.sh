#!/usr/bin/env bash
# A mutation of a `Join` table that is committed but interrupted before its replacement file is put
# in place leaves the marker `tmp/mut.commit` holding the number the replacement will take. An
# `INSERT` that starts afterwards publishes a file with a *larger* number, so finishing the mutation
# on the next load must keep that file: `completeMutation` only removes the files numbered below the
# replacement. This is the reason the replacement takes `increment + 1` instead of rewinding to `1`.
# `clickhouse local` is used because the test looks at the files of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05235_${CLICKHOUSE_DATABASE}"
rm -rf "${workdir}"
mkdir -p "${workdir}"

# The names of the files staged in the `tmp` directory of the table, in one line.
function staged_files()
{
    find "${workdir}/store" -path "*/tmp/mut.*" | while read -r file; do basename "${file}"; done | sort | tr '\n' ' '
}

# The names of the persisted files of the table, in one line.
function persisted_files()
{
    find "${workdir}/store" -name "*.bin" -not -path "*/store/*/*/tmp/*" | while read -r file; do basename "${file}"; done | sort | tr '\n' ' '
}

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
CREATE TABLE j (id UInt64, v String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO j SELECT number, toString(number) FROM numbers(50);
INSERT INTO j SELECT number, toString(number) FROM numbers(50, 50);
SELECT 'rows', count() FROM j;
"
echo "persisted: $(persisted_files)"

# The mutation is committed - the marker holds the number `3` its replacement will take - and then
# fails before the replacement is put in place. The failure is not printed, because `--ignore-error`
# swallows it; the files and the rows show it happened. The `INSERT` that follows takes the next
# number, `4`, which is above the number the marker holds.
${CLICKHOUSE_LOCAL} --path "${workdir}" --ignore-error -q "
SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
ALTER TABLE j DELETE WHERE id < 50;
SYSTEM DISABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
SELECT 'the mutation is committed and not finished, and applied in memory', count(), min(id), max(id) FROM (SELECT id FROM j);
INSERT INTO j SELECT number, toString(number) FROM numbers(100, 25);
SELECT 'after the insert', count(), min(id), max(id) FROM (SELECT id FROM j);
"

# The replacement of the mutation is still staged, the marker is still in place, and the file of the
# insert is the only persisted one.
echo "persisted: $(persisted_files)"
echo "staged: $(staged_files)"

# The load finishes the mutation - the replacement becomes `3.bin` and the marker is removed - and
# keeps the file of the insert, so the rows of both are there.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'after a reload, the rows of the mutation and of the insert', count(), min(id), max(id) FROM (SELECT id FROM j);
"
echo "persisted: $(persisted_files)"
echo "staged: $(staged_files)"

rm -rf "${workdir}"
