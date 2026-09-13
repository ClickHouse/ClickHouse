#!/usr/bin/env bash
# `ALTER TABLE ... DELETE` on a `Join` table replaces every persisted file with one holding the rows
# it kept, which takes more than one step. A server that died in between - a kill, an OOM, a crash -
# left the table with whichever of the old files happened to survive, while the replacement, staged in
# a directory the load does not read, was ignored: rows the mutation never even matched were silently
# gone. `clickhouse local` is used because the test looks at the files of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05189_${CLICKHOUSE_DATABASE}"
rm -rf "${workdir}"
mkdir -p "${workdir}"

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
CREATE TABLE j (id UInt64, v String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO j SELECT number, toString(number) FROM numbers(100);
SELECT 'rows', count() FROM j;
"

# The failpoint stops the mutation where a crash used to lose the data: the old files are gone and the
# replacement is still staged.
echo -n 'the mutation is interrupted mid-swap: '
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
ALTER TABLE j DELETE WHERE id < 50;
" 2>&1 | grep -c -m1 FAULT_INJECTED

echo -n 'the replacement is staged and the window is marked: '
find "${workdir}/store" -path "*/tmp/mut.*" -printf '%f\n' | sort | tr '\n' ' '
echo

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'the rows the mutation kept are there', count(), min(id), max(id) FROM (SELECT id FROM j);
"

echo -n 'and nothing is left staged: '
find "${workdir}/store" -path "*/tmp/mut.*" -printf '%f\n' | wc -l

# A mutation that is not interrupted leaves the same table behind, and it survives a reload.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
ALTER TABLE j DELETE WHERE id < 75;
SELECT 'after a mutation that completes', count(), min(id), max(id) FROM (SELECT id FROM j);
"
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'and after a reload', count(), min(id), max(id) FROM (SELECT id FROM j);
"

rm -rf "${workdir}"
