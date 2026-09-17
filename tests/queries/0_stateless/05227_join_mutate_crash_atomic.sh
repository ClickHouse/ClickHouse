#!/usr/bin/env bash
# `ALTER TABLE ... DELETE` on a `Join` table replaces every persisted file with one holding the rows
# it kept, which takes more than one step. A server that died in between - a kill, an OOM, a crash -
# left the table with whichever of the old files happened to survive, while the replacement, staged in
# a directory the load does not read, was ignored: rows the mutation never even matched were silently
# gone. `clickhouse local` is used because the test looks at the files of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05227_${CLICKHOUSE_DATABASE}"
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

# Up to the commit of the mutation, a failure leaves the table exactly as it was: nothing has been
# published or removed yet, so the rows are all there, and the next insert takes the next number
# instead of reusing one of the persisted files. The failure of the mutation is not printed, because
# `--ignore-error` swallows it; that the rows are all still there shows it happened.
${CLICKHOUSE_LOCAL} --path "${workdir}" --ignore-error -q "
SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_commit;
ALTER TABLE j DELETE WHERE id < 50;
SELECT 'the mutation fails before it is committed, and the table is as it was', count(), min(id), max(id) FROM (SELECT id FROM j);
INSERT INTO j SELECT number, toString(number) FROM numbers(100, 10);
SELECT 'and the insert after it is in memory', count(), min(id), max(id) FROM (SELECT id FROM j);
"
echo "persisted: $(persisted_files)"
echo "the replacement is staged but not committed: $(staged_files)"
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'and on disk', count(), min(id), max(id) FROM (SELECT id FROM j);
ALTER TABLE j DELETE WHERE id >= 100;
SELECT 'back to', count(), min(id), max(id) FROM (SELECT id FROM j);
"
echo "persisted: $(persisted_files)"

# The failpoint stops the mutation where a crash used to lose the data: the old files are gone and the
# replacement is still staged.
echo -n 'the mutation is interrupted mid-swap: '
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
ALTER TABLE j DELETE WHERE id < 50;
" 2>&1 | grep -c -m1 FAULT_INJECTED

echo "the replacement is staged and the window is marked: $(staged_files)"

${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'the rows the mutation kept are there', count(), min(id), max(id) FROM (SELECT id FROM j);
"

echo "and nothing is left staged: $(staged_files)"
echo "persisted: $(persisted_files)"

# A mutation that is not interrupted leaves the same table behind, and it survives a reload.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
ALTER TABLE j DELETE WHERE id < 75;
SELECT 'after a mutation that completes', count(), min(id), max(id) FROM (SELECT id FROM j);
"
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'and after a reload', count(), min(id), max(id) FROM (SELECT id FROM j);
"
echo "persisted: $(persisted_files)"

rm -rf "${workdir}"
