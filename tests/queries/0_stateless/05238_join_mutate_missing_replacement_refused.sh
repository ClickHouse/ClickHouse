#!/usr/bin/env bash
# Once a mutation of a `Join` table is committed - the marker `tmp/mut.commit` is in place - the files
# it replaces may already be removed, and its replacement is the only durable copy of the table.
# Finishing the mutation on load must be fail-closed: if the replacement is neither staged in
# `tmp/mut.bin` nor in place as `<mutation_id>.bin`, the load throws and leaves every file alone,
# instead of clearing the marker and silently loading whatever is left.
# `clickhouse local` is used because the test looks at and removes the files of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05238_${CLICKHOUSE_DATABASE}"
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
"

# The mutation is committed and then fails before its replacement is put in place, and an insert
# made afterwards publishes `4.bin`.
${CLICKHOUSE_LOCAL} --path "${workdir}" --ignore-error -q "
SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
ALTER TABLE j DELETE WHERE id < 50;
SYSTEM DISABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
INSERT INTO j SELECT number, toString(number) FROM numbers(100, 25);
"
echo "persisted: $(persisted_files)"
echo "staged: $(staged_files)"

# The staged replacement is lost, so the committed mutation cannot be finished.
find "${workdir}/store" -path "*/tmp/mut.bin" -delete
echo "staged: $(staged_files)"

# The load refuses to finish the mutation.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "SELECT count() FROM j" 2>&1 | grep -o -m1 "CORRUPTED_DATA"

# Nothing was touched: the marker and the file of the insert are still there.
echo "persisted: $(persisted_files)"
echo "staged: $(staged_files)"

rm -rf "${workdir}"
