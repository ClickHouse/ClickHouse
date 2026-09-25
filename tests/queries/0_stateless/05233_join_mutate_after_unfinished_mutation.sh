#!/usr/bin/env bash
# A mutation of a `Join` table that fails after it is committed - the marker is in place, the swap of
# the files is not finished - leaves the table in the mutated state in memory, while the only durable
# copy of that state is the replacement staged in `tmp/mut.bin`. The next mutation stages its own
# replacement in the same file. If it then failed before its own commit, the marker of the first
# mutation pointed at the snapshot of the second, and the load published it under the number of the
# first: the rows the second mutation deleted, which was never committed, were lost. The first
# mutation is now finished before the second stages anything. `clickhouse local` is used because the
# test looks at the files of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05233_${CLICKHOUSE_DATABASE}"
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

# The first mutation is committed and fails before its replacement is put in place; the second fails
# before it is committed. The failures are not printed, because `--ignore-error` swallows them; the
# files and the rows show they happened. Only the first is applied in memory: the second fails before
# the point where the table is changed.
${CLICKHOUSE_LOCAL} --path "${workdir}" --ignore-error -q "
SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
ALTER TABLE j DELETE WHERE id < 50;
SYSTEM DISABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
SELECT 'the first mutation is committed and not finished, and applied in memory', count(), min(id), max(id) FROM (SELECT id FROM j);
SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_commit;
ALTER TABLE j DELETE WHERE id >= 75;
SELECT 'the second mutation fails before it is committed, and the table is as it was', count(), min(id), max(id) FROM (SELECT id FROM j);
"

# The first mutation was finished before the second staged its replacement: its file is published,
# the marker is gone, and only the uncommitted snapshot of the second is left staged.
echo "persisted: $(persisted_files)"
echo "staged: $(staged_files)"

# The load recovers the rows the first mutation kept - including the ones the second, never
# committed, mutation deleted - and does not touch the uncommitted snapshot.
${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
SELECT 'after a reload, the rows of the committed mutation', count(), min(id), max(id) FROM (SELECT id FROM j);
"
echo "persisted: $(persisted_files)"

rm -rf "${workdir}"
