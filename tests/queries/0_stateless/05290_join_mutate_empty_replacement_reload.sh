#!/usr/bin/env bash
# A mutation of a `Join` table that deletes every row has an empty replacement: `tmp/mut.bin`, and
# then `<mutation_id>.bin`, is a zero-length file, which `restore` skips like any empty file.
# Finishing such a committed mutation on load must accept the empty replacement - it is the table
# being empty, not a lost file - and keep the files of inserts made after the mutation.
# Both states of an interrupted mutation are covered: the replacement still staged, and already in place.
# `clickhouse local` is used because the test looks at and moves the files of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

workdir="${CLICKHOUSE_TMP}/05290_${CLICKHOUSE_DATABASE}"

# The names of the files staged in the `tmp` directory of the table, in one line.
function staged_files()
{
    find "${workdir}/store" -path "*/tmp/mut.*" | while read -r file; do basename "${file}"; done | sort | tr '\n' ' '
}

# The names and sizes of the persisted files of the table, in one line.
function persisted_files()
{
    find "${workdir}/store" -name "*.bin" -not -path "*/store/*/*/tmp/*" | while read -r file; do echo "$(basename "${file}"):$(stat -c %s "${file}" | sed 's/^[1-9][0-9]*$/non-empty/')"; done | sort | tr '\n' ' '
}

# The mutation deletes every row, is committed and then fails before its replacement is put in place;
# an insert made afterwards publishes `4.bin`.
function prepare()
{
    rm -rf "${workdir}"
    mkdir -p "${workdir}"
    ${CLICKHOUSE_LOCAL} --path "${workdir}" -q "
    CREATE TABLE j (id UInt64, v String) ENGINE = Join(ANY, LEFT, id);
    INSERT INTO j SELECT number, toString(number) FROM numbers(50);
    INSERT INTO j SELECT number, toString(number) FROM numbers(50, 50);
    "
    ${CLICKHOUSE_LOCAL} --path "${workdir}" --ignore-error -q "
    SYSTEM ENABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
    ALTER TABLE j DELETE WHERE 1;
    SYSTEM DISABLE FAILPOINT storage_join_mutate_interrupt_before_replacing_file;
    INSERT INTO j SELECT number, toString(number) FROM numbers(100, 25);
    " 2> "${workdir}/ignored_errors"
    echo "injected faults: $(grep -c -F FAULT_INJECTED "${workdir}/ignored_errors")"
    echo "persisted: $(persisted_files)"
    echo "staged: $(staged_files)"
}

function reload()
{
    ${CLICKHOUSE_LOCAL} --path "${workdir}" -q "SELECT count(), min(id), max(id) FROM j"
    echo "persisted: $(persisted_files)"
    echo "staged: $(staged_files)"
}

echo "--- staged"
prepare
reload

echo "--- in place"
prepare
# The interruption happens after the replacement is renamed into place, before the marker is removed.
mutation_dir=$(dirname "$(find "${workdir}/store" -path "*/tmp/mut.commit")")
mv "${mutation_dir}/mut.bin" "${mutation_dir}/../$(cat "${mutation_dir}/mut.commit").bin"
echo "persisted: $(persisted_files)"
echo "staged: $(staged_files)"
reload

rm -rf "${workdir}"
