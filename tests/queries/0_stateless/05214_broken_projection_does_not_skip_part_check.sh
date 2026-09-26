#!/usr/bin/env bash

# A broken projection does not break its part, on purpose. But the part's own consistency check used to
# be skipped whenever one of its projections was broken, which took the part's crash-corruption
# protection away with it: a marks file left empty by a power loss loads as an empty `index_granularity`,
# the row count becomes zero, and the acknowledged rows disappear from every query with nothing detached
# to recover them from. The part is detached as broken now, while a broken projection alone still is not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05214_broken_projection_does_not_skip_part_check"

create_table()
{
    local min_bytes_for_wide_part=${1:-1000000000}
    rm -rf "${WORKING_DIR}"
    mkdir -p "${WORKING_DIR}"
    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
        CREATE TABLE t (id UInt64, v UInt64, PROJECTION p (SELECT v, count() GROUP BY v))
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_bytes_for_wide_part = ${min_bytes_for_wide_part}, min_rows_for_wide_part = 1000000000, compress_marks = 1;

        INSERT INTO t SELECT number, number % 10 FROM numbers(5000);
        SELECT 'inserted', count() FROM t;
    " </dev/null
}

report()
{
    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
        SELECT '$1 rows', count() FROM t;
        SELECT '$1 detached parts', count(), any(reason) FROM system.detached_parts
        WHERE database = currentDatabase() AND table = 't';
        SELECT '$1 broken projections', countIf(is_broken) FROM system.projection_parts
        WHERE database = currentDatabase() AND table = 't';
    " </dev/null
}

# The marks of the part itself are gone as well as the projection's, which is what a single power loss
# can leave behind.
create_table
find "${WORKING_DIR}" -name 'data.cmrk4' -exec truncate -s 0 {} \;
report 'both'

# Only the projection's marks are gone: the part keeps all of its rows and stays attached.
create_table
find "${WORKING_DIR}" -path '*p.proj*' -name 'data.cmrk4' -exec truncate -s 0 {} \;
report 'projection only'

# The same, but `checksums.txt` of the part is gone as well, so it is regenerated from the files on disk
# while loading, and those checksums bless the empty marks file. The shape of the marks files is checked
# directly in this case.
create_table
find "${WORKING_DIR}" -name 'data.cmrk4' -exec truncate -s 0 {} \;
find "${WORKING_DIR}" -name 'checksums.txt' -not -path '*.proj*' -delete
report 'both, no checksums'

create_table
find "${WORKING_DIR}" -path '*p.proj*' -name 'data.cmrk4' -exec truncate -s 0 {} \;
find "${WORKING_DIR}" -name 'checksums.txt' -not -path '*.proj*' -delete
report 'projection only, no checksums'

# Intact compressed marks of a regenerated part are not mistaken for broken ones, for a Wide part too.
create_table 0
find "${WORKING_DIR}" -name 'checksums.txt' -not -path '*.proj*' -delete
report 'wide, no checksums'

rm -rf "${WORKING_DIR}"
