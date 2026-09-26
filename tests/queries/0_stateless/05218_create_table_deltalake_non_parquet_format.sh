#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# A Delta Lake table may only hold Parquet data files, so a different data format or an outer
# compression must be rejected instead of committing data files no Delta reader can read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="SET allow_experimental_delta_kernel_rs = 1;
SET allow_experimental_delta_lake_writes = 1;
SET allow_delta_lake_create_table = 1;"

# Reports `create rejected` when the engine refused the CREATE, otherwise inserts and verifies that
# every committed data file starts with `PAR1`, the magic every Parquet file begins with.
check_variant() {
    local name=$1
    local engine_args=$2
    local table="t_dl_fmt_${name}"
    local path="${CLICKHOUSE_USER_FILES_UNIQUE}_fmt_${name}"

    rm -rf "$path"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${table}"

    if $CLICKHOUSE_CLIENT --query "
${SETTINGS}
CREATE TABLE ${table} (a Int32) ENGINE = DeltaLakeLocal('${path}', ${engine_args});
" 2>&1 | grep -q "can only contain Parquet data files"; then
        echo "${name}: create rejected"
    else
        $CLICKHOUSE_CLIENT --query "${SETTINGS} INSERT INTO ${table} VALUES (1), (2), (3);"
        local verdict="data files are parquet"
        local f magic
        for f in $(find "$path" -type f -not -path '*_delta_log*' 2>/dev/null); do
            magic=$(head -c 4 "$f" | od -An -tx1 | tr -d ' \n')
            [ "$magic" = "50415231" ] || verdict="BROKEN"
        done
        echo "${name}: ${verdict}"
        $CLICKHOUSE_CLIENT --query "${SETTINGS} SELECT a FROM ${table} ORDER BY a;"
    fi

    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${table}"
    rm -rf "$path"
}

check_variant parquet "Parquet"
check_variant csv "CSV"
check_variant gzip "Parquet, 'gzip'"

# Attaching with a non-Parquet format stays allowed, but an INSERT through it must still be rejected.
TABLE_PATH_ATTACH="${CLICKHOUSE_USER_FILES_UNIQUE}_fmt_attach"
rm -rf "$TABLE_PATH_ATTACH"

$CLICKHOUSE_CLIENT --query "
${SETTINGS}
DROP TABLE IF EXISTS t_dl_fmt_attach;
DROP TABLE IF EXISTS t_dl_fmt_attach_csv;
CREATE TABLE t_dl_fmt_attach (a Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH_ATTACH}', Parquet);
CREATE TABLE t_dl_fmt_attach_csv (a Int32) ENGINE = DeltaLakeLocal('${TABLE_PATH_ATTACH}', CSV);
"

if $CLICKHOUSE_CLIENT --query "
${SETTINGS}
INSERT INTO t_dl_fmt_attach_csv VALUES (1);
" 2>&1 | grep -q "can only contain Parquet data files"; then echo "attach: insert rejected"; else echo "attach: insert NOT rejected"; fi

$CLICKHOUSE_CLIENT --query "
DROP TABLE IF EXISTS t_dl_fmt_attach;
DROP TABLE IF EXISTS t_dl_fmt_attach_csv;
"
rm -rf "$TABLE_PATH_ATTACH"
