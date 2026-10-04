#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `TZ` is an absolute path to a symlink whose target lies outside of `TZDIR`, and `TZDIR` holds no
# byte-identical copy of that target either, so the time zone id can only be recovered from the
# path suffix. See https://github.com/ClickHouse/ClickHouse/issues/86495
SOURCE_TZ_FILE=/usr/share/zoneinfo/Europe/Amsterdam

if [ ! -f "$SOURCE_TZ_FILE" ]; then
    # There is no system time zone database to build the fixture from.
    echo "Europe/Amsterdam"
    echo "Europe/Amsterdam"
    exit 0
fi

WORK_DIR="$CLICKHOUSE_TMP/$CLICKHOUSE_TEST_UNIQUE_NAME"
rm -rf "$WORK_DIR"
# An empty time zone database: it exists, but contains neither the file nor a copy of its contents.
mkdir -p "$WORK_DIR/store/zoneinfo/Europe" "$WORK_DIR/tzdir"
# `TZ` has to be an absolute path, while `CLICKHOUSE_TMP` may be relative.
WORK_DIR=$(cd "$WORK_DIR" && pwd)

cp "$SOURCE_TZ_FILE" "$WORK_DIR/store/zoneinfo/Europe/Amsterdam"
ln -s "$WORK_DIR/store/zoneinfo/Europe/Amsterdam" "$WORK_DIR/localtime"

TZDIR="$WORK_DIR/tzdir" TZ="$WORK_DIR/localtime" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"
# The same, in the POSIX form with the colon prefix, as in the report.
TZDIR="$WORK_DIR/tzdir" TZ=":$WORK_DIR/localtime" ${CLICKHOUSE_LOCAL} --query "SELECT timezone()"

rm -rf "$WORK_DIR"
