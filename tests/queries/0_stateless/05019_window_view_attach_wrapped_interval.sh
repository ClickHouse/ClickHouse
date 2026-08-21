#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-catalog
# The test edits on-disk metadata to emulate a view created before interval validation.

set -e

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE=window_view_attach_wrapped_interval
DATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default'")

${CLICKHOUSE_CLIENT} --allow_experimental_window_view 1 --allow_experimental_analyzer 0 -q "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --allow_experimental_window_view 1 --allow_experimental_analyzer 0 -q "
    CREATE WINDOW VIEW ${TABLE} ENGINE = Memory
    AS SELECT count() AS c, tumble(now(), toIntervalDay(1), 'UTC') AS w FROM system.one GROUP BY w"

METADATA_PATH=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'")
METADATA_FILE="${DATA_PATH}${METADATA_PATH}"

${CLICKHOUSE_CLIENT} --allow_experimental_window_view 1 --allow_experimental_analyzer 0 -q "DETACH TABLE ${TABLE}"
grep -q 'toIntervalDay(1)' "${METADATA_FILE}"
sed -i 's/toIntervalDay(1)/toIntervalDay(2147483648)/' "${METADATA_FILE}"
grep -q 'toIntervalDay(2147483648)' "${METADATA_FILE}"

# The short form loads stored metadata. A full-definition ATTACH is CREATE-like user input and
# must remain rejected for the unsafe interval.
${CLICKHOUSE_CLIENT} --allow_experimental_window_view 1 --allow_experimental_analyzer 0 -q "ATTACH TABLE ${TABLE}"
echo attached

${CLICKHOUSE_CLIENT} --allow_experimental_window_view 1 --allow_experimental_analyzer 0 -q "DROP TABLE ${TABLE}"
