#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# ^ the Vortex format is not included in the fast test and MSan builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Set` table can grow while a query that reads it runs, and the `in` above the scan checks the
# live set. A snapshot of it pushed into a Vortex scan could therefore drop rows the final filter
# would accept, so such an `IN` is left to ClickHouse. An `IN` over a literal set, whose content
# is fixed, is still pushed down. The `ProfileEvents` of the pushdown show which one happened; each
# case runs in its own process, as `system.events` accumulates over the process.

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05260_vortex_in_mutable_set_XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

LOCAL=(${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}")

DATA_DIR="${LOCAL_DIR}/user_files"
mkdir -p "$DATA_DIR"

"${LOCAL[@]}" --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.vortex', Vortex)
    SELECT number AS n FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1;
"

EVENTS="
    SELECT
        sumIf(value, event = 'VortexFilterPushdownConjunctsPushed'),
        sumIf(value, event = 'VortexFilterPushdownConjunctsDropped')
    FROM system.events"

echo "-- an IN over a Set table is not pushed down"
"${LOCAL[@]}" --enable_analyzer=1 --query "
    CREATE TABLE keys (k UInt64) ENGINE = Set;
    INSERT INTO keys VALUES (1), (5), (7);
    SELECT count() FROM file('${DATA_DIR}/data.vortex', Vortex) WHERE n IN keys;
    ${EVENTS};
"

echo "-- an IN over a literal set is pushed down"
"${LOCAL[@]}" --enable_analyzer=1 --query "
    SELECT count() FROM file('${DATA_DIR}/data.vortex', Vortex) WHERE n IN (1, 5, 7);
    ${EVENTS};
"
