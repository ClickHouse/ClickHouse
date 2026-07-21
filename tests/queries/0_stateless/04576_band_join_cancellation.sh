#!/usr/bin/env bash
# Cancellation of a long-running band join probe: every interval covers every point, so each
# point row walks the whole index, and the residual condition rejects every candidate, so the
# LEFT ANTI join emits nothing until a row's walk completes. The probe must yield on its work
# budget so the executor observes KILL QUERY and max_execution_time. The join would run for
# many minutes if cancellation did not bite; the test only waits for the kill.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ct_p (t Int64, s String) ENGINE = Memory;
    CREATE TABLE ct_i (lo Int64, hi Int64, s String) ENGINE = Memory;
    INSERT INTO ct_p SELECT number, 'x' FROM numbers(300000);
    INSERT INTO ct_i SELECT 0, 2000000000, 'x' FROM numbers(300000);
"

# 9e10 candidate pairs, all rejected by the residual: zero output rows for minutes.
DEGENERATE_QUERY="
    SELECT count() FROM ct_p p LEFT ANTI JOIN ct_i i ON p.t >= i.lo AND p.t <= i.hi AND p.s <> i.s
    SETTINGS join_algorithm = 'band_join'
    FORMAT Null"

echo "timeout observed: $($CLICKHOUSE_CLIENT --max_execution_time 1 -q "$DEGENERATE_QUERY" 2>&1 | grep -c -m1 'TIMEOUT_EXCEEDED')"

QUERY_ID="04576_band_join_kill_$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -q "$DEGENERATE_QUERY" 2>&1 | grep -c -m1 'QUERY_WAS_CANCELLED' > "${CLICKHOUSE_TMP}/04576_kill.out" &
for _ in $(seq 1 3000); do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$QUERY_ID'") == 1 ]] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$QUERY_ID' SYNC FORMAT Null"
wait
echo "kill observed: $(cat "${CLICKHOUSE_TMP}/04576_kill.out")"

$CLICKHOUSE_CLIENT -q "DROP TABLE ct_p; DROP TABLE ct_i"
