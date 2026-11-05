#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Depends on S3

# A query over an object storage table must not wait for another query that is listing the same
# unreachable storage: it stops on its own `max_execution_time`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_unreachable;
    CREATE TABLE t_unreachable (id UInt64)
    ENGINE = S3('http://localhost:1/no-such-bucket/*.parquet', 'test', 'testtest', 'Parquet');
"

query_id_slow="${CLICKHOUSE_DATABASE}_slow_${RANDOM}${RANDOM}"

# Keeps retrying the listing of the unreachable endpoint until it is killed below.
$CLICKHOUSE_CLIENT --query_id "$query_id_slow" \
    -q "SELECT id FROM t_unreachable FORMAT Null SETTINGS max_execution_time = 120" >/dev/null 2>&1 &

for _ in {1..300}; do
    listing=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes
        WHERE query_id = '$query_id_slow' AND ProfileEvents['S3ReadRequestsErrors'] > 0")
    [ "$listing" = 1 ] && break
    sleep 0.1
done
echo "listing: $listing"

for query in "DESCRIBE TABLE t_unreachable" "SELECT id FROM t_unreachable FORMAT Null"; do
    $CLICKHOUSE_CLIENT -q "$query SETTINGS max_execution_time = 1" >/dev/null 2>&1
    # 1 = the slow query is still listing, so the query above did not wait for it.
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id_slow'"
done

$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id_slow' SYNC FORMAT Null"
wait

$CLICKHOUSE_CLIENT -q "DROP TABLE t_unreachable"
