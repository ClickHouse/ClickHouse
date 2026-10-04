#!/usr/bin/env bash

# A clustered read from `url` with a `body(...)` and an automatic format must not send the body once
# more on the initiator just to detect the format again after the structure was inferred: the
# initiator infers the structure and the format from the same request(s), like the plain `url` does.
# The server's own HTTP interface is used as the endpoint: it executes the `POST` body as a query, so
# `system.query_log` counts the requests. Format detection may send several requests (one per tried
# format), so the counts are compared with the plain `url` instead of being pinned: the clustered read
# repeats the detection on the worker, which gives `2 * plain - 1` requests (one read request only).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

function body()
{
    echo "body('SELECT 1 AS x /* ${CLICKHOUSE_DATABASE}_$1 */ FORMAT JSONEachRow')"
}

$CLICKHOUSE_CLIENT --query "SELECT * FROM url('${URL}', $(body plain))"
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', '${URL}', $(body cluster))"
$CLICKHOUSE_CLIENT --query "SELECT * FROM url('${URL}', $(body replicas)) SETTINGS enable_parallel_replicas = 1, cluster_for_parallel_replicas = 'test_shard_localhost', parallel_replicas_for_cluster_engines = 1"
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', '${URL}', 'JSONEachRow', 'x UInt8', $(body explicit))"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"

function requests()
{
    echo "(SELECT count() FROM system.query_log WHERE event_date >= yesterday() AND type = 'QueryFinish' AND interface = 2 AND query LIKE '%/* ${CLICKHOUSE_DATABASE}_$1 */%')"
}

$CLICKHOUSE_CLIENT --query "
    SELECT
        $(requests plain) > 1,
        $(requests cluster) = 2 * $(requests plain) - 1,
        $(requests replicas) = 2 * $(requests plain) - 1,
        $(requests explicit)"
