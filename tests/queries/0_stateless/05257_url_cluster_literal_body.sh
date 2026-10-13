#!/usr/bin/env bash

# A constant string `body(...)` is the only request-body mode supported by `urlCluster` (a subquery
# body is rejected, see 04626_url_cluster_rejects_subquery_body). The body promotes the request to
# `POST` and is sent unchanged by every node. The server's own HTTP interface is used as the endpoint:
# it executes the `POST` body as a query, so the result proves that the body was delivered.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL="http://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}/"

# Explicit structure.
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', '${URL}', 'TSV', 'x UInt32, s String', body('SELECT 42, \'hello\''))"

# Omitted structure: the schema-inference request carries the body as well.
$CLICKHOUSE_CLIENT --query "SELECT * FROM urlCluster('test_shard_localhost', '${URL}', 'TSVWithNamesAndTypes', body('SELECT 43 AS x, \'world\' AS s FORMAT TSVWithNamesAndTypes'))"

# Several nodes: every node sends the same body.
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM urlCluster('test_cluster_two_shards_localhost', '${URL}', 'TSV', 'x UInt32', body('SELECT 7'))"
