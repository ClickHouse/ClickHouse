#!/usr/bin/env bash

# `additional_table_filters` apply to the rows read through a `*Cluster` table function the same way as through a
# plain table function, including an entry keyed by the alias of the table function.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every replica of the cluster is this server, and the URL returns the numbers 0..3.
SOURCE="urlCluster('test_cluster_one_shard_three_replicas_localhost', 'http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+number+AS+n+FROM+numbers(4)', TSV, 'n UInt64')"

# Keyed by the alias.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1'}"
$CLICKHOUSE_CLIENT -q "SELECT n FROM $SOURCE AS t ORDER BY n SETTINGS additional_table_filters = {'t': 'n > 1'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(t.n) FROM $SOURCE AS t INNER JOIN numbers(4) AS r ON t.n = r.number SETTINGS additional_table_filters = {'t': 'n > 1'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(t.n) FROM numbers(4) AS r INNER JOIN $SOURCE AS t ON r.number = t.n SETTINGS additional_table_filters = {'t': 'n > 1'}"

# Keyed by the full name of the table function.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'_table_function.urlCluster': 'n > 1'}"

# The first entry naming the table applies, as for a plain table function.
$CLICKHOUSE_CLIENT -q "SELECT sum(n) FROM $SOURCE AS t SETTINGS additional_table_filters = {'t': 'n > 1', '_table_function.urlCluster': 'n > 2'}"
