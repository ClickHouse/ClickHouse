#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

function test_detach_distributed_table_without_pending_files() {
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"CREATE TABLE test_02536 (n UInt64) ENGINE=MergeTree() ORDER BY tuple()"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"CREATE TABLE test_dist_02536 (n UInt64) ENGINE=Distributed(test_shard_localhost, currentDatabase(), test_02536)"

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"INSERT INTO test_dist_02536 SELECT number FROM numbers(5)"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SYSTEM FLUSH DISTRIBUTED test_dist_02536"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SELECT count(n), sum(n) FROM test_dist_02536"

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"DETACH TABLE test_dist_02536"
    # TODO: check that there is no table
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"ATTACH TABLE test_dist_02536"

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SELECT count(n), sum(n) FROM test_dist_02536"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"DROP TABLE test_02536" && echo "dropped MergeTree table"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"DROP TABLE test_dist_02536" && echo "dropped Distributed table"
}

function test_detach_distributed_table_with_pending_files() {
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"CREATE TABLE test_02536 (n Int8) ENGINE=MergeTree() ORDER BY tuple()"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"CREATE TABLE test_dist_02536 (n Int8) ENGINE=Distributed(test_shard_localhost, currentDatabase(), test_02536) SETTINGS bytes_to_delay_insert=1"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SYSTEM STOP DISTRIBUTED SENDS test_dist_02536"

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"INSERT INTO test_dist_02536 SELECT number FROM numbers(5) SETTINGS prefer_localhost_replica=0"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SELECT count(n), sum(n) FROM test_dist_02536"

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"DETACH TABLE test_dist_02536"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"ATTACH TABLE test_dist_02536"

    # ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SYSTEM START DISTRIBUTED SENDS test_dist_02536"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SYSTEM FLUSH DISTRIBUTED test_dist_02536"

    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"SELECT count(n), sum(n) FROM test_dist_02536"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"DROP TABLE test_02536" && echo "dropped MergeTree table"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" -d @- <<<"DROP TABLE test_dist_02536" && echo "dropped Distributed table"
}

function run_test()
{
    echo "Start test $1"
    local test_case=$1 && shift
    echo "End test $1"

    $test_case
}

function main()
{
    run_test test_detach_distributed_table_without_pending_files
    run_test test_detach_distributed_table_with_pending_files

}
main "$@"
