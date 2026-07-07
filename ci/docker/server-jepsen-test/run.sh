#!/usr/bin/env bash
set -euo pipefail


CLICKHOUSE_PACKAGE=${CLICKHOUSE_PACKAGE:=""}
CLICKHOUSE_REPO_PATH=${CLICKHOUSE_REPO_PATH:=""}


if [ -z "$CLICKHOUSE_REPO_PATH" ]; then
    CLICKHOUSE_REPO_PATH=ch
    rm -rf ch ||:
    mkdir ch ||:
    wget -nv -nd -c "https://clickhouse-test-reports.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/repo/clickhouse_no_subs.tar.gz"
    tar -C ch --strip-components=1 -xf clickhouse_no_subs.tar.gz
    ls -lath ||:
fi

clickhouse_source="--clickhouse-source $CLICKHOUSE_PACKAGE"
if [ -n "$WITH_LOCAL_BINARY" ]; then
    clickhouse_source="--clickhouse-source /clickhouse"
fi

# $TESTS_TO_RUN comes from docker
# shellcheck disable=SC2153
tests_count="--test-count $TESTS_TO_RUN"
tests_to_run="test-all"
workload=""
if [ -n "$WORKLOAD" ]; then
    tests_to_run="test"
    workload="--workload $WORKLOAD"
    tests_count=""
fi

nemesis=""
if [ -n "$NEMESIS" ]; then
    nemesis="--nemesis $NEMESIS"
fi

rate=""
if [ -n "$RATE" ]; then
    rate="--rate $RATE"
fi

concurrency=""
if [ -n "$CONCURRENCY" ]; then
    concurrency="--concurrency $CONCURRENCY"
fi


cd "$CLICKHOUSE_REPO_PATH/tests/jepsen.clickhouse"

(lein run server $tests_to_run "$workload" --keeper "$KEEPER_NODE" "$concurrency" "$nemesis" "$rate" --nodes-file "$NODES_FILE_PATH" --username "$NODES_USERNAME" --logging-json --password "$NODES_PASSWORD" --time-limit "$TIME_LIMIT" --concurrency 50 "$clickhouse_source" "$tests_count" --reuse-binary || true) | tee "$TEST_OUTPUT/jepsen_run_all_tests.log"

mv store "$TEST_OUTPUT/"
