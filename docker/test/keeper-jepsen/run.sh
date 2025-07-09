#!/usr/bin/env bash
set -euo pipefail


CLICKHOUSE_PACKAGE=${CLICKHOUSE_PACKAGE:="https://clickhouse-builds.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/clickhouse_build_check/clang-18_relwithdebuginfo_none_unsplitted_disable_False_binary/clickhouse"}
CLICKHOUSE_REPO_PATH=${CLICKHOUSE_REPO_PATH:=""}


if [ -z "$CLICKHOUSE_REPO_PATH" ]; then
    CLICKHOUSE_REPO_PATH=ch
    rm -rf ch ||:
    mkdir ch ||:
    wget -nv -nd -c "https://clickhouse-test-reports.s3.amazonaws.com/$PR_TO_TEST/$SHA_TO_TEST/repo/clickhouse_no_subs.tar.gz"
    tar -C ch --strip-components=1 -xf clickhouse_no_subs.tar.gz
    ls -lath ||:
fi

cd "$CLICKHOUSE_REPO_PATH/tests/jepsen.clickhouse"

(lein run keeper test-all --nodes-file "$NODES_FILE_PATH" --username "$NODES_USERNAME" --logging-json --password "$NODES_PASSWORD" --time-limit "$TIME_LIMIT" --concurrency 50 -r 50 --snapshot-distance 100 --stale-log-gap 100 --reserved-log-items 10 --lightweight-run  --clickhouse-source "$CLICKHOUSE_PACKAGE" -q --test-count "$TESTS_TO_RUN" || true) | tee "$TEST_OUTPUT/jepsen_run_all_tests.log"

mv store "$TEST_OUTPUT/"
