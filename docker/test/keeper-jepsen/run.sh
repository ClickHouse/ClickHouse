#!/usr/bin/env bash
set -euo pipefail


cd "$CLICKHOUSE_REPO_PATH/tests/jepsen.clickhouse-keeper"

lein run test-all --nodes-file "$NODES_FILE_PATH" --ssh-private-key "$SSH_KEY_PATH" --username "$NODES_USERNAME" --password "$NODES_PASSWORD" --time-limit "$TIME_LIMIT" --concurrency 50 -r 50 --snapshot-distance 100 --stale-log-gap 100 --reserved-log-items 10 --lightweight-run  --clickhouse-source "$CLICKHOUSE_PACKAGE" -q --test-count "$TESTS_TO_RUN" | tee "$TEST_OUTPUT/jepsen_run_all_tests.log"

mv store "$TEST_OUTPUT/"
