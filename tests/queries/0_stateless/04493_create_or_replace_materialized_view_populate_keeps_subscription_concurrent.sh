#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# no-ordinary-database: CREATE OR REPLACE MATERIALIZED VIEW requires an Atomic database.
# no-replicated-database: POPULATE is not supported in a Replicated database.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/108726
# `CREATE OR REPLACE MATERIALIZED VIEW ... POPULATE` used to leave the new view unsubscribed from
# its source table, so every row inserted after the replace was silently dropped. This is the
# concurrent variant of 04492: the source is hammered with inserts while the replace is in progress
# (`merge_tree_storage_snapshot_sleep_ms` widens the replace's snapshot window so the inserts
# reliably overlap it), and once the replace has completed the new view must still be subscribed.
#
# We prove the subscription is live with a sentinel row inserted *after* everything settles: it must
# reach the view. We deliberately do not assert anything about the rows inserted *during* the
# replace: the source-to-view dependency transfer inside the internal `EXCHANGE` is not yet atomic,
# so a row inserted in that narrow window can be missed, and an insert that lands exactly mid-swap
# can even fail outright with `UNKNOWN_TABLE` (the new view's target table is momentarily
# unreachable). Closing that window is a separate change. This test covers only the deterministic
# fix: the new view stays subscribed once the replace completes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS mv SYNC"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO src SELECT number FROM numbers(5000)"

# The view must already exist so that CREATE OR REPLACE actually replaces it (EXCHANGE), not just creates it.
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src"

# Hammer the source with inserts while the replace is in progress. An insert that lands in the brief
# instant the replace swaps the tables can transiently fail with `UNKNOWN_TABLE` (the new view's
# target table is momentarily unreachable, because the source-to-view dependency transfer inside the
# internal `EXCHANGE` is not yet atomic). That residual window is out of scope for this test (see the
# note above), so we tolerate that specific error on the concurrent inserts; any other insert failure
# still fails the test. The contract we assert is the sentinel below, after everything has settled.
(
    for j in $(seq 0 9); do
        if ! err=$(${CLICKHOUSE_CLIENT} --query "INSERT INTO src SELECT number FROM numbers(100000 + ${j} * 1000, 1000)" 2>&1); then
            echo "$err" | grep -qF "UNKNOWN_TABLE" || { echo "$err" >&2; exit 1; }
        fi
    done
) &
inserts_pid=$!

${CLICKHOUSE_CLIENT} --merge_tree_storage_snapshot_sleep_ms=150 --query "CREATE OR REPLACE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src"

# A concurrent insert may have hit the tolerated residual window above; any other failure fails here.
wait "$inserts_pid"

# The crucial part: once the (concurrent) replace has completed, the new view must still be
# subscribed to its source - a sentinel row inserted now must reach the view. On the buggy version
# the view was detached by the replace, so this row would never arrive and the count would be 0.
${CLICKHOUSE_CLIENT} --query "INSERT INTO src VALUES (999999999)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM mv WHERE id = 999999999"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mv SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE src SYNC"
