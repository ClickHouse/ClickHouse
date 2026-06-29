#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# no-ordinary-database: CREATE OR REPLACE MATERIALIZED VIEW requires an Atomic database.
# no-replicated-database: POPULATE is not supported in a Replicated database.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/108726
# `CREATE OR REPLACE MATERIALIZED VIEW ... POPULATE` used to leave the new view unsubscribed
# from its source table, so every row inserted after (and concurrently with) the replace was
# silently dropped. This is the concurrent variant of 04489: rows are inserted into the source
# while the replace is in progress, and every source row must end up in the view exactly once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS mv SYNC"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} --query "INSERT INTO src SELECT number FROM numbers(5000)"

# The view must already exist so that CREATE OR REPLACE actually replaces it (EXCHANGE), not just creates it.
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src"

# Insert into the source concurrently with the replace. `merge_tree_storage_snapshot_sleep_ms`
# widens the replace's snapshot window so the inserts reliably overlap it.
for j in $(seq 0 9); do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO src SELECT number FROM numbers(100000 + ${j} * 1000, 1000)"
done &

${CLICKHOUSE_CLIENT} --merge_tree_storage_snapshot_sleep_ms=150 --query "CREATE OR REPLACE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src"
wait

# With the fix the new view stays subscribed to its source, so every source row reaches the view:
# the set of distinct ids in the view equals the set in the source, with nothing lost.
# (We only assert that no row is lost. POPULATE is not atomic on its own - a row inserted
# concurrently can be both captured by the snapshot and delivered through the subscription, i.e.
# duplicated; making POPULATE exactly-once is a separate change. That is why we compare distinct
# id sets rather than row counts here.)
${CLICKHOUSE_CLIENT} --query "SELECT (SELECT uniqExact(id) FROM mv) = (SELECT uniqExact(id) FROM src) AS no_rows_lost"

${CLICKHOUSE_CLIENT} --query "DROP TABLE mv SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE src SYNC"
