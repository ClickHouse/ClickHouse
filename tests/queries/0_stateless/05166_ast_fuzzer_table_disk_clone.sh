#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# `table_disk = true` points the disk at the table's own data instead of the database's. The query
# fuzzer rewrites a CREATE into a `__fuzz_N` clone with a mutated column list and sorting key, so a
# clone that kept `table_disk` would read the original table's parts under a schema those parts do
# not satisfy. Both carriers below go through QueryFuzzer::fuzzTableStorage, which drops the setting.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The serverfuzz/stress profile sets ast_fuzzer_runs server-wide, which would fuzz the setup and
# oracle statements too. Pin the baseline to 0 so only the two statements below fire the fuzzer.
CLIENT="${CLICKHOUSE_CLIENT} --ast_fuzzer_runs=0 --ast_fuzzer_any_query=0"
FUZZ="${CLICKHOUSE_CLIENT} --ast_fuzzer_runs=25 --ast_fuzzer_any_query=1 --send_logs_level=fatal"

${CLIENT} --query "DROP VIEW IF EXISTS viewer SYNC"
${CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"
${CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"

disk_path="disks/05166/${CLICKHOUSE_DATABASE}/"

${CLIENT} --query "
CREATE TABLE writer (key Int32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true,
  disk = disk(
      name = 05166_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"
${CLIENT} --query "INSERT INTO writer SELECT number FROM numbers(64)"

# Carrier 1: the outer storage clause of a plain CREATE TABLE, over the root `writer` owns.
${FUZZ} --query "
CREATE TABLE reader (key Int32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true,
  disk = disk(
      read_only = true,
      name = 05166_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
" 2>/dev/null

# Carrier 2: a view's inner engine, reached through create.targets->getInnerEngines(). It needs a
# root of its own: two plain-rewritable disks cannot share one prefix unless one is read-only.
${FUZZ} --query "
CREATE MATERIALIZED VIEW viewer (key Int32) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = true,
  disk = disk(
      name = 05166_viewer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = 'disks/05166_viewer/${CLICKHOUSE_DATABASE}/')
AS SELECT key FROM writer
" 2>/dev/null

${CLIENT} --query "SYSTEM FLUSH LOGS query_log"

# The `attempted` lines are positive controls: without them the contract lines would also pass on a
# fuzzer that never rewrote anything. The fuzzer logs a clone CREATE even when it fails to execute.
${CLIENT} --query "
SELECT 'table_clones_attempted', count() > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create'
  AND position(query, 'reader__fuzz_') > 0;

SELECT 'table_clones_with_table_disk', count() FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create'
  AND position(query, 'reader__fuzz_') > 0
  AND match(query, '(^|[^0-9A-Za-z_])table_disk($|[^0-9A-Za-z_])');

SELECT 'view_clones_attempted', count() > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create'
  AND position(query, 'viewer__fuzz_') > 0;

SELECT 'view_clones_with_table_disk', count() FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create'
  AND position(query, 'viewer__fuzz_') > 0
  AND match(query, '(^|[^0-9A-Za-z_])table_disk($|[^0-9A-Za-z_])');

SELECT 'seed_predicate_live', count() > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND query_kind = 'Create'
  AND position(query, 'CREATE TABLE reader ') > 0
  AND match(query, '(^|[^0-9A-Za-z_])table_disk($|[^0-9A-Za-z_])');
"
