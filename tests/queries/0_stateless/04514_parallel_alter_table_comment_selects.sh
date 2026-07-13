#!/usr/bin/env bash
# Tags: no-fasttest, no-flaky-check, long
#  no-fasttest, no-flaky-check: concurrency stress test, can run for a while under sanitizers.
#
# Regression test for issue #104692: a concurrent ALTER TABLE ... MODIFY COMMENT rewrites the
# table's `.sql` metadata object in place while another query reads it. On object-storage-backed
# metadata (the CI "db disk" variant) the read goes through a buffer that caches the file size at
# construction and asserts `file_offset_of_buffer_end <= getFileSize()`; a rewrite growing the
# object under the open reader aborted the server before the fix.
#
# readMetadataFile() forces a size-assertion-free synchronous read via three settings, each of
# which the read pipeline may otherwise pick and each of which snapshots the size up front:
#   1. remote_fs_settings.method = read   avoids the async prefetch threadpool buffer
#   2. reader_executor.enabled = false    avoids ReadPipeline::tryBuildReaderExecutor()
#   3. disableCaches()                    avoids CachedInMemoryReadBufferFromFile / page cache
# The three phases below drive the metadata read down each of those pipeline branches so removing
# any one of the three lines makes this test fail: default settings exercise the threadpool path,
# use_reader_executor=1 the executor path, and use_page_cache_for_disks_without_file_cache=1 the
# in-memory page-cache path. They are separate phases because enabling the executor and the page
# cache together falls back to the page-cache path instead of exercising the executor.
#
# Each concurrent worker sends its whole loop over a SINGLE client connection (one multiquery
# batch) rather than spawning one client process per iteration: under sanitizers the per-process
# client startup dominated the runtime and pushed the test over the per-test time budget.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -euo pipefail

RUNS=${RUNS:-200}
CONNECTIONS_PER_ROLE=${CONNECTIONS_PER_ROLE:-4}

pids=()
cleanup() {
  local rc=$?
  kill -9 "${pids[@]}" &>/dev/null || true
  wait "${pids[@]}" &>/dev/null || true
  exit $rc
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "
  DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t;
  DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.dst;
  CREATE TABLE ${CLICKHOUSE_DATABASE}.dst (a UInt64, b String) ENGINE = MergeTree ORDER BY a;
  CREATE TABLE ${CLICKHOUSE_DATABASE}.t (a UInt64, b String)
    ENGINE = Buffer(${CLICKHOUSE_DATABASE}, dst, 1, 1, 1, 100, 1000, 1000000, 10000000);
"

# Vary the comment length so the rewritten `.sql` object grows and shrinks under concurrent
# readers. The whole loop is one multiquery batch sent over a single connection.
alter_thread() {
  {
    for _ in $(seq 1 "$RUNS"); do
      local len=$(( (RANDOM % 300) + 1 ))
      local c
      c=$(printf 'x%.0s' $(seq 1 "$len"))
      echo "ALTER TABLE ${CLICKHOUSE_DATABASE}.t MODIFY COMMENT '$c';"
    done
  } | $CLICKHOUSE_CLIENT --multiquery &>/dev/null || true
}

# The SELECT over system.tables reads each table's `.sql` metadata; the settings passed here flow
# into readMetadataFile() and choose which read-pipeline branch the metadata read takes. The whole
# loop is one multiquery batch sent over a single connection with those settings.
select_thread() {
  local settings=$1
  {
    for _ in $(seq 1 "$RUNS"); do
      echo "SELECT * FROM system.tables WHERE database = currentDatabase() FORMAT Null;"
    done
  # shellcheck disable=SC2086
  } | $CLICKHOUSE_CLIENT $settings --multiquery &>/dev/null || true
}

# One race phase: spawn ALTER + SELECT connections with the given SELECT settings, then join.
run_phase() {
  local settings=$1
  pids=()
  for _ in $(seq 1 "$CONNECTIONS_PER_ROLE"); do
    alter_thread & pids+=("$!")
    select_thread "$settings" & pids+=("$!")
  done
  wait "${pids[@]}"
  pids=()
}

# Phase 1: default read settings, async threadpool metadata buffer.
run_phase ""
# Phase 2: use_reader_executor=1, ReadPipeline reader-executor path.
run_phase "--use_reader_executor 1"
# Phase 3: use_page_cache_for_disks_without_file_cache=1, CachedInMemoryReadBufferFromFile.
run_phase "--use_page_cache_for_disks_without_file_cache 1"

# If the server survived every race phase, it still answers.
$CLICKHOUSE_CLIENT -q "SELECT 1"
