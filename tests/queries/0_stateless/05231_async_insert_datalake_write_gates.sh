#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-replicated-database
# Tag no-fasttest: delta-kernel, Iceberg and Paimon pull in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.
# Tag no-replicated-database: kept for PaimonLocal, which no other test exercises without it.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/120717
# A write refused by a data lake write gate used to be acknowledged and then silently dropped
# when the insert was asynchronous and did not wait: the gates lived only on the sink path,
# which an asynchronous insert reaches in a background flush, long after the client was told
# the insert succeeded. The rows were lost with nothing but a `FlushError` row in
# `system.asynchronous_insert_log` to show it.
#
# `system.asynchronous_insert_log` is therefore the oracle for the queueing itself: while the
# defect was present the refused batch left a `FlushError` row there, and now that the insert
# is refused before it is queued there is no row at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_async_gates"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"
mkdir -p "${ROOT}"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition" "${ROOT}/paimon"

# Both flags are load-bearing, as in 03148_async_queries_in_query_log_errors: with the adaptive
# timeout on, the batch can be scheduled immediately, and with it off the deadline thread drains
# it once the busy timeout expires (5000 ms under tests/config/users.d/timeouts.xml). Pushing the
# timeout out keeps the batch queued until this test flushes it, so the pre-fix flush error is
# recorded deterministically rather than racing the assertion.
ASYNC_NOWAIT=(--async_insert=1 --wait_for_async_insert=0
              --async_insert_use_adaptive_busy_timeout=0 --async_insert_busy_timeout_max_ms=300000)

# Rows the insert left in the queue, seen through the log the flush writes. `SYSTEM FLUSH ASYNC
# INSERT QUEUE` waits for the jobs it schedules itself, so one `SYSTEM FLUSH LOGS` after it is
# enough to read the outcome.
queued_rows() {
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${1}"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS asynchronous_insert_log"
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.asynchronous_insert_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
          AND database = currentDatabase() AND query_id = '${2}'"
}

# The same question for a table function, asked of the live queue instead. A batch queued by an
# `INSERT INTO FUNCTION` cannot be reached by `SYSTEM FLUSH ASYNC INSERT QUEUE <table>`: the queue
# keys its entries on the insert's `table_id` (`InsertQuery::getStorageID`), which is empty for a
# table function, so no table name matches it, and an unscoped flush would drain other tests'
# batches. The busy timeout above keeps the batch parked, so reading the queue needs no flush.
queued_now() {
    ${CLICKHOUSE_CLIENT} -q \
        "SELECT count() FROM system.asynchronous_inserts WHERE has(\`entries.query_id\`, '${1}')"
}

${CLICKHOUSE_CLIENT} -q "
    SET allow_delta_kernel_rs = 1, allow_delta_lake_writes = 1, allow_delta_lake_create_table = 1;
    CREATE TABLE t_delta (id Int32) ENGINE = DeltaLakeLocal('${ROOT}/delta', Parquet);
"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_iceberg (id Int32) ENGINE = IcebergLocal('${ROOT}/iceberg/')"
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine=1 -q \
    "CREATE TABLE t_paimon ENGINE = PaimonLocal('${ROOT}/paimon')"

echo '-- delta, writes off, asynchronous insert that does not wait: refused, not acknowledged'
QID="${CLICKHOUSE_DATABASE}_delta_nowait"
${CLICKHOUSE_CLIENT} --allow_delta_lake_writes=0 "${ASYNC_NOWAIT[@]}" --query_id="${QID}" \
    -q "INSERT INTO t_delta VALUES (1)" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
echo "-- rows queued by it: $(queued_rows t_delta "${QID}")"
echo "-- rows in the table: $(${CLICKHOUSE_CLIENT} -q 'SELECT count() FROM t_delta')"

echo '-- delta control, writes on: an asynchronous insert still goes through'
${CLICKHOUSE_CLIENT} --allow_delta_lake_writes=1 --async_insert=1 --wait_for_async_insert=1 \
    -q "INSERT INTO t_delta VALUES (2)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_delta"

echo '-- delta control, writes off, synchronous insert: unchanged'
${CLICKHOUSE_CLIENT} --allow_delta_lake_writes=0 -q "INSERT INTO t_delta VALUES (3)" 2>&1 \
    | grep -o -m1 'SUPPORT_IS_DISABLED'

# An `INSERT INTO FUNCTION` resolves to the same storage a named table does, and the gate is not an
# access check, so the table function's own access check does not stand in for it.
echo '-- delta through a table function, writes off, asynchronous insert that does not wait'
QID="${CLICKHOUSE_DATABASE}_delta_tf_nowait"
${CLICKHOUSE_CLIENT} --allow_delta_lake_writes=0 "${ASYNC_NOWAIT[@]}" --query_id="${QID}" \
    -q "INSERT INTO FUNCTION deltaLakeLocal('${ROOT}/delta') VALUES (4)" 2>&1 \
    | grep -o -m1 'SUPPORT_IS_DISABLED'
echo "-- entries it left in the queue: $(queued_now "${QID}")"

# A materialized view has no sink of its own: it passes the write to its target table, so the target
# is the storage whose refusal has to be reported. The view is a named table, so the flush reaches it.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_src (id Int32) ENGINE = Null;
    CREATE MATERIALIZED VIEW mv_delta TO t_delta AS SELECT id FROM t_src;
"

echo '-- delta through a materialized view, writes off, asynchronous insert that does not wait'
QID="${CLICKHOUSE_DATABASE}_delta_mv_nowait"
${CLICKHOUSE_CLIENT} --allow_delta_lake_writes=0 "${ASYNC_NOWAIT[@]}" --query_id="${QID}" \
    -q "INSERT INTO mv_delta VALUES (6)" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
echo "-- rows queued by it: $(queued_rows mv_delta "${QID}")"

echo '-- delta through a materialized view, writes on: an asynchronous insert still goes through'
${CLICKHOUSE_CLIENT} --allow_delta_lake_writes=1 --async_insert=1 --wait_for_async_insert=1 \
    -q "INSERT INTO mv_delta VALUES (7)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_delta"

echo '-- iceberg, inserts off, asynchronous insert that does not wait: refused, not acknowledged'
QID="${CLICKHOUSE_DATABASE}_iceberg_nowait"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=0 "${ASYNC_NOWAIT[@]}" --query_id="${QID}" \
    -q "INSERT INTO t_iceberg VALUES (1)" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
echo "-- rows queued by it: $(queued_rows t_iceberg "${QID}")"

echo '-- iceberg control, inserts on: an asynchronous insert still goes through'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=1 --wait_for_async_insert=1 \
    -q "INSERT INTO t_iceberg VALUES (2)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_iceberg"

# A `DEFINER` view runs its write under the definer's settings, not the invoker's, so the target has to be
# asked the same way, or a write the sink would accept is refused first. No test profile sets Iceberg's gate.
DEFINER_USER="definer_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "
    CREATE USER ${DEFINER_USER} IDENTIFIED WITH no_password SETTINGS allow_insert_into_iceberg = 1;
    GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.* TO ${DEFINER_USER};
    CREATE MATERIALIZED VIEW mv_iceberg_definer TO t_iceberg
        DEFINER = ${DEFINER_USER} SQL SECURITY DEFINER AS SELECT id FROM t_src;
"

echo '-- iceberg through a definer view: the definer may write, the invoker session may not: accepted'
${CLICKHOUSE_CLIENT} --async_insert=1 --wait_for_async_insert=1 \
    -q "INSERT INTO mv_iceberg_definer VALUES (8)"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_iceberg"

# Paimon has no writer at all, so its refusal comes from the unsupported default rather than from
# a setting; the initiator must report it for the same reason.
echo '-- paimon, asynchronous insert that does not wait: refused, not acknowledged'
QID="${CLICKHOUSE_DATABASE}_paimon_nowait"
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine=1 "${ASYNC_NOWAIT[@]}" \
    --query_id="${QID}" -q "INSERT INTO t_paimon VALUES (1)" 2>&1 \
    | grep -o -m1 'Writes are not supported for engine'
echo "-- rows queued by it: $(queued_rows t_paimon "${QID}")"

echo '-- paimon, synchronous insert: unchanged'
${CLICKHOUSE_CLIENT} --allow_experimental_paimon_storage_engine=1 \
    -q "INSERT INTO t_paimon VALUES (1)" 2>&1 \
    | grep -o -m1 'Writes are not supported for engine'

# Plain object storage is refused for a path it cannot write to at all, which is decided from the
# path alone. No endpoint is needed: the refusal happens before any request is issued.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_globs (k UInt64) ENGINE = S3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_*.parquet', 'clickhouse', 'clickhouse', Parquet);
    CREATE TABLE t_archive (k UInt64) ENGINE = S3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}.zip :: data.parquet', 'clickhouse', 'clickhouse', Parquet);
    CREATE TABLE t_plain (k UInt64) ENGINE = S3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_plain.parquet', 'clickhouse', 'clickhouse', Parquet);
"

echo '-- plain object storage, globbed path, asynchronous insert that does not wait'
QID="${CLICKHOUSE_DATABASE}_globs_nowait"
${CLICKHOUSE_CLIENT} "${ASYNC_NOWAIT[@]}" --query_id="${QID}" \
    -q "INSERT INTO t_globs VALUES (1)" 2>&1 | grep -o -m1 'the table is in readonly mode'
echo "-- rows queued by it: $(queued_rows t_globs "${QID}")"

echo '-- plain object storage, archive path, asynchronous insert that does not wait'
${CLICKHOUSE_CLIENT} "${ASYNC_NOWAIT[@]}" -q "INSERT INTO t_archive VALUES (1)" 2>&1 \
    | grep -o -m1 'Write into archive is not supported'

echo '-- plain object storage through a table function, globbed path, asynchronous insert that does not wait'
QID="${CLICKHOUSE_DATABASE}_globs_tf_nowait"
${CLICKHOUSE_CLIENT} "${ASYNC_NOWAIT[@]}" --query_id="${QID}" -q \
    "INSERT INTO FUNCTION s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_tf_*.parquet', 'clickhouse', 'clickhouse', Parquet, 'k UInt64') VALUES (1)" 2>&1 \
    | grep -o -m1 'the table is in readonly mode'
echo "-- entries it left in the queue: $(queued_now "${QID}")"

# The cluster storage is a carrier of its own, and only an explicit cluster function builds it on the
# initiator for an insert: the plain table function builds it only for a read.
echo '-- plain object storage through a cluster table function, globbed path, asynchronous insert that does not wait'
QID="${CLICKHOUSE_DATABASE}_globs_cluster_nowait"
${CLICKHOUSE_CLIENT} "${ASYNC_NOWAIT[@]}" --query_id="${QID}" -q \
    "INSERT INTO FUNCTION s3Cluster('test_shard_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_cl_*.parquet', 'clickhouse', 'clickhouse', Parquet, 'k UInt64') VALUES (1)" 2>&1 \
    | grep -o -m1 'the table is in readonly mode'
echo "-- entries it left in the queue: $(queued_now "${QID}")"

# The initiator must not start refusing a writable plain object storage table. Asserted positively,
# on the client's own status and output, so that an unrelated failure cannot satisfy it; the batch is
# never flushed here, so no endpoint is needed for this case either.
echo '-- plain object storage, ordinary path: still accepted on the initiator'
out=$(${CLICKHOUSE_CLIENT} "${ASYNC_NOWAIT[@]}" -q "INSERT INTO t_plain VALUES (1)" 2>&1); rc=$?
echo "-- accepted on the initiator: rc=${rc} output_lines=$(printf '%s' "${out}" | grep -c . || true)"

# That accepted batch stays queued until the busy timeout expires, which outlives this test. Drain
# it, scoped to its own table, so that a later unscoped flush cannot write to a dropped table. Its
# outcome depends on the endpoint and is not this test's subject.
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE t_plain" 2>/dev/null || true

${CLICKHOUSE_CLIENT} -q "
    DROP VIEW mv_iceberg_definer; DROP VIEW mv_delta; DROP TABLE t_src;
    DROP TABLE t_delta; DROP TABLE t_iceberg; DROP TABLE t_paimon;
    DROP TABLE t_globs; DROP TABLE t_archive; DROP TABLE t_plain;
    DROP USER ${DEFINER_USER};
"
