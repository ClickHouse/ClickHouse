#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Random settings limits: optimize_read_in_order=(1, None)
# Tag no-fasttest: Depends on Avro and Parquet
# Tag no-msan: DeltaKernel is not compiled with msan
# The clamp above pins optimize_read_in_order because the sorted-order arm separates the metadata
# key from a declared one only while read-in-order can be taken: with it off the sort always runs
# and both answers agree. Everything else stays randomized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DELTA="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta"
mkdir -p "${USER_FILES_PATH}"
cp -r "${CUR_DIR}/data_delta_lake/struct_column_mapping" "${DELTA}"
# Read-only committed fixture, shared like iceberg_sorted_order_test below.
EQDEL="deletes_db/eq_deletes_table"

CH="${CLICKHOUSE_CLIENT}"

# Runs a group of queries in one client session. An unexpected error aborts the rest of the session,
# so an arm expecting one either carries a `serverError` hint (which the client consumes, leaving the
# session alive) or goes last in its group; an arm whose output is piped keeps its own client.
batch() { ${CH} -n; }

batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
SELECT '-- iceberg cluster: a worker keeps the metadata sorting key when only the initiator injected a structure';
CREATE TABLE ice14 (id Int64, data String) ENGINE = IcebergS3(s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/') ORDER BY id;
SQL

# A cluster initiator injects its own resolved columns into the remote query whether or not the user
# passed a structure, so the worker still sees a structure and must keep the key those columns match.
# `icebergLocalCluster` cannot express this (its argument injection is a no-op), so this arm goes
# through S3. The oracle has to be the WORKER's plan: an initiator-side EXPLAIN cannot see it, and a
# value or type oracle reads the same on both sides. These two need distinct query ids, so they keep
# their own clients.
QID_NOSTRUCT="04751-nostruct-${CLICKHOUSE_DATABASE}"
QID_STRUCT="04751-struct-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_NOSTRUCT}" -q "SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet') ORDER BY id SETTINGS optimize_read_in_order = 1" > /dev/null
${CH} --query_id="${QID_STRUCT}" -q "SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='id Nullable(Int64)') ORDER BY id SETTINGS optimize_read_in_order = 1" > /dev/null

batch <<SQL
SYSTEM FLUSH LOGS processors_profile_log;
-- Worker rows only: query_id differs from initial_query_id on a secondary query. No sort on the
-- worker means the key survived and read-in-order was taken there. Asserted for this query id
-- first, so that a count of zero sorts cannot be read from an absence of worker rows.
SELECT count() > 0 FROM system.processors_profile_log WHERE initial_query_id = '${QID_NOSTRUCT}' AND query_id != initial_query_id;
SELECT countIf(name = 'PartialSortingTransform') FROM system.processors_profile_log WHERE initial_query_id = '${QID_NOSTRUCT}' AND query_id != initial_query_id;
-- The same read WITH a user structure still reaches the worker, which proves the arm above is not
-- green merely because nothing was dispatched, and its own sort count is the paired contrast: a
-- structure the user passed clears the key, so the worker has to sort.
SELECT count() > 0 FROM system.processors_profile_log WHERE initial_query_id = '${QID_STRUCT}' AND query_id != initial_query_id;
SELECT countIf(name = 'PartialSortingTransform') > 0 FROM system.processors_profile_log WHERE initial_query_id = '${QID_STRUCT}' AND query_id != initial_query_id;
SQL

batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
SELECT '-- iceberg cluster: a worker keeps declared columns that the metadata does not have';
-- Keeping the key and keeping the columns are two separate decisions: clearing the key must not
-- also discard the declared columns, or the snapshot would overwrite them and a column that exists
-- only in the declared structure would not be found on the shard. Declaring a column absent from
-- the metadata is what shows the columns survive the clear. Rows are required: on an empty table no
-- reader runs, so the extraction that has to find zzz is never reached, and the row count is part of
-- the oracle because groupArray skips NULLs.
INSERT INTO ice14 SELECT number, 'x' FROM numbers(2);
SELECT count(), countIf(zzz IS NULL), any(toTypeName(zzz)) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='zzz Nullable(String)');
SELECT '-- iceberg cluster: a DEFAULT declared in the structure is not honored, on either branch';
-- A named collection and an explicit URL reach different argument-rewriting branches, and only the
-- explicit one rewrites the key-value arguments in place, so both are covered, together with the
-- non-cluster read that is their reference. All three leave a defaulted declaration to the lake
-- schema, so the column only the declaration has is unknown on the initiator and on the shard alike.
SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='zzz UInt64 DEFAULT 42'); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure='zzz UInt64 DEFAULT 42'); -- { serverError UNKNOWN_IDENTIFIER }
SELECT count(), sum(zzz) FROM icebergS3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure='zzz UInt64 DEFAULT 42'); -- { serverError UNKNOWN_IDENTIFIER }
SELECT '-- iceberg cluster: a structure given as a server-constant expression is dispatched';
-- A key-value value may be any constant expression, not just a literal, and it is resolved once while
-- the arguments are rewritten. A function that is deliberately not foldable once a context is
-- distributed must therefore not be re-resolved during dispatch. The old analyzer is pinned because
-- only it marks the context distributed before the arguments are rewritten, so it is the mode where a
-- re-resolution is observable at all; the literal-operand arm beside it is the pair that shows the
-- outcome is about folding rather than about concat.
SELECT count(), countIf(zzz IS NULL) FROM icebergS3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure=concat('zzz Nullable(String)', left(hostName(), 0))) SETTINGS enable_analyzer = 0;
SELECT count(), countIf(zzz IS NULL) FROM icebergS3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure=concat('zzz Nullable', '(String)')) SETTINGS enable_analyzer = 0;
-- The same shape on the plain s3 cluster function, which shares that rewriting branch and has its own
-- long-standing coverage: a structure that is not a literal must keep working there too.
SELECT count() FROM s3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/**.parquet', 'test', 'testtest', format='Parquet', structure=concat('id Nullable(Int64)', left(hostName(), 0))) SETTINGS enable_analyzer = 0;
SELECT '-- iceberg cluster: a declared type that reorders the key column is not ordered by the metadata key';
-- The key describes the metadata schema, so a declared type that reorders the key column makes it
-- unsound: read-in-order would then emit rows in the underlying numeric order while the user asked
-- for a String. Rows 2, 9, 10 differ numerically and lexicographically, so the ORDER is the oracle.
-- The fixture is committed because read-in-order additionally needs a per-file sort_order_id
-- matching the table's default-sort-order-id, which ClickHouse's own writer leaves NULL; Spark
-- writes 0, which is what this manifest carries.
SELECT groupArray(id) FROM (SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='iceberg_sorted_order_test/', format='Parquet', structure='id String') ORDER BY id);
-- Same table without a structure: still in its own correct order, so the arm above cannot pass
-- merely because nothing was dispatched or the fixture is unsorted.
SELECT groupArray(id) FROM (SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='iceberg_sorted_order_test/', format='Parquet') ORDER BY id);
SELECT '-- iceberg: a retyped filter column is read as declared where equality deletes are applied';
-- The declared String puts 1000..1009 below '2' while the lake Int32 does not, so the two row sets are
-- 92 and 2. WHERE, PREWHERE and a lake-typed oracle must all report 92; declaring the lake's own type
-- instead is the pair that keeps 2, and the unfiltered arm shows the deletes are still applied. Ids
-- reaching four digits are what separate the row sets, and no writer emits equality deletes, so the
-- fixture is committed.
SELECT countIf(toString(id) < '2') FROM icebergS3(s3_conn, filename='${EQDEL}');
SELECT count() FROM icebergS3(s3_conn, filename='${EQDEL}', format='Parquet', structure='id String, name String') WHERE id < '2';
-- Returning only the other column is what lets the mover put this same WHERE into PREWHERE, so no
-- explicit PREWHERE is needed to reach it; the mover is pinned because it is randomized off.
SELECT count(name) FROM icebergS3(s3_conn, filename='${EQDEL}', format='Parquet', structure='id String, name String') WHERE id < '2' SETTINGS parallel_replicas_for_cluster_engines = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT count() FROM icebergS3(s3_conn, filename='${EQDEL}', format='Parquet', structure='id String, name String') PREWHERE id < '2' SETTINGS parallel_replicas_for_cluster_engines = 0;
SELECT count() FROM icebergS3(s3_conn, filename='${EQDEL}', format='Parquet', structure='id Nullable(Int32), name Nullable(String)') PREWHERE id < 2 SETTINGS parallel_replicas_for_cluster_engines = 0;
SELECT any(toTypeName(id)), count() FROM icebergS3(s3_conn, filename='${EQDEL}', format='Parquet', structure='id String, name String');

SELECT '-- control: a declared DEFAULT is still honored where it always was';
-- The lake formats whose schema reload is not a user opt-in are the only ones this policy reaches, so
-- a declared default has to keep working on the others and on plain object storage. Without these two
-- arms a policy applied to every table function would read as green.
SELECT count(), sum(zzz) FROM s3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/**.parquet', 'test', 'testtest', 'Parquet', 'zzz UInt64 DEFAULT 42');
-- deltaLake reaches its column mapping with the declared name whether or not a default is attached, so
-- it answers the same way it always has.
SELECT groupArray(zzz) FROM deltaLakeLocal('${DELTA}', 'Parquet', 'zzz UInt64 DEFAULT 42'); -- { serverError INCORRECT_DATA }
DROP TABLE IF EXISTS ice14;
SQL
rm -rf "${DELTA}"
