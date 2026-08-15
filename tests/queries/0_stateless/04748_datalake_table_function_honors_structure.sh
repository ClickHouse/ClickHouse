#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Random settings limits: optimize_read_in_order=(1, None)
# Tag no-fasttest: Depends on Avro and Parquet
# Tag no-msan: DeltaKernel is not compiled with msan
# The clamp above pins optimize_read_in_order because the sorting-key arm asserts whether
# read-in-order was taken, which optimize_read_in_order=0 disables outright. Everything else
# stays randomized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PAIMON="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_paimon"
DELTA="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta"
ICE="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_ice"
mkdir -p "${USER_FILES_PATH}"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition" "${PAIMON}"
cp -r "${CUR_DIR}/data_delta_lake/struct_column_mapping" "${DELTA}"

CH="${CLICKHOUSE_CLIENT}"

# Runs a group of queries in one client session. An error aborts the rest of the session, so an arm
# expecting one goes last in its group, and an arm whose output is piped keeps its own client.
batch() { ${CH} -n; }

# Emits `<name> <type>` per column, so a DESC arm needs no pipe and can share a session.
DESCFMT="FORMAT CustomSeparated SETTINGS describe_compact_output = 1, format_custom_escaping_rule = 'Raw', format_custom_field_delimiter = ' ', format_custom_row_before_delimiter = '', format_custom_row_after_delimiter = '\n', format_custom_row_between_delimiter = ''"

echo "-- paimon: DESC and SELECT report the same declared type"
batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
DESC paimonLocal('${PAIMON}', 'Parquet', 'f_int Nullable(Int64)') ${DESCFMT};
SELECT toTypeName(f_int) FROM paimonLocal('${PAIMON}', 'Parquet', 'f_int Nullable(Int64)') LIMIT 1;
SELECT '-- paimon: the declared subset is honored and the values are intact';
SELECT groupArray(f_int) FROM (SELECT f_int FROM paimonLocal('${PAIMON}', 'Parquet', 'f_int Nullable(Int64)') ORDER BY f_int);
SELECT '-- paimon: a declared column absent from the metadata reads as a default, as for file()';
SELECT any(toTypeName(zzz)), countIf(zzz IS NULL) FROM paimonLocal('${PAIMON}', 'Parquet', 'zzz Nullable(String)');
SELECT '-- iceberg: DESC and SELECT report the same declared type';
CREATE TABLE ice (c0 String) ENGINE = IcebergLocal('${ICE}/', 'Parquet');
INSERT INTO TABLE FUNCTION icebergLocal('${ICE}/', 'Parquet', 'c0 String') (c0) SELECT 'a';
DESC icebergLocal('${ICE}/', 'Parquet', 'c0 LowCardinality(String)') ${DESCFMT};
SELECT toTypeName(c0), c0 FROM icebergLocal('${ICE}/', 'Parquet', 'c0 LowCardinality(String)');
SQL

# The compatible override just above is the pair for this arm: it stays green while a declared type
# the file cannot supply is rejected, which is what makes the rejection specific rather than the
# expression being broken. Map over a scalar is a structural conflict, so the outcome does not depend
# on the fixture's values.
echo "-- iceberg: a conflicting declared type is rejected instead of silently using the metadata schema"
${CH} -q "SELECT c0 FROM icebergLocal('${ICE}/', 'Parquet', 'c0 Map(String, String)')" 2>&1 | grep -oE 'Code: [0-9]+' | head -1

echo "-- paimon: a conflicting declared type is rejected instead of silently using the metadata schema"
${CH} -q "SELECT f_int FROM paimonLocal('${PAIMON}', 'Parquet', 'f_int Map(String, String)')" 2>&1 | grep -oE 'Code: [0-9]+' | head -1

# Files written before a rename carry an older schema id, so they are read through the evolution
# transform, whose output is the metadata schema alone. A declared column that no schema has must
# still reach the caller, and a declared column the CURRENT schema renamed away must not resurrect.
echo "-- iceberg: a declared column absent from the metadata reads as a default after schema evolution"
batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
CREATE TABLE ice16 (a Int64, b String) ENGINE = IcebergLocal('${ICE}16/', 'Parquet');
INSERT INTO ice16 SELECT number, 'x' FROM numbers(3);
ALTER TABLE ice16 RENAME COLUMN a TO renamed_a;
-- The evolved read itself works, so the arms below cannot pass or fail for want of a readable file.
SELECT groupArray(renamed_a) FROM ice16;
-- Row counts are part of every oracle here: groupArray skips NULLs, so an empty array reads the
-- same whether default rows arrived or the read returned nothing.
SELECT count(), countIf(zzz IS NULL), any(toTypeName(zzz)) FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz Nullable(String)');
-- A declared column alongside one the file does have, to show the surviving column still carries values.
SELECT groupArray(renamed_a), count(), countIf(zzz IS NULL) FROM icebergLocal('${ICE}16/', 'Parquet', 'renamed_a Nullable(Int64), zzz Nullable(String)');
-- A declared DEFAULT expression is honored rather than replaced by the bare type default.
SELECT groupArray(zzz) FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz UInt64 DEFAULT 42');
-- A filter on the declared column is evaluated against those defaults, so it must not read as
-- absent and drop every row. PREWHERE is separate from WHERE here because on this path it is
-- re-applied after the evolution transform rather than inside the reader.
SELECT count() FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz Nullable(String)') WHERE zzz IS NULL;
-- parallel_replicas_for_cluster_engines = 0 is required, not tidying: otherwise the table
-- function is wrapped in StorageObjectStorageCluster, which does not support PREWHERE, and the
-- analyzer rejects the query before any reader runs. Scoped to these arms so the cluster arms
-- below keep their coverage.
SELECT count() FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz Nullable(String)') PREWHERE zzz IS NULL SETTINGS parallel_replicas_for_cluster_engines = 0;
-- Selecting a different column is a separate case: the declared column is then filter-only, and
-- filter-only inputs are not in the reader's requested columns.
SELECT groupArray(renamed_a) FROM icebergLocal('${ICE}16/', 'Parquet', 'renamed_a Nullable(Int64), zzz Nullable(String)') PREWHERE zzz IS NULL SETTINGS parallel_replicas_for_cluster_engines = 0;
SQL
# A name the file's own (older) schema carries is NOT a missing column: the reader resolves it by
# field id. It stays rejected, as before, rather than being turned into a silent default.
${CH} -q "SELECT groupArray(a) FROM icebergLocal('${ICE}16/', 'Parquet', 'a Nullable(Int64)')" 2>&1 | grep -oE 'Code: [0-9]+' | head -1
# Requesting it beside a column that IS synthesized must not change that: the synthesis list is
# what every column in it is built from, so a name left out of it stays rejected either way.
${CH} -q "SELECT groupArray(a), countIf(zzz IS NULL) FROM icebergLocal('${ICE}16/', 'Parquet', 'a Nullable(Int64), zzz Nullable(String)')" 2>&1 | grep -oE 'Code: [0-9]+' | head -1

# An added column is emitted by the evolution transform itself rather than synthesized here, so it
# reaches `AddingDefaultsTransform` at a position the reader's missing-value bitmask does not
# describe. A declared `DEFAULT` on such a column is what selects that transform, so these arms
# exist separately from the renamed-column ones above.
#
# The nullability group that follows declares a filtered column at a different nullability than the
# file's own schema: the evolution transform emits the file's type, so a filter planned against the
# declared one would see its function return a differently-nullable result than the plan states. It
# shares this fixture, and nothing between the groups reads their output, so they run in one session.
# This marker contains an apostrophe, so it stays an echo rather than a SQL string literal.
echo "-- iceberg: a declared DEFAULT on a column added by schema evolution reads the file's values"
batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
CREATE TABLE ice17 (a Int64) ENGINE = IcebergLocal('${ICE}17/', 'Parquet');
INSERT INTO ice17 SELECT number FROM numbers(3);
ALTER TABLE ice17 ADD COLUMN c Nullable(UInt64);
-- Iceberg fills an added column with NULL for rows written before it existed, and the table read is
-- the authority on that. A declared DEFAULT must not override it, or one declaration would mean
-- different values per file, so the row count is asserted alongside the NULL count.
SELECT count(), countIf(c IS NULL) FROM ice17;
SELECT count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'c Nullable(UInt64) DEFAULT 42');
SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64) DEFAULT 42');
-- A file written after the column was added carries real values, which the same declaration must
-- still return: this is what distinguishes reading the bitmask correctly from ignoring defaults.
INSERT INTO ice17 SELECT number + 10, number + 500 FROM numbers(2);
SELECT groupArray(c) FROM (SELECT c FROM icebergLocal('${ICE}17/', 'Parquet', 'c Nullable(UInt64) DEFAULT 42') WHERE c IS NOT NULL ORDER BY c);
SELECT '-- iceberg: a filtered column declared at another nullability is read after schema evolution';
-- The filter is on a column the file has while an evolution-added one is also returned, and WHERE
-- and PREWHERE are both covered because only the latter is re-applied after the transform.
SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64)') WHERE a < 3;
SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64)') PREWHERE a < 3 SETTINGS parallel_replicas_for_cluster_engines = 0;
-- The declared type matching the file's is the control: it shares this path but needs no conversion.
SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Int64, c Nullable(UInt64)') WHERE a < 3;
SELECT '-- iceberg: a declared DEFAULT is applied to a table with deleted rows';
-- The reader's missing-value bitmask describes the reader's own rows, and a delete drops rows
-- without carrying it along, so a declared DEFAULT has to be applied before any row-dropping
-- transform. The row count is what makes this arm meaningful: it must reflect the delete.
CREATE TABLE ice18 (a Int64) ENGINE = IcebergLocal('${ICE}18/', 'Parquet');
INSERT INTO ice18 SELECT number FROM numbers(6);
DELETE FROM ice18 WHERE a = 2;
-- The delete must be visible here, or the arm below would read an undeleted table and pass either way.
SELECT count() FROM ice18;
SELECT groupArray(zzz) FROM icebergLocal('${ICE}18/', 'Parquet', 'zzz UInt64 DEFAULT 42');
SELECT '-- iceberg: a structure-bearing read does not keep the metadata sorting key';
-- A sorted iceberg table: the metadata sorting key is resolved against the metadata schema, so it
-- must not survive alongside a user-declared structure. An empty table counts as sorted, which is
-- what makes this reachable without a Spark-written fixture.
CREATE TABLE ice13 (id Int64, data String) ENGINE = IcebergLocal('${ICE}13/', 'Parquet') ORDER BY id;
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT * FROM icebergLocal('${ICE}13/', 'Parquet') ORDER BY id);
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT id FROM icebergLocal('${ICE}13/', 'Parquet', 'id Nullable(Int64)') ORDER BY id);
SELECT '-- iceberg: a declared subset that omits a key column does not keep the metadata sorting key';
-- The arm above retypes the key column; this one omits a key column instead. A declared subset that
-- drops the second component of (id, data) leaves the key describing a column that will not be
-- emitted, so it must be cleared too, even though the surviving prefix would still order correctly.
CREATE TABLE ice15 (id Int64, data String) ENGINE = IcebergLocal('${ICE}15/', 'Parquet') ORDER BY (id, data);
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT * FROM icebergLocal('${ICE}15/', 'Parquet') ORDER BY id);
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT id FROM icebergLocal('${ICE}15/', 'Parquet', 'id Int64') ORDER BY id);
SELECT '-- iceberg cluster: a worker keeps the metadata sorting key when only the initiator injected a structure';
CREATE TABLE ice14 (id Int64, data String) ENGINE = IcebergS3(s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/') ORDER BY id;
SQL

# A cluster initiator injects its own resolved columns into the remote query whether or not the user
# passed a structure, so the worker still sees a structure and must keep the key those columns match.
# `icebergLocalCluster` cannot express this (its argument injection is a no-op), so this arm goes
# through S3. The oracle has to be the WORKER's plan: an initiator-side EXPLAIN cannot see it, and a
# value or type oracle reads the same on both sides. These two need distinct query ids, so they keep
# their own clients.
QID_NOSTRUCT="04748-nostruct-${CLICKHOUSE_DATABASE}"
QID_STRUCT="04748-struct-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_NOSTRUCT}" -q "SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet') ORDER BY id SETTINGS optimize_read_in_order = 1" > /dev/null
${CH} --query_id="${QID_STRUCT}" -q "SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='id Nullable(Int64)') ORDER BY id SETTINGS optimize_read_in_order = 1" > /dev/null

batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
SYSTEM FLUSH LOGS processors_profile_log;
-- Worker rows only: query_id differs from initial_query_id on a secondary query. No sort on the
-- worker means the key survived and read-in-order was taken there. Asserted for this query id
-- first, so that a count of zero sorts cannot be read from an absence of worker rows.
SELECT count() > 0 FROM system.processors_profile_log WHERE initial_query_id = '${QID_NOSTRUCT}' AND query_id != initial_query_id;
SELECT countIf(name = 'PartialSortingTransform') FROM system.processors_profile_log WHERE initial_query_id = '${QID_NOSTRUCT}' AND query_id != initial_query_id;
-- The same read WITH a user structure still reaches the worker, which proves the arm above is not
-- green merely because nothing was dispatched.
SELECT count() > 0 FROM system.processors_profile_log WHERE initial_query_id = '${QID_STRUCT}' AND query_id != initial_query_id;
SELECT '-- iceberg cluster: a worker keeps declared columns that the metadata does not have';
-- Keeping the key and keeping the columns are two separate decisions: clearing the key must not
-- also discard the declared columns, or the snapshot would overwrite them and a column that exists
-- only in the declared structure would not be found on the shard. Declaring a column absent from
-- the metadata is what shows the columns survive the clear. Rows are required: on an empty table no
-- reader runs, so the extraction that has to find zzz is never reached, and the row count is part of
-- the oracle because groupArray skips NULLs.
INSERT INTO ice14 SELECT number, 'x' FROM numbers(2);
SELECT count(), countIf(zzz IS NULL), any(toTypeName(zzz)) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='zzz Nullable(String)');
SELECT '-- iceberg cluster: a worker honors a DEFAULT declared in the structure';
-- A DEFAULT clause survives only as long as the structure reaches the worker as text: the columns
-- the initiator resolves carry names and types alone, so a structure rebuilt from them would read as
-- zzz UInt64 and fill zeros. Summing is the oracle, since a dropped clause still yields two rows.
SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='zzz UInt64 DEFAULT 42');
SELECT '-- iceberg cluster: a DEFAULT declared next to an explicit URL is honored too';
-- A named collection and an explicit URL reach different argument-rewriting branches, and only the
-- explicit one rewrites the key-value arguments in place, so both are covered. The non-cluster read
-- is the reference: the two must agree, whichever branch rewrote the arguments.
SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure='zzz UInt64 DEFAULT 42');
SELECT count(), sum(zzz) FROM icebergS3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure='zzz UInt64 DEFAULT 42');
SELECT '-- iceberg cluster: a DEFAULT in a non-literal structure expression is honored';
-- A key-value value may be any constant expression, not just a literal, and it is evaluated before
-- the arguments are rewritten. Reading it as a literal alone would treat it as absent.
SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure=concat('zzz UInt64 DEFAULT ', '42'));
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
SELECT '-- iceberg cluster: DESC and SELECT report the same declared type';
DESC icebergLocalCluster('test_cluster_two_shards_localhost', '${ICE}/', 'Parquet', 'c0 LowCardinality(String)') ${DESCFMT};
SELECT DISTINCT toTypeName(c0) FROM icebergLocalCluster('test_cluster_two_shards_localhost', '${ICE}/', 'Parquet', 'c0 LowCardinality(String)');
SELECT '-- control: a cluster read without a structure argument still works';
SELECT DISTINCT c0, toTypeName(c0) FROM icebergLocalCluster('test_cluster_two_shards_localhost', '${ICE}/', 'Parquet');
SQL

echo "-- control: without a structure argument the metadata schema is still used"
# DESC cannot be a subquery, so a per-column assertion over its output keeps its own client here and
# at the two arms below.
${CH} -q "DESC paimonLocal('${PAIMON}')" | wc -l
${CH} -q "DESC paimonLocal('${PAIMON}')" | awk -F'\t' '$1=="f_int"{print $2}'
batch <<SQL
SET allow_experimental_paimon_storage_engine = 1;
SELECT toTypeName(f_int) FROM paimonLocal('${PAIMON}') LIMIT 1;
SELECT count() FROM paimonLocal('${PAIMON}');
SELECT '-- control: the persistent engine still takes its schema from the metadata';
CREATE TABLE pt (f_int Int64) ENGINE = PaimonLocal('${PAIMON}', 'Parquet');
SQL
${CH} -q "DESC pt" | awk -F'\t' '$1=="f_int"{print $2}'
echo "-- control: a column added externally stays visible on a table created with columns"
batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
CREATE TABLE t7 (c0 String) ENGINE = IcebergLocal('${ICE}7/', 'Parquet');
INSERT INTO TABLE FUNCTION icebergLocal('${ICE}7/', 'Parquet', 'c0 String') (c0) SELECT 'a';
CREATE TABLE u7 ENGINE = IcebergLocal('${ICE}7/', 'Parquet');
ALTER TABLE u7 ADD COLUMN c_ext Nullable(Int64);
SQL
${CH} -q "DESC t7" | grep -c c_ext
echo "-- control: an INSERT writes with the authoritative iceberg schema, not the declared one"
batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
CREATE TABLE t11 (c0 String) ENGINE = IcebergLocal('${ICE}11/', 'Parquet');
INSERT INTO TABLE FUNCTION icebergLocal('${ICE}11/', 'Parquet', 'c0 Int64') (c0) SELECT 7;
SELECT c0, toTypeName(c0) FROM icebergLocal('${ICE}11/');
-- A value the declared type accepts and the iceberg schema does not: the INSERT below must be
-- rejected while converting, because the sink header comes from the iceberg schema. If the
-- read-side policy leaked into writes, it would instead succeed and store a wrongly typed column.
CREATE TABLE t12 (c0 Int64) ENGINE = IcebergLocal('${ICE}12/', 'Parquet');
SQL
${CH} --allow_experimental_insert_into_iceberg=1 -q \
    "INSERT INTO TABLE FUNCTION icebergLocal('${ICE}12/', 'Parquet', 'c0 String') (c0) SELECT 'zzz'" 2>&1 \
    | grep -c "while converting source column"

echo "-- control: deltaLake is unchanged at its default"
batch <<SQL
SELECT toTypeName(c0), c0 FROM deltaLakeLocal('${DELTA}', 'Parquet', 'c0 Nullable(Int64)') ORDER BY c0;
SELECT '-- control: deltaLake with an explicit schema reload still prefers the metadata schema';
SELECT DISTINCT toTypeName(c1) FROM deltaLakeLocal('${DELTA}', 'Parquet', 'c1 String') SETTINGS delta_lake_reload_schema_for_consistency = 1;
DROP TABLE IF EXISTS ice14;
SQL
rm -rf "${PAIMON}" "${DELTA}" "${ICE}" "${ICE}7" "${ICE}11" "${ICE}12" "${ICE}13" "${ICE}15" "${ICE}16" "${ICE}17" "${ICE}18"
