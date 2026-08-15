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

echo "-- paimon: DESC and SELECT report the same declared type"
${CH} -q "DESC paimonLocal('${PAIMON}', 'Parquet', 'f_int Nullable(Int64)')" | awk -F'\t' '{print $1, $2}'
${CH} -q "SELECT toTypeName(f_int) FROM paimonLocal('${PAIMON}', 'Parquet', 'f_int Nullable(Int64)') LIMIT 1"

echo "-- paimon: the declared subset is honored and the values are intact"
${CH} -q "SELECT groupArray(f_int) FROM (SELECT f_int FROM paimonLocal('${PAIMON}', 'Parquet', 'f_int Nullable(Int64)') ORDER BY f_int)"

echo "-- paimon: a declared column absent from the metadata reads as a default, as for file()"
${CH} -q "SELECT toTypeName(zzz), countIf(zzz IS NULL) FROM paimonLocal('${PAIMON}', 'Parquet', 'zzz Nullable(String)')"

echo "-- iceberg: DESC and SELECT report the same declared type"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE ice (c0 String) ENGINE = IcebergLocal('${ICE}/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO TABLE FUNCTION icebergLocal('${ICE}/', 'Parquet', 'c0 String') (c0) SELECT 'a'"
${CH} -q "DESC icebergLocal('${ICE}/', 'Parquet', 'c0 LowCardinality(String)')" | awk -F'\t' '{print $1, $2}'
${CH} -q "SELECT toTypeName(c0), c0 FROM icebergLocal('${ICE}/', 'Parquet', 'c0 LowCardinality(String)')"

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
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE ice16 (a Int64, b String) ENGINE = IcebergLocal('${ICE}16/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO ice16 SELECT number, 'x' FROM numbers(3)"
${CH} --allow_experimental_insert_into_iceberg=1 -q "ALTER TABLE ice16 RENAME COLUMN a TO renamed_a"
# The evolved read itself works, so the arms below cannot pass or fail for want of a readable file.
${CH} -q "SELECT groupArray(renamed_a) FROM ice16"
# Row counts are part of every oracle here: `groupArray` skips NULLs, so an empty array reads the
# same whether default rows arrived or the read returned nothing.
${CH} -q "SELECT count(), countIf(zzz IS NULL), toTypeName(zzz) FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz Nullable(String)')"
# A declared column alongside one the file does have, to show the surviving column still carries values.
${CH} -q "SELECT groupArray(renamed_a), count(), countIf(zzz IS NULL) FROM icebergLocal('${ICE}16/', 'Parquet', 'renamed_a Nullable(Int64), zzz Nullable(String)')"
# A declared DEFAULT expression is honored rather than replaced by the bare type default.
${CH} -q "SELECT groupArray(zzz) FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz UInt64 DEFAULT 42')"
# A filter on the declared column is evaluated against those defaults, so it must not read as
# absent and drop every row. PREWHERE is separate from WHERE here because on this path it is
# re-applied after the evolution transform rather than inside the reader.
${CH} -q "SELECT count() FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz Nullable(String)') WHERE zzz IS NULL"
# `parallel_replicas_for_cluster_engines = 0` is required, not tidying: otherwise the table
# function is wrapped in `StorageObjectStorageCluster`, which does not support PREWHERE, and the
# analyzer rejects the query before any reader runs. Scoped to this arm so the cluster arms below
# keep their coverage.
${CH} -q "SELECT count() FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz Nullable(String)') PREWHERE zzz IS NULL SETTINGS parallel_replicas_for_cluster_engines = 0"
# Selecting a different column is a separate case: the declared column is then filter-only, and
# filter-only inputs are not in the reader's requested columns.
${CH} -q "SELECT groupArray(renamed_a) FROM icebergLocal('${ICE}16/', 'Parquet', 'renamed_a Nullable(Int64), zzz Nullable(String)') PREWHERE zzz IS NULL SETTINGS parallel_replicas_for_cluster_engines = 0"
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
echo "-- iceberg: a declared DEFAULT on a column added by schema evolution reads the file's values"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE ice17 (a Int64) ENGINE = IcebergLocal('${ICE}17/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO ice17 SELECT number FROM numbers(3)"
${CH} --allow_experimental_insert_into_iceberg=1 -q "ALTER TABLE ice17 ADD COLUMN c Nullable(UInt64)"
# Iceberg fills an added column with NULL for rows written before it existed, and the table read is
# the authority on that. A declared DEFAULT must not override it, or one declaration would mean
# different values per file, so the row count is asserted alongside the NULL count.
${CH} -q "SELECT count(), countIf(c IS NULL) FROM ice17"
${CH} -q "SELECT count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'c Nullable(UInt64) DEFAULT 42')"
${CH} -q "SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64) DEFAULT 42')"
# A file written after the column was added carries real values, which the same declaration must
# still return: this is what distinguishes reading the bitmask correctly from ignoring defaults.
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO ice17 SELECT number + 10, number + 500 FROM numbers(2)"
${CH} -q "SELECT groupArray(c) FROM (SELECT c FROM icebergLocal('${ICE}17/', 'Parquet', 'c Nullable(UInt64) DEFAULT 42') WHERE c IS NOT NULL ORDER BY c)"

# Declaring a filtered column at a different nullability than the file's own schema is the separate
# case: the evolution transform emits the file's type, so a filter planned against the declared one
# would see its function return a differently-nullable result than the plan states. The filter is on
# a column the file has while an evolution-added one is also returned, and `WHERE` and `PREWHERE`
# are both covered because only the latter is re-applied after the transform.
echo "-- iceberg: a filtered column declared at another nullability is read after schema evolution"
${CH} -q "SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64)') WHERE a < 3"
${CH} -q "SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64)') PREWHERE a < 3 SETTINGS parallel_replicas_for_cluster_engines = 0"
# The declared type matching the file's is the control: it shares this path but needs no conversion.
${CH} -q "SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Int64, c Nullable(UInt64)') WHERE a < 3"

# The reader's missing-value bitmask describes the reader's own rows, and a delete drops rows without
# carrying it along, so a declared DEFAULT has to be applied before any row-dropping transform. The
# row count is what makes this arm meaningful: it must reflect the delete.
echo "-- iceberg: a declared DEFAULT is applied to a table with deleted rows"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE ice18 (a Int64) ENGINE = IcebergLocal('${ICE}18/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO ice18 SELECT number FROM numbers(6)"
${CH} --allow_experimental_insert_into_iceberg=1 -q "DELETE FROM ice18 WHERE a = 2"
# The delete must be visible here, or the arm below would read an undeleted table and pass either way.
${CH} -q "SELECT count() FROM ice18"
${CH} -q "SELECT groupArray(zzz) FROM icebergLocal('${ICE}18/', 'Parquet', 'zzz UInt64 DEFAULT 42')"

# A sorted iceberg table: the metadata sorting key is resolved against the metadata schema, so it
# must not survive alongside a user-declared structure. An empty table counts as sorted, which is
# what makes this reachable without a Spark-written fixture.
echo "-- iceberg: a structure-bearing read does not keep the metadata sorting key"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE ice13 (id Int64, data String) ENGINE = IcebergLocal('${ICE}13/', 'Parquet') ORDER BY id"
${CH} -q "EXPLAIN PIPELINE SELECT * FROM icebergLocal('${ICE}13/', 'Parquet') ORDER BY id" | grep -c PartialSortingTransform
${CH} -q "EXPLAIN PIPELINE SELECT id FROM icebergLocal('${ICE}13/', 'Parquet', 'id Nullable(Int64)') ORDER BY id" | grep -c PartialSortingTransform

# The arm above retypes the key column; this one omits a key column instead. A declared subset that
# drops the second component of `(id, data)` leaves the key describing a column that will not be
# emitted, so it must be cleared too, even though the surviving prefix would still order correctly.
echo "-- iceberg: a declared subset that omits a key column does not keep the metadata sorting key"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE ice15 (id Int64, data String) ENGINE = IcebergLocal('${ICE}15/', 'Parquet') ORDER BY (id, data)"
${CH} -q "EXPLAIN PIPELINE SELECT * FROM icebergLocal('${ICE}15/', 'Parquet') ORDER BY id" | grep -c PartialSortingTransform
${CH} -q "EXPLAIN PIPELINE SELECT id FROM icebergLocal('${ICE}15/', 'Parquet', 'id Int64') ORDER BY id" | grep -c PartialSortingTransform

# A cluster initiator injects its own resolved columns into the remote query whether or not the user
# passed a structure, so the worker still sees a structure and must keep the key those columns match.
# `icebergLocalCluster` cannot express this (its argument injection is a no-op), so this arm goes
# through S3. The oracle has to be the WORKER's plan: an initiator-side EXPLAIN cannot see it, and a
# value or type oracle reads the same on both sides.
echo "-- iceberg cluster: a worker keeps the metadata sorting key when only the initiator injected a structure"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE ice14 (id Int64, data String) ENGINE = IcebergS3(s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/') ORDER BY id"
QID_NOSTRUCT="04748-nostruct-${CLICKHOUSE_DATABASE}"
QID_STRUCT="04748-struct-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_NOSTRUCT}" -q "SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet') ORDER BY id SETTINGS optimize_read_in_order = 1" > /dev/null
${CH} --query_id="${QID_STRUCT}" -q "SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='id Nullable(Int64)') ORDER BY id SETTINGS optimize_read_in_order = 1" > /dev/null
${CH} -q "SYSTEM FLUSH LOGS processors_profile_log"
# Worker rows only: query_id differs from initial_query_id on a secondary query. No sort on the
# worker means the key survived and read-in-order was taken there. Asserted for this query id
# first, so that a count of zero sorts cannot be read from an absence of worker rows.
${CH} -q "SELECT count() > 0 FROM system.processors_profile_log WHERE initial_query_id = '${QID_NOSTRUCT}' AND query_id != initial_query_id"
${CH} -q "SELECT countIf(name = 'PartialSortingTransform') FROM system.processors_profile_log WHERE initial_query_id = '${QID_NOSTRUCT}' AND query_id != initial_query_id"
# The same read WITH a user structure still reaches the worker, which proves the arm above is not
# green merely because nothing was dispatched.
${CH} -q "SELECT count() > 0 FROM system.processors_profile_log WHERE initial_query_id = '${QID_STRUCT}' AND query_id != initial_query_id"

# Keeping the key and keeping the columns are two separate decisions: clearing the key must not also
# discard the declared columns, or the snapshot would overwrite them and a column that exists only in
# the declared structure would not be found on the shard. Declaring a column absent from the metadata
# is what shows the columns survive the clear.
echo "-- iceberg cluster: a worker keeps declared columns that the metadata does not have"
# Rows are required: on an empty table no reader runs, so the extraction that has to find `zzz` is
# never reached. The row count is part of the oracle because `groupArray` skips NULLs, so an empty
# array reads the same whether two default rows arrived or none did.
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO ice14 SELECT number, 'x' FROM numbers(2)"
${CH} -q "SELECT count(), countIf(zzz IS NULL), toTypeName(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='zzz Nullable(String)')"

# A DEFAULT clause survives only as long as the structure reaches the worker as text: the columns the
# initiator resolves carry names and types alone, so a structure rebuilt from them would read as
# `zzz UInt64` and fill zeros. Summing is the oracle, since a dropped clause still yields two rows.
echo "-- iceberg cluster: a worker honors a DEFAULT declared in the structure"
${CH} -q "SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='${CLICKHOUSE_DATABASE}_ice14/', format='Parquet', structure='zzz UInt64 DEFAULT 42')"

# A named collection and an explicit URL reach different argument-rewriting branches, and only the
# explicit one rewrites the key-value arguments in place, so both are covered. The non-cluster read
# is the reference: the two must agree, whichever branch rewrote the arguments.
echo "-- iceberg cluster: a DEFAULT declared next to an explicit URL is honored too"
${CH} -q "SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure='zzz UInt64 DEFAULT 42')"
${CH} -q "SELECT count(), sum(zzz) FROM icebergS3('http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure='zzz UInt64 DEFAULT 42')"

# A key-value value may be any constant expression, not just a literal, and it is evaluated before
# the arguments are rewritten. Reading it as a literal alone would treat it as absent.
echo "-- iceberg cluster: a DEFAULT in a non-literal structure expression is honored"
${CH} -q "SELECT count(), sum(zzz) FROM icebergS3Cluster('test_cluster_two_shards_localhost', 'http://localhost:11111/test/${CLICKHOUSE_DATABASE}_ice14/', 'test', 'testtest', format='Parquet', structure=concat('zzz UInt64 DEFAULT ', '42'))"

# The key describes the metadata schema, so a declared type that reorders the key column makes it
# unsound: read-in-order would then emit rows in the underlying numeric order while the user asked
# for a String. Rows 2, 9, 10 differ numerically and lexicographically, so the ORDER is the oracle.
# The fixture is committed because read-in-order additionally needs a per-file `sort_order_id`
# matching the table's `default-sort-order-id`, which ClickHouse's own writer leaves NULL; Spark
# writes 0, which is what this manifest carries.
echo "-- iceberg cluster: a declared type that reorders the key column is not ordered by the metadata key"
${CH} -q "SELECT groupArray(id) FROM (SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='iceberg_sorted_order_test/', format='Parquet', structure='id String') ORDER BY id)"
# Same table without a structure: still in its own correct order, so the arm above cannot pass
# merely because nothing was dispatched or the fixture is unsorted.
${CH} -q "SELECT groupArray(id) FROM (SELECT id FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename='iceberg_sorted_order_test/', format='Parquet') ORDER BY id)"

echo "-- iceberg cluster: DESC and SELECT report the same declared type"
${CH} -q "DESC icebergLocalCluster('test_cluster_two_shards_localhost', '${ICE}/', 'Parquet', 'c0 LowCardinality(String)')" | awk -F'\t' '{print $1, $2}'
${CH} -q "SELECT DISTINCT toTypeName(c0) FROM icebergLocalCluster('test_cluster_two_shards_localhost', '${ICE}/', 'Parquet', 'c0 LowCardinality(String)')"

echo "-- control: a cluster read without a structure argument still works"
${CH} -q "SELECT DISTINCT c0, toTypeName(c0) FROM icebergLocalCluster('test_cluster_two_shards_localhost', '${ICE}/', 'Parquet')"

echo "-- control: without a structure argument the metadata schema is still used"
${CH} -q "DESC paimonLocal('${PAIMON}')" | wc -l
${CH} -q "DESC paimonLocal('${PAIMON}')" | awk -F'\t' '$1=="f_int"{print $2}'
${CH} -q "SELECT toTypeName(f_int) FROM paimonLocal('${PAIMON}') LIMIT 1"
${CH} -q "SELECT count() FROM paimonLocal('${PAIMON}')"

echo "-- control: the persistent engine still takes its schema from the metadata"
${CH} --allow_experimental_paimon_storage_engine=1 -q "CREATE TABLE pt (f_int Int64) ENGINE = PaimonLocal('${PAIMON}', 'Parquet')"
${CH} -q "DESC pt" | awk -F'\t' '$1=="f_int"{print $2}'

echo "-- control: a column added externally stays visible on a table created with columns"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE t7 (c0 String) ENGINE = IcebergLocal('${ICE}7/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO TABLE FUNCTION icebergLocal('${ICE}7/', 'Parquet', 'c0 String') (c0) SELECT 'a'"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE u7 ENGINE = IcebergLocal('${ICE}7/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q "ALTER TABLE u7 ADD COLUMN c_ext Nullable(Int64)"
${CH} -q "DESC t7" | grep -c c_ext

echo "-- control: an INSERT writes with the authoritative iceberg schema, not the declared one"
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE t11 (c0 String) ENGINE = IcebergLocal('${ICE}11/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q "INSERT INTO TABLE FUNCTION icebergLocal('${ICE}11/', 'Parquet', 'c0 Int64') (c0) SELECT 7"
${CH} -q "SELECT c0, toTypeName(c0) FROM icebergLocal('${ICE}11/')"

# A value the declared type accepts and the iceberg schema does not: the INSERT must be rejected
# while converting, because the sink header comes from the iceberg schema. If the read-side policy
# leaked into writes, the INSERT would instead succeed and store a wrongly typed column.
${CH} --allow_experimental_insert_into_iceberg=1 -q "CREATE TABLE t12 (c0 Int64) ENGINE = IcebergLocal('${ICE}12/', 'Parquet')"
${CH} --allow_experimental_insert_into_iceberg=1 -q \
    "INSERT INTO TABLE FUNCTION icebergLocal('${ICE}12/', 'Parquet', 'c0 String') (c0) SELECT 'zzz'" 2>&1 \
    | grep -c "while converting source column"

echo "-- control: deltaLake is unchanged at its default"
${CH} -q "SELECT toTypeName(c0), c0 FROM deltaLakeLocal('${DELTA}', 'Parquet', 'c0 Nullable(Int64)') ORDER BY c0"

echo "-- control: deltaLake with an explicit schema reload still prefers the metadata schema"
${CH} -q "SELECT DISTINCT toTypeName(c1) FROM deltaLakeLocal('${DELTA}', 'Parquet', 'c1 String') SETTINGS delta_lake_reload_schema_for_consistency = 1"

${CH} -q "DROP TABLE IF EXISTS ice14"
rm -rf "${PAIMON}" "${DELTA}" "${ICE}" "${ICE}7" "${ICE}11" "${ICE}12" "${ICE}13" "${ICE}15" "${ICE}16" "${ICE}17" "${ICE}18"
