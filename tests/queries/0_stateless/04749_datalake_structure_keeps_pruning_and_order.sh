#!/usr/bin/env bash
# Tags: no-fasttest
# Random settings limits: optimize_read_in_order=(1, None)
# Tag no-fasttest: Depends on Avro and Parquet
# The clamp above pins optimize_read_in_order because the sorting-key arms assert whether
# read-in-order was taken, which optimize_read_in_order=0 disables outright. Everything else
# stays randomized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The paimon fixture here has partition keys, unlike paimon_no_partition: with none, no partition
# key condition is ever built and a partition-pruning arm would read the same either way.
PAIMONP="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_paimonp"
ICE="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_ice"
NEST="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_nest"
mkdir -p "${USER_FILES_PATH}"
cp -r "${CUR_DIR}/data_minio/paimon_all_types" "${PAIMONP}"
cp -r "${CUR_DIR}/data_minio/iceberg_nested_sort_order" "${NEST}"

CH="${CLICKHOUSE_CLIENT}"

# Runs a group of queries in one client session. An unexpected error aborts the rest of the session,
# so an arm expecting one either carries a `serverError` hint (which the client consumes, leaving the
# session alive) or goes last in its group; an arm whose output is piped keeps its own client.
batch() { ${CH} -n; }

batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
SELECT '-- iceberg: a retyped filter column does not prune manifest entries by the metadata type';
-- Two files, ids 0..3 and 10..13. Under the declared String every id is below '4'; under the lake
-- Int64 only four are, so a manifest bound compared at the lake type drops the second file and the
-- four rows it holds. Needs no deletes and no explicit PREWHERE, so the whole table is the oracle.
CREATE TABLE ice22 (id Int64, data String) ENGINE = IcebergLocal('${ICE}22/', 'Parquet');
INSERT INTO ice22 SELECT number, concat('r', toString(number)) FROM numbers(4);
INSERT INTO ice22 SELECT number + 10, concat('r', toString(number + 10)) FROM numbers(4);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${ICE}22/', 'Parquet', 'id String, data String') WHERE id < '4' ORDER BY id);
-- Declaring the lake's own type instead is the pair: the bound then means what the metadata means,
-- so the answer is the four rows below 4 rather than all eight. Whether an entry was pruned to reach
-- it is not observable here, and is left to the counter arm further down.
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${ICE}22/', 'Parquet', 'id Int64, data String') WHERE id < 4 ORDER BY id);
SELECT '-- iceberg: a retyped filter column does not prune row groups by the file type on an evolved file';
-- ice22 above is written under the current schema, where a file's own type is also the lake's
-- current one. An unrelated ADD COLUMN makes this file schema-evolved instead, so the two part
-- company: the reader still prunes row groups and pages from the query filter, and a declared String
-- bound compared against the file's Int64 statistics discards the group holding every matching row.
-- The ids are 10..13 so lexicographic and numeric order disagree against '4'; single-digit ids order
-- alike and the arm could not fail.
CREATE TABLE ice23 (id Int64, data String) ENGINE = IcebergLocal('${ICE}23/', 'Parquet');
INSERT INTO ice23 SELECT number + 10, concat('r', toString(number + 10)) FROM numbers(4);
ALTER TABLE ice23 ADD COLUMN extra Nullable(String);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${ICE}23/', 'Parquet', 'id String, data String, extra Nullable(String)') WHERE id < '4' ORDER BY id);
-- A legal promotion with no declaration at all is the contrast, and it must keep pruning. Rows 0..4
-- are written required, then the column is relaxed to optional and 5 and NULL are added, so a bound
-- of 4 can only prune the first file. Manifest pruning would reach that file first and leave the
-- reader nothing to decide, so it is off here and the counter arm below reads the reader's own work.
CREATE TABLE ice24 (id Int64, s String) ENGINE = IcebergLocal('${ICE}24/', 'Parquet');
INSERT INTO ice24 SELECT number, concat('v', toString(number)) FROM numbers(5);
ALTER TABLE ice24 MODIFY COLUMN id Nullable(Int64);
INSERT INTO ice24 VALUES (5, 'five'), (NULL, 'none');
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
SELECT '-- iceberg: a nested sorting key is recognized as present when the declaration keeps its parent';
-- A sort order may point at a nested field, and the key expression then names the subcolumn (\`t.x\`),
-- so deciding whether the key still describes the declared columns has to resolve subcolumns and not
-- just top-level names. The fixture is committed because ClickHouse's own writer records source-id 0
-- for a nested ORDER BY, which reads back as an empty column name.
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT id, t FROM icebergLocal('${NEST}/', 'Parquet', 'id Int64, t Tuple(x Int64)') ORDER BY t.x);
-- Reading it without a declaration keeps the key too, so the arm above cannot pass because the fixture
-- is unsorted or unreadable.
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT id, t FROM icebergLocal('${NEST}/', 'Parquet') ORDER BY t.x);
-- Omitting the nested key's parent entirely, and declaring that parent with a nested type the key
-- cannot be ordered by: both leave the key describing something the read will not emit.
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT id FROM icebergLocal('${NEST}/', 'Parquet', 'id Int64') ORDER BY id);
SELECT countIf(explain LIKE '%PartialSortingTransform%') FROM (EXPLAIN PIPELINE SELECT id, t FROM icebergLocal('${NEST}/', 'Parquet', 'id Int64, t Tuple(x String)') ORDER BY t.x);
SQL

# Withholding manifest pruning is invisible in the values, so the only oracle for keeping it is the
# counter. This declaration differs from the metadata in nullability alone, which cannot reorder the
# values a bound is compared against, so the second file must still be pruned. Its own query id.
QID_PRUNE="04749-prune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_PRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}22/', 'Parquet', 'id Nullable(Int64), data String') WHERE id < 4" > /dev/null

# Low cardinality is the other wrapper that cannot reorder values, and each pruning site decides about
# it separately, so each one needs its own arm. This is the manifest site. The declaration is rejected
# outright without allow_suspicious_low_cardinality_types.
QID_LCPRUNE="04749-lcprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_LCPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}22/', 'Parquet', 'id LowCardinality(Int64), data String') WHERE id < 4 SETTINGS allow_suspicious_low_cardinality_types = 1" > /dev/null

# Same for the reader's own row-group pruning on a schema-evolved file, which is likewise invisible in
# the values: this read declares no structure, so a legal Iceberg promotion must not cost it. Manifest
# pruning is off so that the file reaches the reader at all, and it is the reader's counter that
# answers. Its own query id.
QID_RGPRUNE="04749-rgprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_RGPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}24/', 'Parquet') WHERE id > 4 SETTINGS use_iceberg_partition_pruning = 0" > /dev/null

# The declared type is what the arm above leaves untested: this one declares the evolved file's own
# column as Nullable(Int64) over the metadata's Int64. Nullability cannot reorder the values a bound is
# compared against, so the reader's row-group pruning has to survive a difference in it alone, and only
# the counter says whether it did. The bound excludes the single group entirely, and manifest pruning is
# off so the file reaches the reader rather than being discarded before it. Its own query id.
QID_WRAPPRUNE="04749-wrapprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_WRAPPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}23/', 'Parquet', 'id Nullable(Int64), data String, extra Nullable(String)') WHERE id < 5 SETTINGS use_iceberg_partition_pruning = 0" > /dev/null

# The reader site's own decision about low cardinality, which is separate from the manifest site's.
QID_LCWRAPPRUNE="04749-lcwrapprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_LCWRAPPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}23/', 'Parquet', 'id LowCardinality(Int64), data String, extra Nullable(String)') WHERE id < 5 SETTINGS use_iceberg_partition_pruning = 0, allow_suspicious_low_cardinality_types = 1" > /dev/null

# The paimon partition key is typed by its metadata, so the same nullability-only difference must not
# cost it its pruning either. Withholding is invisible in the values, and the count to compare against
# is the same read with pruning off rather than a constant, so this takes a pair of query ids. Both
# counts were measured to be the single value 1 and 10 across max_threads 1..16 and three block sizes.
QID_PPRUNE="04749-pprune-${CLICKHOUSE_DATABASE}"
QID_PPRUNEOFF="04749-ppruneoff-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_PPRUNE}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Nullable(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0" > /dev/null
${CH} --query_id="${QID_PPRUNEOFF}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Nullable(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0" > /dev/null

# The paimon site's own decision about low cardinality, the third of the three. Same relative oracle.
QID_LCPPRUNE="04749-lcpprune-${CLICKHOUSE_DATABASE}"
QID_LCPPRUNEOFF="04749-lcppruneoff-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_LCPPRUNE}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn LowCardinality(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0, allow_suspicious_low_cardinality_types = 1" > /dev/null
${CH} --query_id="${QID_LCPPRUNEOFF}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn LowCardinality(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0, allow_suspicious_low_cardinality_types = 1" > /dev/null

# A column outside the partition keys is not part of the key the bounds are compared against, so
# retyping one cannot mis-prune and must not cost the partition key its pruning. The filter has to name
# both, or the retyped column never reaches the filter's required columns and the arm reads the same
# either way. Its own pair of query ids.
QID_PPRUNENK="04749-pprunenk-${CLICKHOUSE_DATABASE}"
QID_PPRUNENKOFF="04749-pprunenkoff-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_PPRUNENK}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Int64, f_int String') WHERE f_bigint_nn < 2 AND f_int < '5' SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0" > /dev/null
${CH} --query_id="${QID_PPRUNENKOFF}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Int64, f_int String') WHERE f_bigint_nn < 2 AND f_int < '5' SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0" > /dev/null
batch <<SQL
SELECT '-- iceberg: a declaration differing only in nullability still prunes manifest entries';
SYSTEM FLUSH LOGS query_log;
-- Every query_log lookup below is scoped to this run's database as well as to its query id,
-- because a query id is only unique within a run.
SELECT max(ProfileEvents['IcebergMinMaxIndexPrunedFiles']) > 0 FROM system.query_log WHERE query_id = '${QID_PRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- iceberg: a declaration differing only in low cardinality still prunes manifest entries';
SELECT max(ProfileEvents['IcebergMinMaxIndexPrunedFiles']) > 0 FROM system.query_log WHERE query_id = '${QID_LCPRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- iceberg: an undeclared read of a promoted column still prunes row groups';
-- Row-group pruning is counted where the reader runs, which is the secondary query whenever the
-- cluster rewrite wraps the table function, and a secondary query logs current_database as the
-- default one rather than the initiator's, so these three are scoped to the initial query id and
-- admit a row that is not the initial one. The manifest counters above are emitted while the
-- initiator lists files and stay on its own row.
SELECT max(ProfileEvents['ParquetPrunedRowGroups']) > 0 FROM system.query_log WHERE initial_query_id = '${QID_RGPRUNE}' AND type = 'QueryFinish'
  AND (current_database = currentDatabase() OR is_initial_query = 0);
SELECT '-- iceberg: a declaration differing only in nullability still prunes row groups';
SELECT max(ProfileEvents['ParquetPrunedRowGroups']) > 0 FROM system.query_log WHERE initial_query_id = '${QID_WRAPPRUNE}' AND type = 'QueryFinish'
  AND (current_database = currentDatabase() OR is_initial_query = 0);
SELECT '-- iceberg: a declaration differing only in low cardinality still prunes row groups';
SELECT max(ProfileEvents['ParquetPrunedRowGroups']) > 0 FROM system.query_log WHERE initial_query_id = '${QID_LCWRAPPRUNE}' AND type = 'QueryFinish'
  AND (current_database = currentDatabase() OR is_initial_query = 0);
SELECT '-- paimon: a declaration differing only in nullability still prunes partitions';
-- Each of the three arms below requires both reads to be present: max() over no rows is 0, not
-- NULL, so a pruning query that failed would compare as 0 and satisfy the inequality on its own.
SELECT countIf(query_id = '${QID_PPRUNE}') > 0 AND countIf(query_id = '${QID_PPRUNEOFF}') > 0
   AND maxIf(ProfileEvents['EngineFileLikeReadFiles'], query_id = '${QID_PPRUNE}')
     < maxIf(ProfileEvents['EngineFileLikeReadFiles'], query_id = '${QID_PPRUNEOFF}')
FROM system.query_log WHERE query_id IN ('${QID_PPRUNE}', '${QID_PPRUNEOFF}')
  AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- paimon: a declaration differing only in low cardinality still prunes partitions';
SELECT countIf(query_id = '${QID_LCPPRUNE}') > 0 AND countIf(query_id = '${QID_LCPPRUNEOFF}') > 0
   AND maxIf(ProfileEvents['EngineFileLikeReadFiles'], query_id = '${QID_LCPPRUNE}')
     < maxIf(ProfileEvents['EngineFileLikeReadFiles'], query_id = '${QID_LCPPRUNEOFF}')
FROM system.query_log WHERE query_id IN ('${QID_LCPPRUNE}', '${QID_LCPPRUNEOFF}')
  AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- paimon: retyping a column outside the partition keys still prunes partitions';
SELECT countIf(query_id = '${QID_PPRUNENK}') > 0 AND countIf(query_id = '${QID_PPRUNENKOFF}') > 0
   AND maxIf(ProfileEvents['EngineFileLikeReadFiles'], query_id = '${QID_PPRUNENK}')
     < maxIf(ProfileEvents['EngineFileLikeReadFiles'], query_id = '${QID_PPRUNENKOFF}')
FROM system.query_log WHERE query_id IN ('${QID_PPRUNENK}', '${QID_PPRUNENKOFF}')
  AND type = 'QueryFinish' AND current_database = currentDatabase();

SELECT '-- paimon: a retyped partition key is not pruned against the metadata-typed partition key';
-- The declared String puts 1000 below '2' while the metadata Int64 does not, so a partition dropped on
-- the metadata typing loses a matching row. Both settings and a metadata-typed oracle must agree, and
-- f_bigint_nn is one of this fixture's twenty partition keys.
SELECT groupArray(f_bigint_nn) FROM (SELECT f_bigint_nn FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn String') WHERE f_bigint_nn < '2' ORDER BY f_bigint_nn) SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0;
SELECT groupArray(f_bigint_nn) FROM (SELECT f_bigint_nn FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn String') WHERE f_bigint_nn < '2' ORDER BY f_bigint_nn) SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0;
SELECT groupArray(s) FROM (SELECT toString(f_bigint_nn) AS s FROM paimonLocal('${PAIMONP}') WHERE toString(f_bigint_nn) < '2' ORDER BY s);
SQL
rm -rf "${PAIMONP}" "${NEST}" "${ICE}13" "${ICE}15" "${ICE}22" "${ICE}23" "${ICE}24"
