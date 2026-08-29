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
# A second paimon fixture, because the one above has no partition keys at all: with none, no partition
# key condition is ever built and a partition-pruning arm would read the same either way.
PAIMONP="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_paimonp"
DELTA="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta"
ICE="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_ice"
NEST="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_nest"
# Read-only committed fixture, shared like iceberg_sorted_order_test below.
EQDEL="deletes_db/eq_deletes_table"
mkdir -p "${USER_FILES_PATH}"
cp -r "${CUR_DIR}/data_minio/paimon_no_partition" "${PAIMON}"
cp -r "${CUR_DIR}/data_minio/paimon_all_types" "${PAIMONP}"
cp -r "${CUR_DIR}/data_delta_lake/struct_column_mapping" "${DELTA}"
cp -r "${CUR_DIR}/data_minio/iceberg_nested_sort_order" "${NEST}"

CH="${CLICKHOUSE_CLIENT}"

# Runs a group of queries in one client session. An unexpected error aborts the rest of the session,
# so an arm expecting one either carries a `serverError` hint (which the client consumes, leaving the
# session alive) or goes last in its group; an arm whose output is piped keeps its own client.
batch() { ${CH} -n; }

# Emits `<name> <type>` per column, so a DESC arm needs no pipe and can share a session.
DESCFMT="FORMAT CustomSeparated SETTINGS describe_compact_output = 1, format_custom_escaping_rule = 'Raw', format_custom_field_delimiter = ' ', format_custom_row_before_delimiter = '', format_custom_row_after_delimiter = '\n', format_custom_row_between_delimiter = ''"
# Same, without the compact projection, so a DESC arm can also show the default clause it reports.
DESCFULL="FORMAT CustomSeparated SETTINGS describe_compact_output = 0, format_custom_escaping_rule = 'Raw', format_custom_field_delimiter = ' ', format_custom_row_before_delimiter = '', format_custom_row_after_delimiter = '\n', format_custom_row_between_delimiter = ''"

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
-- A declaration carrying a column default is not authoritative for a lake read, so the lake schema
-- is used and a column only the declaration has is unknown, as it is without this feature.
SELECT groupArray(zzz) FROM icebergLocal('${ICE}16/', 'Parquet', 'zzz UInt64 DEFAULT 42'); -- { serverError UNKNOWN_IDENTIFIER }
-- DESC still reports the declaration, default clause included: that is the surface the read no longer
-- follows, and it is asserted so the divergence is recorded rather than implied.
DESC icebergLocal('${ICE}16/', 'Parquet', 'zzz UInt64 DEFAULT 42') ${DESCFULL};
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

# A subcolumn of a column the metadata DOES declare is not a missing column either: it reaches the
# read as one flat name with no storage parent recorded, while its value lives in the parent the block
# already carries. No structure argument is involved, so this is the evolved read's own contract.
echo "-- iceberg: a subcolumn of a column the metadata declares is not a missing column"
batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
CREATE TABLE ice20 (a Int64, t Tuple(x Int64)) ENGINE = IcebergLocal('${ICE}20/', 'Parquet');
INSERT INTO ice20 SELECT number, tuple(number + 100) FROM numbers(3);
CREATE TABLE ice21 (a Int64, t Tuple(x Int64)) ENGINE = IcebergLocal('${ICE}21/', 'Parquet');
INSERT INTO ice21 SELECT number, tuple(number + 100) FROM numbers(3);
ALTER TABLE ice20 RENAME COLUMN a TO renamed_a;
-- The parent carries the written values and the rename puts the data file's schema id behind the
-- current one, so neither arm below can pass for want of a value or of an evolved read.
SELECT groupArray(a), groupArray(t), count() FROM (SELECT renamed_a AS a, t FROM ice20 ORDER BY a);
-- The same subcolumn of the same shape without evolution reads its values, which is what makes the
-- rejection below specific to the evolved read rather than to the expression.
SELECT groupArray(x), count() FROM (SELECT t.x AS x FROM icebergLocal('${ICE}21/', 'Parquet') ORDER BY x);
SELECT groupArray(x), count() FROM (SELECT t.x AS x FROM icebergLocal('${ICE}20/', 'Parquet') ORDER BY x); -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
SQL

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
-- Column a is declared Nullable and reads back as the lake's Int64, which is what a set-aside
-- declaration means and what the values alone cannot show. It also makes the pin necessary rather than
-- tidying: the cluster rewrite plans the initiator on the declared types while a worker reads the lake's.
SELECT groupArray(a), count(), countIf(c IS NULL), any(toTypeName(a)) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64) DEFAULT 42') SETTINGS parallel_replicas_for_cluster_engines = 0;
-- A file written after the column was added carries real values, which the same declaration must
-- still return: an evolution-added column is emitted by the lake's own transform, so its values come
-- from the file whether or not the declaration also names a default.
INSERT INTO ice17 SELECT number + 10, number + 500 FROM numbers(2);
SELECT groupArray(c) FROM (SELECT c FROM icebergLocal('${ICE}17/', 'Parquet', 'c Nullable(UInt64) DEFAULT 42') WHERE c IS NOT NULL ORDER BY c);
SELECT '-- iceberg: a filtered column declared at another nullability is read after schema evolution';
-- The filter is on a column the file has while an evolution-added one is also returned, and WHERE
-- and PREWHERE are both covered because only the latter is re-applied after the transform.
SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64)') WHERE a < 3;
SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Nullable(Int64), c Nullable(UInt64)') PREWHERE a < 3 SETTINGS parallel_replicas_for_cluster_engines = 0;
-- The declared type matching the file's is the control: it shares this path but needs no conversion.
SELECT groupArray(a), count(), countIf(c IS NULL) FROM icebergLocal('${ICE}17/', 'Parquet', 'a Int64, c Nullable(UInt64)') WHERE a < 3;
SELECT '-- iceberg: a declared DEFAULT is not applied to a table with deleted rows';
-- A lake decides row-level deletion from its own metadata against the values that reach the filter,
-- and a column default exists only where one reader's missing-value bitmask does, so the two cannot
-- both be served on one path. The declaration is left to the lake schema here.
CREATE TABLE ice18 (a Int64) ENGINE = IcebergLocal('${ICE}18/', 'Parquet');
INSERT INTO ice18 SELECT number FROM numbers(6);
DELETE FROM ice18 WHERE a = 2;
-- The delete must be visible here, or the arms below would read an undeleted table and pass either way.
SELECT count() FROM ice18;
SELECT groupArray(zzz) FROM icebergLocal('${ICE}18/', 'Parquet', 'zzz UInt64 DEFAULT 42'); -- { serverError UNKNOWN_IDENTIFIER }
-- Reading the same deleted table through a declaration of names and types alone is unaffected, so the
-- arm above fails for the default clause rather than for the delete.
SELECT groupArray(a) FROM (SELECT a FROM icebergLocal('${ICE}18/', 'Parquet', 'a Nullable(Int64)') ORDER BY a);
SELECT '-- iceberg: a declaration with a MATERIALIZED column stays rejected';
-- Nothing is rewritten when a declaration is set aside, so the columns a declaration may not carry at
-- all are still refused where they always were.
SELECT count() FROM icebergLocal('${ICE}18/', 'Parquet', 'a Int64, zzz UInt64 MATERIALIZED 1'); -- { serverError BAD_ARGUMENTS }
SELECT '-- iceberg: a defaulted declaration survives CREATE, DETACH and ATTACH unchanged';
-- ATTACH re-executes the stored table function, so this is the reachable proxy for a table persisted
-- by a version that read the declaration differently: it must still load and still read the same.
CREATE TABLE ice19 AS icebergLocal('${ICE}18/', 'Parquet', 'zzz UInt64 DEFAULT 42');
SELECT countIf(create_table_query LIKE '%DEFAULT 42%') FROM system.tables WHERE database = currentDatabase() AND name = 'ice19';
DETACH TABLE ice19;
ATTACH TABLE ice19;
SELECT countIf(create_table_query LIKE '%DEFAULT 42%') FROM system.tables WHERE database = currentDatabase() AND name = 'ice19';
-- The reattached table reads its rows, and reads them through the lake schema: the row count reflects
-- the delete, and the declared-only column is still unknown.
SELECT count() FROM ice19;
SELECT zzz FROM ice19; -- { serverError UNKNOWN_IDENTIFIER }
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

# Withholding manifest pruning is invisible in the values, so the only oracle for keeping it is the
# counter. This declaration differs from the metadata in nullability alone, which cannot reorder the
# values a bound is compared against, so the second file must still be pruned. Its own query id.
QID_PRUNE="04748-prune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_PRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}22/', 'Parquet', 'id Nullable(Int64), data String') WHERE id < 4" > /dev/null

# Low cardinality is the other wrapper that cannot reorder values, and each pruning site decides about
# it separately, so each one needs its own arm. This is the manifest site. The declaration is rejected
# outright without allow_suspicious_low_cardinality_types.
QID_LCPRUNE="04748-lcprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_LCPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}22/', 'Parquet', 'id LowCardinality(Int64), data String') WHERE id < 4 SETTINGS allow_suspicious_low_cardinality_types = 1" > /dev/null

# Same for the reader's own row-group pruning on a schema-evolved file, which is likewise invisible in
# the values: this read declares no structure, so a legal Iceberg promotion must not cost it. Manifest
# pruning is off so that the file reaches the reader at all, and it is the reader's counter that
# answers. Its own query id.
QID_RGPRUNE="04748-rgprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_RGPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}24/', 'Parquet') WHERE id > 4 SETTINGS use_iceberg_partition_pruning = 0" > /dev/null

# The declared type is what the arm above leaves untested: this one declares the evolved file's own
# column as Nullable(Int64) over the metadata's Int64. Nullability cannot reorder the values a bound is
# compared against, so the reader's row-group pruning has to survive a difference in it alone, and only
# the counter says whether it did. The bound excludes the single group entirely, and manifest pruning is
# off so the file reaches the reader rather than being discarded before it. Its own query id.
QID_WRAPPRUNE="04748-wrapprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_WRAPPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}23/', 'Parquet', 'id Nullable(Int64), data String, extra Nullable(String)') WHERE id < 5 SETTINGS use_iceberg_partition_pruning = 0" > /dev/null

# The reader site's own decision about low cardinality, which is separate from the manifest site's.
QID_LCWRAPPRUNE="04748-lcwrapprune-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_LCWRAPPRUNE}" -q "SELECT count() FROM icebergLocal('${ICE}23/', 'Parquet', 'id LowCardinality(Int64), data String, extra Nullable(String)') WHERE id < 5 SETTINGS use_iceberg_partition_pruning = 0, allow_suspicious_low_cardinality_types = 1" > /dev/null

# The paimon partition key is typed by its metadata, so the same nullability-only difference must not
# cost it its pruning either. Withholding is invisible in the values, and the count to compare against
# is the same read with pruning off rather than a constant, so this takes a pair of query ids. Both
# counts were measured to be the single value 1 and 10 across max_threads 1..16 and three block sizes.
QID_PPRUNE="04748-pprune-${CLICKHOUSE_DATABASE}"
QID_PPRUNEOFF="04748-ppruneoff-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_PPRUNE}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Nullable(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0" > /dev/null
${CH} --query_id="${QID_PPRUNEOFF}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Nullable(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0" > /dev/null

# The paimon site's own decision about low cardinality, the third of the three. Same relative oracle.
QID_LCPPRUNE="04748-lcpprune-${CLICKHOUSE_DATABASE}"
QID_LCPPRUNEOFF="04748-lcppruneoff-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_LCPPRUNE}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn LowCardinality(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0, allow_suspicious_low_cardinality_types = 1" > /dev/null
${CH} --query_id="${QID_LCPPRUNEOFF}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn LowCardinality(Int64)') WHERE f_bigint_nn < 2 SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0, allow_suspicious_low_cardinality_types = 1" > /dev/null

# A column outside the partition keys is not part of the key the bounds are compared against, so
# retyping one cannot mis-prune and must not cost the partition key its pruning. The filter has to name
# both, or the retyped column never reaches the filter's required columns and the arm reads the same
# either way. Its own pair of query ids.
QID_PPRUNENK="04748-pprunenk-${CLICKHOUSE_DATABASE}"
QID_PPRUNENKOFF="04748-pprunenkoff-${CLICKHOUSE_DATABASE}"
${CH} --query_id="${QID_PPRUNENK}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Int64, f_int String') WHERE f_bigint_nn < 2 AND f_int < '5' SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0" > /dev/null
${CH} --query_id="${QID_PPRUNENKOFF}" -q "SELECT count() FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn Int64, f_int String') WHERE f_bigint_nn < 2 AND f_int < '5' SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0" > /dev/null

batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
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
SELECT '-- iceberg: a declaration differing only in nullability still prunes manifest entries';
SYSTEM FLUSH LOGS query_log;
-- Every query_log lookup below is scoped to this run's database as well as to its query id,
-- because a query id is only unique within a run.
SELECT max(ProfileEvents['IcebergMinMaxIndexPrunedFiles']) > 0 FROM system.query_log WHERE query_id = '${QID_PRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- iceberg: a declaration differing only in low cardinality still prunes manifest entries';
SELECT max(ProfileEvents['IcebergMinMaxIndexPrunedFiles']) > 0 FROM system.query_log WHERE query_id = '${QID_LCPRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- iceberg: an undeclared read of a promoted column still prunes row groups';
SELECT max(ProfileEvents['ParquetPrunedRowGroups']) > 0 FROM system.query_log WHERE query_id = '${QID_RGPRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- iceberg: a declaration differing only in nullability still prunes row groups';
SELECT max(ProfileEvents['ParquetPrunedRowGroups']) > 0 FROM system.query_log WHERE query_id = '${QID_WRAPPRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- iceberg: a declaration differing only in low cardinality still prunes row groups';
SELECT max(ProfileEvents['ParquetPrunedRowGroups']) > 0 FROM system.query_log WHERE query_id = '${QID_LCWRAPPRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase();
SELECT '-- paimon: a declaration differing only in nullability still prunes partitions';
-- A missing query id makes max() NULL, so an absent read reports \N rather than passing.
SELECT (SELECT max(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE query_id = '${QID_PPRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase())
     < (SELECT max(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE query_id = '${QID_PPRUNEOFF}' AND type = 'QueryFinish' AND current_database = currentDatabase());
SELECT '-- paimon: a declaration differing only in low cardinality still prunes partitions';
SELECT (SELECT max(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE query_id = '${QID_LCPPRUNE}' AND type = 'QueryFinish' AND current_database = currentDatabase())
     < (SELECT max(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE query_id = '${QID_LCPPRUNEOFF}' AND type = 'QueryFinish' AND current_database = currentDatabase());
SELECT '-- paimon: retyping a column outside the partition keys still prunes partitions';
SELECT (SELECT max(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE query_id = '${QID_PPRUNENK}' AND type = 'QueryFinish' AND current_database = currentDatabase())
     < (SELECT max(ProfileEvents['EngineFileLikeReadFiles']) FROM system.query_log WHERE query_id = '${QID_PPRUNENKOFF}' AND type = 'QueryFinish' AND current_database = currentDatabase());
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
SELECT '-- paimon: a retyped partition key is not pruned against the metadata-typed partition key';
-- The declared String puts 1000 below '2' while the metadata Int64 does not, so a partition dropped on
-- the metadata typing loses a matching row. Both settings and a metadata-typed oracle must agree, and
-- f_bigint_nn is one of this fixture's twenty partition keys.
SELECT groupArray(f_bigint_nn) FROM (SELECT f_bigint_nn FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn String') WHERE f_bigint_nn < '2' ORDER BY f_bigint_nn) SETTINGS use_paimon_partition_pruning = 1, parallel_replicas_for_cluster_engines = 0;
SELECT groupArray(f_bigint_nn) FROM (SELECT f_bigint_nn FROM paimonLocal('${PAIMONP}', 'Parquet', 'f_bigint_nn String') WHERE f_bigint_nn < '2' ORDER BY f_bigint_nn) SETTINGS use_paimon_partition_pruning = 0, parallel_replicas_for_cluster_engines = 0;
SELECT groupArray(s) FROM (SELECT toString(f_bigint_nn) AS s FROM paimonLocal('${PAIMONP}') WHERE toString(f_bigint_nn) < '2' ORDER BY s);
SELECT '-- iceberg: a retyped identity partition column is read as declared';
-- An identity partition value comes from the manifest, and the injection replaces the column even
-- where the writer did store it in the file. The value is extracted at the metadata type. A plain
-- WHERE reaches both the filter substitution and the reader injection; only an explicit PREWHERE
-- isolates the substitution, and it needs the cluster rewrite off because that wrapper rejects
-- PREWHERE. Ids 2 and 10 order differently as numbers and as strings, so both rows are below '4' but
-- only one is below 4.
CREATE TABLE ice25 (id Int64, data String) ENGINE = IcebergLocal('${ICE}25/', 'Parquet') PARTITION BY id;
INSERT INTO ice25 VALUES (2, 'two');
INSERT INTO ice25 VALUES (10, 'ten');
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${ICE}25/', 'Parquet', 'id String, data String') ORDER BY id);
SELECT groupArray(data) FROM (SELECT data FROM icebergLocal('${ICE}25/', 'Parquet', 'id String, data String') WHERE id < '4' ORDER BY data) SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SELECT groupArray(data) FROM (SELECT data FROM icebergLocal('${ICE}25/', 'Parquet', 'id String, data String') PREWHERE id < '4' ORDER BY data) SETTINGS parallel_replicas_for_cluster_engines = 0;
-- Reading it with no declaration, and with one that agrees, are the pair: both must be unchanged, or
-- the arms above could pass with the partition value dropped rather than converted.
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${ICE}25/', 'Parquet') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${ICE}25/', 'Parquet', 'id Int64, data String') WHERE id < 4 ORDER BY id);
SELECT '-- iceberg: a retyped date identity partition column reads as the date, not as its day count';
-- A date partition value is held as a day count, so rendering it instead of converting it would give
-- that number. Only a date fixture separates the two, since an integer column renders the same either
-- way. The undeclared read is the pair, and shows which date each day count denotes.
CREATE TABLE ice26 (d Date, data String) ENGINE = IcebergLocal('${ICE}26/', 'Parquet') PARTITION BY d;
INSERT INTO ice26 VALUES ('2024-01-02', 'a');
INSERT INTO ice26 VALUES ('2024-01-10', 'b');
SELECT groupArray(d) FROM (SELECT d FROM icebergLocal('${ICE}26/', 'Parquet', 'd String, data String') ORDER BY d);
SELECT groupArray(d) FROM (SELECT d FROM icebergLocal('${ICE}26/', 'Parquet') ORDER BY d);
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
rm -rf "${PAIMON}" "${PAIMONP}" "${DELTA}" "${NEST}" "${ICE}" "${ICE}7" "${ICE}11" "${ICE}12" "${ICE}13" "${ICE}15" "${ICE}16" "${ICE}17" "${ICE}18" "${ICE}20" "${ICE}21" "${ICE}22" "${ICE}23" "${ICE}24" "${ICE}25" "${ICE}26"
