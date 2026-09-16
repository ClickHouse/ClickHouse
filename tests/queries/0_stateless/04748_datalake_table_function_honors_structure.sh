#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: Depends on Avro and Parquet
# Tag no-msan: DeltaKernel is not compiled with msan

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
-- parallel_replicas_for_cluster_engines = 0 is required on an icebergLocal arm that needs PREWHERE,
-- not tidying: otherwise the table function is wrapped in StorageObjectStorageCluster, which does not
-- support PREWHERE, and the analyzer rejects the query before any reader runs. Scoped to these arms so
-- the cluster arms below keep their coverage. The same pin on a paimonLocal arm is inert, because the
-- rewrite fires only for a registered *Cluster name and paimonLocalCluster is not one; it is kept so
-- those arms are pinned the day it is registered.
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
SQL

batch <<SQL
SET allow_experimental_insert_into_iceberg = 1;
SELECT '-- iceberg: a retyped identity partition column is read as declared';
-- An identity partition value comes from the manifest, and the injection replaces the column even
-- where the writer did store it in the file. The value is extracted at the metadata type. A plain
-- WHERE reaches both the filter substitution and the reader injection; only an explicit PREWHERE
-- isolates the substitution, and it needs the cluster rewrite off because that wrapper rejects
-- PREWHERE. The plain-WHERE arm pins the rewrite off for the same reason: under the wrapper the mover
-- has no PREWHERE to move into, so its own pins would not reach the substitution. Ids 2 and 10 order
-- differently as numbers and as strings, so both rows are below '4' but only one is below 4.
CREATE TABLE ice25 (id Int64, data String) ENGINE = IcebergLocal('${ICE}25/', 'Parquet') PARTITION BY id;
INSERT INTO ice25 VALUES (2, 'two');
INSERT INTO ice25 VALUES (10, 'ten');
SELECT groupArray(id) FROM (SELECT id FROM icebergLocal('${ICE}25/', 'Parquet', 'id String, data String') ORDER BY id);
SELECT groupArray(data) FROM (SELECT data FROM icebergLocal('${ICE}25/', 'Parquet', 'id String, data String') WHERE id < '4' ORDER BY data) SETTINGS parallel_replicas_for_cluster_engines = 0, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
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
SQL
rm -rf "${PAIMON}" "${DELTA}" "${ICE}" "${ICE}7" "${ICE}11" "${ICE}12" "${ICE}16" "${ICE}17" "${ICE}18" "${ICE}20" "${ICE}21" "${ICE}25" "${ICE}26"
