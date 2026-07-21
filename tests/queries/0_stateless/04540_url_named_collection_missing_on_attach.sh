#!/usr/bin/env bash
# Tags: no-fasttest, long
# no-fasttest: relies on the local user_files directory for the file:// cases.
# long: exercises many DETACH/ATTACH-replay scenarios (each a separate reviewer-required case),
#       so a single run exceeds the 180s flaky-check limit; the coverage cannot be dropped.

# A URL(named_collection) table whose named collection was dropped (allowed via
# check_named_collection_dependencies=false) must still ATTACH: server startup replays ATTACH from
# metadata, and a hard NAMED_COLLECTION_DOESNT_EXIST there aborts the whole server. ATTACH TABLE
# (short form) replays metadata via the same code path as startup, so it exercises the fix without
# a real restart. The error must instead surface at query time.
#
# The deferral is done by attaching a lazy proxy that re-runs the full URL creator on first access,
# so once the collection is recreated the storage class (http vs file:// dispatch), hive
# partitioning, virtual columns and format inference are all rebuilt exactly as the eager path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# ---------------------------------------------------------------------------------------------
echo "--- http named collection: attach with the collection missing, error only at query time ---"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_http;
DROP TABLE IF EXISTS ${U}_http;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_http AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${U}_http (x UInt32) ENGINE = URL(${U}_nc_http);
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_http"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_http;"
# ATTACH must succeed (before the fix it threw NAMED_COLLECTION_DOESNT_EXIST and aborted startup).
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_http"
echo "attached"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${U}_http"
# The missing named collection surfaces at query time, not at attach time.
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${U}_http" 2>&1 | grep -o "NAMED_COLLECTION_DOESNT_EXIST" | head -1
# Recreating the collection makes the table usable again (query then fails only on the network).
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_http AS url = 'http://localhost:8123', format = 'CSV';"
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${U}_http" 2>&1 | grep -oE "NAMED_COLLECTION_DOESNT_EXIST" | head -1
echo "http resolved"

# ---------------------------------------------------------------------------------------------
echo "--- DROP while the named collection is still missing: metadata-only, like plain URL ---"
# A URL table drop removes only the table metadata: the plain URL/File backends have a no-op drop(),
# and a URL(nc) only ever dispatches to plain object storage (S3/Azure/HDFS) whose drop() is also a
# no-op (external objects are never deleted). So dropping while the collection is missing is safe and
# matches the non-deferred URL engine: DROP succeeds without needing the collection.
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_drop;
DROP TABLE IF EXISTS ${U}_drop;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_drop AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${U}_drop (x UInt32) ENGINE = URL(${U}_nc_drop);
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_drop"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_drop;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_drop"
# DROP succeeds while the collection is missing (metadata-only), no NAMED_COLLECTION_DOESNT_EXIST.
$CLICKHOUSE_CLIENT -q "DROP TABLE ${U}_drop"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${U}_drop"
echo "drop resolved"

# ---------------------------------------------------------------------------------------------
echo "--- file:// named collection: storage-class dispatch rebuilt on first use ---"
DATA1="${USER_FILES_PATH}/${U}_data1.csv"
printf '1,10\n2,20\n' > "$DATA1"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_file;
DROP TABLE IF EXISTS ${U}_file;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_file AS url = 'file://${DATA1}', format = 'CSV';
CREATE TABLE ${U}_file (a UInt32, b UInt32) ENGINE = URL(${U}_nc_file);
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_file"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_file;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_file"
echo "file attached"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_file AS url = 'file://${DATA1}', format = 'CSV';"
# Must return the file rows: the deferred path rebuilt the File delegate (not a plain HTTP StorageURL).
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${U}_file ORDER BY a FORMAT CSV"

# ---------------------------------------------------------------------------------------------
echo "--- hive partitioning: virtual columns rebuilt on first use ---"
mkdir -p "${USER_FILES_PATH}/${U}_hive/year=2021"
printf 'v1\nv2\n' > "${USER_FILES_PATH}/${U}_hive/year=2021/data.csv"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_hive;
DROP TABLE IF EXISTS ${U}_hive;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_hive AS url = 'file://${USER_FILES_PATH}/${U}_hive/year=2021/data.csv', format = 'CSV';
CREATE TABLE ${U}_hive (val String) ENGINE = URL(${U}_nc_hive) SETTINGS use_hive_partitioning = 1;
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_hive"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_hive;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_hive"
echo "hive attached"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_hive AS url = 'file://${USER_FILES_PATH}/${U}_hive/year=2021/data.csv', format = 'CSV';"
# The hive partition virtual column `year` must be present (rebuilt with the resolved sample path).
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT year FROM ${U}_hive SETTINGS use_hive_partitioning = 1"

# ---------------------------------------------------------------------------------------------
echo "--- implicit format: format inferred on first use, not left as auto ---"
DATA3="${USER_FILES_PATH}/${U}_data3.csv"
printf '5,50\n6,60\n' > "$DATA3"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_fmt;
DROP TABLE IF EXISTS ${U}_fmt;
"
# No explicit format in the collection: the normal path infers CSV from the .csv suffix.
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_fmt AS url = 'file://${DATA3}';
CREATE TABLE ${U}_fmt (a UInt32, b UInt32) ENGINE = URL(${U}_nc_fmt);
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_fmt"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_fmt;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_fmt"
echo "fmt attached"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_fmt AS url = 'file://${DATA3}';"
# Must NOT throw UNKNOWN_FORMAT (format=auto): the deferred path infers a concrete format.
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${U}_fmt ORDER BY a FORMAT CSV"

# ---------------------------------------------------------------------------------------------
echo "--- DDL while the named collection is still missing: URL semantics preserved ---"
# The deferred proxy must keep the plain URL engine's DDL contract while the collection is missing,
# instead of materializing the nested storage (which would throw NAMED_COLLECTION_DOESNT_EXIST):
#   - RENAME is metadata-only (URL rename never touches the external resource).
#   - ALTER ... MODIFY COMMENT is metadata-only (plain URL handles comment alters via IStorage).
#   - TRUNCATE TABLE surfaces NOT_IMPLEMENTED (the URL engine does not support truncate).
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_ddl;
DROP TABLE IF EXISTS ${U}_ddl;
DROP TABLE IF EXISTS ${U}_ddl2;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_ddl AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${U}_ddl (x UInt32) ENGINE = URL(${U}_nc_ddl);
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_ddl"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_ddl;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_ddl"
# RENAME succeeds without the collection (metadata-only), no NAMED_COLLECTION_DOESNT_EXIST.
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${U}_ddl TO ${U}_ddl2"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE ${U}_ddl2"
# ALTER ... MODIFY COMMENT succeeds without the collection (metadata-only), no NAMED_COLLECTION_DOESNT_EXIST.
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${U}_ddl2 MODIFY COMMENT 'deferred url comment'"
$CLICKHOUSE_CLIENT -q "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = '${U}_ddl2'"
# A non-metadata alter is rejected with NOT_IMPLEMENTED (the URL contract), not the missing collection.
$CLICKHOUSE_CLIENT -q "ALTER TABLE ${U}_ddl2 ADD COLUMN y UInt32" 2>&1 | grep -oE "NOT_IMPLEMENTED|NAMED_COLLECTION_DOESNT_EXIST" | head -1
# TRUNCATE surfaces NOT_IMPLEMENTED (URL contract), not NAMED_COLLECTION_DOESNT_EXIST.
$CLICKHOUSE_CLIENT -q "TRUNCATE TABLE ${U}_ddl2" 2>&1 | grep -oE "NOT_IMPLEMENTED|NAMED_COLLECTION_DOESNT_EXIST" | head -1
echo "ddl ok"

# ---------------------------------------------------------------------------------------------
echo "--- metadata-only DDL applied while missing survives materialization ---"
# The deferred proxy rebuilds the nested storage from the table's CURRENT create query on first
# access, not the attach-time clone. So the comment set by ALTER ... MODIFY COMMENT above must
# still be present after the collection is recreated and the table is materialized (first read).
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_ddl AS url = 'http://localhost:8123', format = 'CSV';"
# First read materializes the nested storage (query fails only on the network, not on the collection).
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${U}_ddl2" 2>&1 | grep -oE "NAMED_COLLECTION_DOESNT_EXIST" | head -1
$CLICKHOUSE_CLIENT -q "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = '${U}_ddl2'"
echo "comment survived"

# ---------------------------------------------------------------------------------------------
echo "--- rename while missing re-registers the dependency under the current name (Ordinary DB) ---"
# On an Ordinary database the named-collection dependency is keyed by table name (no UUID).
# After a real restart the dependency graph starts empty and the deferred URL(nc) path registers the
# dependency at ATTACH time (DETACH PERMANENTLY below removes it first to simulate that). So:
#   - DROP NAMED COLLECTION with check_named_collection_dependencies=1 must be rejected BEFORE any
#     read (the dependency exists from attach, not only after first materialization), and
#   - a metadata-only RENAME while the collection is missing moves the dependency to the CURRENT
#     name (renameDependencies), so the rejection names dep_t2, not the stale attach-time dep_t.
ODB="${U}_odb"
# --send_logs_level=none: creating an Ordinary database emits a deprecation warning to stderr.
$CLICKHOUSE_CLIENT --send_logs_level=none --allow_deprecated_database_ordinary=1 -m -q "
DROP DATABASE IF EXISTS ${ODB};
CREATE DATABASE ${ODB} ENGINE = Ordinary;
"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_dep;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_dep AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${ODB}.dep_t (x UInt32) ENGINE = URL(${U}_nc_dep);
"
# DETACH PERMANENTLY removes the named-collection dependency (like a real restart, where the graph
# starts empty), so the subsequent ATTACH must re-establish it. Plain DETACH keeps the dependency in
# memory and would mask whether ATTACH re-registers it.
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${ODB}.dep_t PERMANENTLY"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_dep;"
# ATTACH registers the dependency at attach time (before any read), even though the collection is
# missing. Recreating and dropping the collection again WITHOUT reading the table must be REJECTED.
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ODB}.dep_t"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_dep AS url = 'http://localhost:8123', format = 'CSV';"
# No read has happened yet: the dependency must already be tracked (registered at attach time).
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${U}_nc_dep" 2>&1 | grep -oE "NAMED_COLLECTION_IS_USED|NAMED_COLLECTION_DOESNT_EXIST" | head -1
echo "dependency tracked before first read"
# Drop the collection again and RENAME (metadata-only) while it is missing. The rename must move the
# dependency to the new name (renameDependencies), so DROP is still rejected under dep_t2 -- again
# without any read.
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_dep;"
$CLICKHOUSE_CLIENT -q "RENAME TABLE ${ODB}.dep_t TO ${ODB}.dep_t2"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_dep AS url = 'http://localhost:8123', format = 'CSV';"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${U}_nc_dep" 2>&1 | grep -oE "NAMED_COLLECTION_IS_USED|NAMED_COLLECTION_DOESNT_EXIST" | head -1
# The rejection names the CURRENT table (dep_t2), not the stale attach-time name (dep_t).
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${U}_nc_dep" 2>&1 | grep -oE "dep_t2|dep_t" | head -1
echo "dependency tracked under new name"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS ${ODB}.dep_t2;
DROP DATABASE IF EXISTS ${ODB};
DROP NAMED COLLECTION IF EXISTS ${U}_nc_dep;
"

# ---------------------------------------------------------------------------------------------
echo "--- table-level constraints and comment seeded on a deferred (unresolved) table ---"
# The deferred proxy must seed the table-level constraints and comment into its cached metadata, like
# the eager IStorageURLBase / StorageURLSchemeDispatch constructors. Seeding only columns would let the
# first INSERT (which snapshots getInMemoryMetadataPtr() before write() materializes the proxy) skip
# every CHECK constraint, and would make system.tables.comment / system.constraints wrong until some
# query materializes the storage.
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_con;
DROP TABLE IF EXISTS ${U}_con;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_con AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${U}_con (x UInt32, CONSTRAINT c_pos CHECK x > 100) ENGINE = URL(${U}_nc_con) COMMENT 'deferred url with constraint';
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_con"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_con;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_con"
# The comment must be visible from metadata before any materialization.
$CLICKHOUSE_CLIENT -q "SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = '${U}_con'"
# Recreate the collection so the table becomes usable, then the first INSERT must enforce the CHECK
# constraint. InterpreterInsertQuery snapshots getInMemoryMetadataPtr() before write() materializes the
# proxy, so CheckConstraintsTransform is built from the deferred metadata: with the constraints seeded, a
# violating row throws VIOLATED_CONSTRAINT before the backend write. Without the seeding the constraint
# would be absent from the snapshot and the row would silently reach the URL backend.
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_con AS url = 'http://localhost:8123', format = 'CSV';"
$CLICKHOUSE_CLIENT -q "INSERT INTO ${U}_con VALUES (5)" 2>&1 | grep -oE "VIOLATED_CONSTRAINT|NAMED_COLLECTION_DOESNT_EXIST" | head -1
echo "constraint and comment seeded"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS ${U}_con;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_con;
"

# ---------------------------------------------------------------------------------------------
echo "--- drop of a deferred URL(nc) does not resurrect the named-collection dependency (Ordinary DB) ---"
# DROP TABLE scrubs the named-collection dependency before dropTable() -> StorageTableProxy::drop().
# The deferred proxy must NOT materialize on drop (which would re-run the creator and re-register the
# dependency for a table already being dropped). On an Ordinary DB that resurrected name-based entry
# would survive and make a later unrelated DROP NAMED COLLECTION wrongly fail with
# NAMED_COLLECTION_IS_USED. So: drop the deferred table (collection recreated so it is droppable
# normally), then a DROP NAMED COLLECTION with check_named_collection_dependencies=1 must SUCCEED.
ODB2="${U}_odb2"
$CLICKHOUSE_CLIENT --send_logs_level=none --allow_deprecated_database_ordinary=1 -m -q "
DROP DATABASE IF EXISTS ${ODB2};
CREATE DATABASE ${ODB2} ENGINE = Ordinary;
"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_dd;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_dd AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${ODB2}.dd_t (x UInt32) ENGINE = URL(${U}_nc_dd);
"
# DETACH PERMANENTLY clears the dependency (like a restart); ATTACH re-registers it at attach time.
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${ODB2}.dd_t PERMANENTLY"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_dd;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${ODB2}.dd_t"
# Recreate the collection and DROP the deferred table (never read, so still unmaterialized).
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_dd AS url = 'http://localhost:8123', format = 'CSV';"
$CLICKHOUSE_CLIENT -q "DROP TABLE ${ODB2}.dd_t"
# Create an UNRELATED table with the SAME name that does NOT reference the collection. If the drop
# above had materialized the deferred proxy and re-registered the dependency, the stale name-based
# entry (cr9_nc -> dd_t) would now match this new table and DROP NAMED COLLECTION would wrongly fail
# with NAMED_COLLECTION_IS_USED. It must SUCCEED: the drop must not resurrect the dependency.
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${ODB2}.dd_t (y UInt32) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${U}_nc_dd" 2>&1 | grep -oE "NAMED_COLLECTION_IS_USED" | head -1
echo "drop did not resurrect dependency"
$CLICKHOUSE_CLIENT --send_logs_level=none -m -q "
SET check_named_collection_dependencies = false;
DROP DATABASE IF EXISTS ${ODB2};
DROP NAMED COLLECTION IF EXISTS ${U}_nc_dd;
"

# ---------------------------------------------------------------------------------------------
echo "--- URL virtual columns visible on a deferred (unresolved) table ---"
# Metadata introspection resolves columns from getInMemoryMetadataPtr() without materializing the
# proxy, so the deferred table must expose the URL file-like virtuals up front. Otherwise the URL
# virtuals (_path/_file/_size/_time/_headers) are invisible until some other query materializes the
# storage first. DESCRIBE with describe_include_virtual_columns=1 lists them from metadata directly:
# before the fix it showed only the real column; after it lists the URL virtuals too.
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_virt;
DROP TABLE IF EXISTS ${U}_virt;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_virt AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${U}_virt (x UInt32) ENGINE = URL(${U}_nc_virt);
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_virt"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_virt;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_virt"
# The URL file-like virtuals must be present on the unresolved table (from metadata, no materialization).
$CLICKHOUSE_CLIENT -q "DESCRIBE TABLE ${U}_virt SETTINGS describe_include_virtual_columns = 1" 2>&1 | grep -oE "^(_path|_file|_size|_time|_headers)\b" | sort -u | paste -sd' '
echo "virtuals visible"

# ---------------------------------------------------------------------------------------------
echo "--- named-collection dependency stays tracked across first-read materialization ---"
# The deferred creator hands off from the attach-time dependency to the one re-registered by the
# eager creator. The handoff must not leave a zero-dependent gap: addDependency is idempotent and the
# attach-time placeholder is kept in place, so check_named_collection_dependencies sees the table as a
# dependent both before AND after the first read materializes it. Verify that after a successful first
# read (which runs the handoff), DROP NAMED COLLECTION is still rejected.
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_track;
DROP TABLE IF EXISTS ${U}_track;
"
$CLICKHOUSE_CLIENT -m -q "
CREATE NAMED COLLECTION ${U}_nc_track AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE ${U}_track (x UInt32) ENGINE = URL(${U}_nc_track);
"
$CLICKHOUSE_CLIENT -q "DETACH TABLE ${U}_track"
$CLICKHOUSE_CLIENT -m -q "SET check_named_collection_dependencies = false; DROP NAMED COLLECTION ${U}_nc_track;"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE ${U}_track"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${U}_nc_track AS url = 'http://localhost:8123', format = 'CSV';"
# First read materializes the storage (runs the dependency handoff); it fails only on the network.
$CLICKHOUSE_CLIENT -q "SELECT * FROM ${U}_track" 2>&1 | grep -oE "NAMED_COLLECTION_DOESNT_EXIST" | head -1
# After the handoff the dependency must still be tracked (not dropped to zero): DROP is rejected.
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION ${U}_nc_track" 2>&1 | grep -oE "NAMED_COLLECTION_IS_USED|NAMED_COLLECTION_DOESNT_EXIST" | head -1
echo "dependency tracked after materialization"
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS ${U}_track;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_track;
DROP TABLE IF EXISTS ${U}_virt;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_virt;
"

# ---------------------------------------------------------------------------------------------
$CLICKHOUSE_CLIENT -m -q "
SET check_named_collection_dependencies = false;
DROP TABLE IF EXISTS ${U}_http;
DROP TABLE IF EXISTS ${U}_file;
DROP TABLE IF EXISTS ${U}_hive;
DROP TABLE IF EXISTS ${U}_fmt;
DROP TABLE IF EXISTS ${U}_drop;
DROP TABLE IF EXISTS ${U}_ddl2;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_ddl;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_drop;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_http;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_file;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_hive;
DROP NAMED COLLECTION IF EXISTS ${U}_nc_fmt;
"
rm -f "$DATA1" "$DATA3"
rm -rf "${USER_FILES_PATH}/${U}_hive"
