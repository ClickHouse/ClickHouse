#!/usr/bin/env bash
# Tags: no-fasttest, no-darwin
# Tag no-fasttest: Kafka is not in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# clickhouse-local refuses --dump-schema with a file, pipe or socket on stdin, whatever the test runner passes.
exec < /dev/null

DB="${CLICKHOUSE_DATABASE}"
ERR_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_err.txt"
KEEPER_GATE='SET allow_experimental_kafka_offsets_storage_in_keeper = 1;'

echo '--- the Kafka Keeper-offsets gate follows its carriers ---'
KAFKA_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka"
KAFKA_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka.sql"
rm -rf "$KAFKA_PATH"
$CLICKHOUSE_LOCAL --path "$KAFKA_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.plain_kafka (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow');
"
$CLICKHOUSE_LOCAL --path "$KAFKA_PATH" --dump-schema="${DB}" > "$KAFKA_DUMP_FILE" 2>"$ERR_FILE"
echo "plain Kafka, keeper gate emitted: $(grep -c "$KEEPER_GATE" "$KAFKA_DUMP_FILE")"

# Empty values store no offsets in Keeper, but the settings still name the carrier.
$CLICKHOUSE_LOCAL --path "$KAFKA_PATH" --query "
CREATE TABLE ${DB}.keeper_kafka (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow')
    SETTINGS kafka_keeper_path = '', kafka_replica_name = ''
"
$CLICKHOUSE_LOCAL --path "$KAFKA_PATH" --dump-schema="${DB}" > "$KAFKA_DUMP_FILE" 2>"$ERR_FILE"
echo "Kafka with Keeper settings, keeper gate emitted: $(grep -c "$KEEPER_GATE" "$KAFKA_DUMP_FILE")"

# A named collection can carry the Keeper settings, and the dump cannot see into it.
$CLICKHOUSE_LOCAL --path "$KAFKA_PATH" --multiquery --query "
DROP TABLE ${DB}.keeper_kafka;
CREATE NAMED COLLECTION kafka_nc AS kafka_broker_list = '127.0.0.1:9092', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow';
CREATE TABLE ${DB}.nc_kafka (x Int64) ENGINE = Kafka(kafka_nc);
"
$CLICKHOUSE_LOCAL --path "$KAFKA_PATH" --dump-schema="${DB}" > "$KAFKA_DUMP_FILE" 2>"$ERR_FILE"
echo "Kafka over a named collection, keeper gate emitted: $(grep -c "$KEEPER_GATE" "$KAFKA_DUMP_FILE")"
rm -rf "$KAFKA_PATH" "$KAFKA_DUMP_FILE"

echo '--- a plain Kafka dump replays under a Keeper-offsets constraint ---'
CONSTRAINT_DB="${DB}_kafka_constraint"
CONSTRAINT_USER="${DB}_kafka_constraint_user"
CONSTRAINT_PROFILE="${DB}_kafka_constraint_profile"
CONSTRAINT_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_constraint"
CONSTRAINT_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_kafka_constraint.sql"
rm -rf "$CONSTRAINT_PATH"
$CLICKHOUSE_LOCAL --path "$CONSTRAINT_PATH" --multiquery --query "
    CREATE DATABASE ${CONSTRAINT_DB};
    CREATE TABLE ${CONSTRAINT_DB}.plain_kafka (x Int64) ENGINE = Kafka('127.0.0.1:9092', 'topic', 'group', 'JSONEachRow');
"
$CLICKHOUSE_LOCAL --path "$CONSTRAINT_PATH" --dump-schema="$CONSTRAINT_DB" > "$CONSTRAINT_DUMP_FILE" 2>"$ERR_FILE"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB};
    DROP USER IF EXISTS ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE IF EXISTS ${CONSTRAINT_PROFILE};
    CREATE SETTINGS PROFILE ${CONSTRAINT_PROFILE} SETTINGS allow_kafka_offsets_storage_in_keeper = 0 CONST;
    CREATE USER ${CONSTRAINT_USER} SETTINGS PROFILE '${CONSTRAINT_PROFILE}';
    GRANT CREATE DATABASE, CREATE TABLE ON *.* TO ${CONSTRAINT_USER};
    GRANT TABLE ENGINE ON * TO ${CONSTRAINT_USER};
    GRANT KAFKA ON *.* TO ${CONSTRAINT_USER};
"
$CLICKHOUSE_CLIENT --user "$CONSTRAINT_USER" --multiquery --queries-file "$CONSTRAINT_DUMP_FILE" > /dev/null 2>"$ERR_FILE"
rc=$?
[[ $rc -eq 0 ]] && echo 'OK: constrained replay succeeded' || echo "FAIL: constrained replay rejected: $(cat "$ERR_FILE")"
echo "constrained replay table present: $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.tables WHERE database = '${CONSTRAINT_DB}' AND name = 'plain_kafka'")"
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CONSTRAINT_DB} SYNC;
    DROP USER ${CONSTRAINT_USER};
    DROP SETTINGS PROFILE ${CONSTRAINT_PROFILE};
"
rm -rf "$CONSTRAINT_PATH" "$CONSTRAINT_DUMP_FILE"

echo '--- a YTsaurus table keeps its engine gate ---'
YT_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_yt"
YT_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_yt.sql"
rm -rf "$YT_PATH"
$CLICKHOUSE_LOCAL --path "$YT_PATH" --multiquery --query "
CREATE DATABASE ${DB};
SET allow_experimental_ytsaurus_table_engine = 1;
CREATE TABLE ${DB}.yt (x Int64) ENGINE = YTsaurus('http://127.0.0.1:1', '//tmp/t', 'token');
"
$CLICKHOUSE_LOCAL --path "$YT_PATH" --dump-schema="${DB}" > "$YT_DUMP_FILE" 2>"$ERR_FILE"
echo "YTsaurus table, engine gate emitted: $(grep -c 'SET allow_experimental_ytsaurus_table_engine = 1;' "$YT_DUMP_FILE")"
rm -rf "$YT_PATH" "$YT_DUMP_FILE" "$ERR_FILE"

echo '--- an unrelated DataLakeCatalog database that cannot be listed does not fail the dump ---'
# `exact_header` is forbidden by tests/config/config.d/forbidden_headers.xml, so listing this catalog's tables fails.
CATALOG_DB="${DB}_unlistable_catalog"
READER_DB="${DB}_catalog_reader"
CATALOG_SRC="zzz_catalog_src_${CLICKHOUSE_TEST_UNIQUE_NAME}"
CATALOG_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_catalog.sql"
$CLICKHOUSE_CLIENT --query "
ATTACH DATABASE ${CATALOG_DB} ENGINE = DataLakeCatalog('http://localhost:18181/v1')
SETTINGS catalog_type = 'rest', auth_header = 'exact_header: some_value', warehouse = 'demo'
"
$CLICKHOUSE_CLIENT --multiquery --query "
CREATE DATABASE ${READER_DB};
USE ${READER_DB};
CREATE TABLE ${READER_DB}.${CATALOG_SRC} (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${READER_DB}.aaa_reader AS SELECT * FROM merge('', '^${CATALOG_SRC}\$');
"
if $CLICKHOUSE_CLIENT --dump-schema="${READER_DB}" > "$CATALOG_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "reader dumped: $(grep -c "CREATE VIEW ${READER_DB}\.aaa_reader " "$CATALOG_DUMP_FILE")"
    # The database-less merge() could also match in the catalog, which the dump cannot rule out.
    echo "unlisted catalog named for the database-less reader: $(grep -F "${READER_DB}.aaa_reader references merge('', '^${CATALOG_SRC}\$') without a database" "$ERR_FILE" | grep -cF "${CATALOG_DB}")"
else
    echo "FAIL: dump failed over an unrelated catalog: $(cat "$ERR_FILE")"
fi
if $CLICKHOUSE_CLIENT --dump-schema="${CATALOG_DB}" > /dev/null 2>"$ERR_FILE"; then
    echo 'FAIL: dump of the unlistable catalog itself succeeded'
else
    echo "unlistable catalog in the dump set still refused: $(grep -c 'is forbidden' "$ERR_FILE")"
fi
$CLICKHOUSE_CLIENT --multiquery --query "
    DROP DATABASE IF EXISTS ${CATALOG_DB};
    DROP DATABASE IF EXISTS ${READER_DB} SYNC;
"
rm -f "$CATALOG_DUMP_FILE" "$ERR_FILE"

echo '--- an unreachable omitted Remote database fails only the checks that need its tables ---'
# Nothing listens on port 1, so listing this database's tables fails at once.
REMOTE_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dead_remote"
REMOTE_DUMP_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_dead_remote.sql"
rm -rf "$REMOTE_PATH"
$CLICKHOUSE_LOCAL --path "$REMOTE_PATH" --multiquery --query "
CREATE DATABASE ${DB};
CREATE TABLE ${DB}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE VIEW ${DB}.qualified_reader AS SELECT * FROM ${DB}.src;
CREATE DATABASE dead_remote ENGINE = Remote('127.0.0.1:1', 'default');
"
if $CLICKHOUSE_LOCAL --path "$REMOTE_PATH" --dump-schema="${DB}" > "$REMOTE_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "qualified reader dumped: $(grep -c "CREATE VIEW ${DB}\.qualified_reader " "$REMOTE_DUMP_FILE")"
    echo "stderr lines: $(wc -l < "$ERR_FILE" | tr -d ' ')"
else
    echo "FAIL: dump failed over an unrelated Remote database: $(cat "$ERR_FILE")"
fi
$CLICKHOUSE_LOCAL --path "$REMOTE_PATH" --multiquery --query "
USE ${DB};
CREATE VIEW ${DB}.merge_reader AS SELECT * FROM merge('', '^src\$');
"
if $CLICKHOUSE_LOCAL --path "$REMOTE_PATH" --dump-schema="${DB}" > "$REMOTE_DUMP_FILE" 2>"$ERR_FILE"; then
    echo "database-less reader dumped: $(grep -c "CREATE VIEW ${DB}\.merge_reader " "$REMOTE_DUMP_FILE")"
    echo "unlisted Remote database named for the database-less reader: $(grep -F "${DB}.merge_reader references merge('', '^src\$') without a database" "$ERR_FILE" | grep -cF 'dead_remote')"
else
    echo "FAIL: dump with a database-less reader failed over an unrelated Remote database: $(cat "$ERR_FILE")"
fi
rm -rf "$REMOTE_PATH" "$REMOTE_DUMP_FILE" "$ERR_FILE"
