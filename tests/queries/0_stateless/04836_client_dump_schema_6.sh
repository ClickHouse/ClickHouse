#!/usr/bin/env bash
# Tags: no-fasttest, no-darwin
# Tag no-fasttest: Kafka is not in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

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
