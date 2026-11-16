#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: on a replicated database the DDL runs with no user, so the access check asserted here is skipped and the deny path silently allows, https://github.com/ClickHouse/ClickHouse/issues/111561
# Tag no-fasttest: requires the Kafka engine omitted from the fast build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}"
nc_locked="${nc}_locked"
user="${nc}_user"
table="${nc}_table"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS ${table};
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS ${nc};
        DROP NAMED COLLECTION IF EXISTS ${nc_locked};
    "
}
trap cleanup EXIT
cleanup

# Flattened `SETTINGS` names overwrite the librdkafka properties that are set from nested collection keys.
${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE USER $user;
    GRANT SOURCES ON *.* TO $user;
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT NAMED COLLECTION ON * TO $user;
    CREATE NAMED COLLECTION ${nc} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.security_protocol\` = 'SASL_SSL',
        \`kafka.consumer.sasl_username\` = 'user',
        \`kafka.producer.compression_codec\` = 'zstd';
    CREATE NAMED COLLECTION ${nc_locked} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.security_protocol\` = 'SASL_SSL' NOT OVERRIDABLE;
"

function expect_error()
{
    local code="$1"
    local query="$2"
    if error=$(${CLICKHOUSE_CLIENT} --user "$user" --query "$query" 2>&1); then
        echo "Expected $code: $query"
        exit 1
    fi
    echo "$error" | { grep -o "$code" || true; } | head -1
}

# Without the privilege, a flattened setting cannot replace a stored nested key.
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc}) SETTINGS kafka_security_protocol = 'PLAINTEXT'"
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc}) SETTINGS kafka_sasl_username = 'x'"
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc}) SETTINGS kafka_compression_codec = 'gzip'"

# A flattened setting without a stored nested equivalent is an addition.
${CLICKHOUSE_CLIENT} --user "$user" --query "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc}) SETTINGS kafka_sasl_mechanism = 'PLAIN'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
echo 'addition OK'

# With the privilege, the override is allowed.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc} TO $user"
${CLICKHOUSE_CLIENT} --user "$user" --query "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc}) SETTINGS kafka_security_protocol = 'PLAINTEXT'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${table}"
echo 'granted OK'

# A grant cannot bypass the explicit lock of a nested key.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc_locked} TO $user"
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_locked}) SETTINGS kafka_security_protocol = 'PLAINTEXT'"
