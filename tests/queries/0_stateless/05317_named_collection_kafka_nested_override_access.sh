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
nc_topic="${nc}_topic"
nc_topic_locked="${nc}_topic_locked"
nc_alias="${nc}_alias"
nc_dot_locked="${nc}_dot_locked"
nc_broker="${nc}_broker"
nc_broker_locked="${nc}_broker_locked"
nc_prefix_locked="${nc}_prefix_locked"
user="${nc}_user"
table="${nc}_table"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS ${table};
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS ${nc};
        DROP NAMED COLLECTION IF EXISTS ${nc_locked};
        DROP NAMED COLLECTION IF EXISTS ${nc_topic};
        DROP NAMED COLLECTION IF EXISTS ${nc_topic_locked};
        DROP NAMED COLLECTION IF EXISTS ${nc_alias};
        DROP NAMED COLLECTION IF EXISTS ${nc_dot_locked};
        DROP NAMED COLLECTION IF EXISTS ${nc_broker};
        DROP NAMED COLLECTION IF EXISTS ${nc_broker_locked};
        DROP NAMED COLLECTION IF EXISTS ${nc_prefix_locked};
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
    CREATE NAMED COLLECTION ${nc_topic} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.kafka_topic.name\` = 'topic',
        \`kafka.kafka_topic.security_protocol\` = 'SASL_SSL',
        \`kafka.consumer.kafka_topic.name\` = 'topic',
        \`kafka.consumer.kafka_topic.sasl_username\` = 'user',
        \`kafka.kafka_other.compression_codec\` = 'zstd';
    CREATE NAMED COLLECTION ${nc_topic_locked} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.kafka_topic.name\` = 'topic',
        \`kafka.kafka_topic.security_protocol\` = 'SASL_SSL' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${nc_alias} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.consumer.kafka_topic.name\` = 'topic',
        \`kafka.consumer.kafka_topic.sasl_mechanisms\` = 'SCRAM-SHA-256',
        \`kafka.producer.compression_type\` = 'zstd';
    CREATE NAMED COLLECTION ${nc_dot_locked} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'sports.football', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.kafka_sports.football.security_protocol\` = 'SASL_SSL' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${nc_broker} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        kafka_sasl_username = 'user', kafka_sasl_password = 'password';
    CREATE NAMED COLLECTION ${nc_broker_locked} AS
        kafka_broker_list = '127.0.0.1:2' NOT OVERRIDABLE, kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        kafka_sasl_username = 'user', kafka_sasl_password = 'password';
    CREATE NAMED COLLECTION ${nc_prefix_locked} AS
        kafka_broker_list = '127.0.0.1:2', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow',
        \`kafka.producer.topic.compression_level\` = '9' NOT OVERRIDABLE;
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

# The same for the keys of per-topic sections, new and legacy syntax.
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_topic}) SETTINGS kafka_security_protocol = 'PLAINTEXT'"
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_topic}) SETTINGS kafka_sasl_username = 'x'"
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_topic}) SETTINGS kafka_compression_codec = 'gzip'"

# The same for the librdkafka aliases of the properties.
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_alias}) SETTINGS kafka_sasl_mechanism = 'PLAIN'"
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_alias}) SETTINGS kafka_compression_codec = 'gzip'"

# A nested key cannot replace a stored flat setting either.
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker}, \`kafka.metadata_broker_list\` = '127.0.0.1:3')"
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker}, \`kafka.consumer.bootstrap_servers\` = '127.0.0.1:3')"
# Also when an index ends a key name without a dot after it.
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker}, \`kafka[0]metadata_broker_list\` = '127.0.0.1:3')"
expect_error ACCESS_DENIED "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker}, \`kafka.consumer[0]bootstrap_servers\` = '127.0.0.1:3')"

# A zero byte in a key is rejected, librdkafka would see only the part before it.
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker}, concat('kafka.metadata_broker_list', char(0), 'ignored') = '127.0.0.1:3')"

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
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc_topic_locked} TO $user"
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_topic_locked}) SETTINGS kafka_security_protocol = 'PLAINTEXT'"
# The legacy section of a topic with a dot in its name.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc_dot_locked} TO $user"
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_dot_locked}) SETTINGS kafka_security_protocol = 'PLAINTEXT'"
# A locked flat setting and a nested key.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc_broker_locked} TO $user"
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker_locked}, \`kafka.metadata_broker_list\` = '127.0.0.1:3')"
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker_locked}, \`kafka[0]metadata_broker_list\` = '127.0.0.1:3')"
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_broker_locked}, concat('kafka.metadata_broker_list', char(0), 'ignored') = '127.0.0.1:3')"
# The `topic.` prefix of a topic property.
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc_prefix_locked} TO $user"
expect_error BAD_ARGUMENTS "CREATE TABLE ${table} (value String) ENGINE = Kafka(${nc_prefix_locked}) SETTINGS kafka_compression_level = 1"
