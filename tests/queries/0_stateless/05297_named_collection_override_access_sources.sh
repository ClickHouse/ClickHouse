#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: on a replicated database the DDL runs with no user, so the access check asserted here is skipped and the deny path silently allows, https://github.com/ClickHouse/ClickHouse/issues/111561
# Requires the external database and message queue engines omitted from the fast build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="${nc}_user"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        DROP TABLE IF EXISTS override_access_source;
        DROP TABLE IF EXISTS override_access_queue;
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS ${nc}_db;
        DROP NAMED COLLECTION IF EXISTS ${nc}_mysql_settings;
        DROP NAMED COLLECTION IF EXISTS ${nc}_pg_settings;
        DROP NAMED COLLECTION IF EXISTS ${nc}_s3;
        DROP NAMED COLLECTION IF EXISTS ${nc}_azure;
        DROP NAMED COLLECTION IF EXISTS ${nc}_nats;
        DROP NAMED COLLECTION IF EXISTS ${nc}_kafka;
        DROP NAMED COLLECTION IF EXISTS ${nc}_rabbitmq;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE USER $user;
    GRANT SOURCES, CREATE TEMPORARY TABLE ON *.* TO $user;
    GRANT CREATE TABLE, SELECT, INSERT, BACKUP ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT NAMED COLLECTION ON * TO $user;
    CREATE TABLE override_access_source (value UInt64) ENGINE = Memory;
    CREATE NAMED COLLECTION ${nc}_db AS
        host = '127.0.0.1', port = 1, user = 'user', password = 'secret', database = 'database', table = 'table';
    CREATE NAMED COLLECTION ${nc}_mysql_settings AS
        host = '127.0.0.1', port = 1, user = 'user', password = 'secret', database = 'database', table = 'table',
        connection_pool_size = 2;
    CREATE NAMED COLLECTION ${nc}_pg_settings AS
        host = '127.0.0.1', port = 1, user = 'user', password = 'secret', database = 'database', table = 'table',
        postgresql_connection_pool_size = 2;
    CREATE NAMED COLLECTION ${nc}_s3 AS
        url = 'http://127.0.0.1:1/bucket/data', access_key_id = 'key', secret_access_key = 'secret';
    CREATE NAMED COLLECTION ${nc}_azure AS
        storage_account_url = 'http://127.0.0.1:1/account', account_name = 'account', account_key = 'secret',
        container = 'container', blob_path = 'data';
    CREATE NAMED COLLECTION ${nc}_nats AS
        nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'JSONEachRow',
        nats_credentials = 'secret';
    CREATE NAMED COLLECTION ${nc}_kafka AS
        kafka_broker_list = '127.0.0.1:1', kafka_topic_list = 'topic', kafka_group_name = 'group', kafka_format = 'JSONEachRow';
    CREATE NAMED COLLECTION ${nc}_rabbitmq AS
        rabbitmq_host_port = '127.0.0.1:1', rabbitmq_exchange_name = 'exchange', rabbitmq_format = 'JSONEachRow';
"

while IFS= read -r query; do
    [ -n "$query" ] || continue
    if error=$(${CLICKHOUSE_CLIENT} --user "$user" --query "$query" 2>&1); then
        echo "Expected the secrets privilege to be required: $query"
        exit 1
    fi
    if [[ "$error" != *"SHOW NAMED COLLECTIONS SECRETS ON"* ]]; then
        echo "$error"
        exit 1
    fi
done <<SQL
    SELECT * FROM mysql(${nc}_db, addresses_expr = '127.0.0.1:2');
    SELECT * FROM postgresql(${nc}_db, addresses_expr = '127.0.0.1:2');
    CREATE TABLE override_access_queue (value UInt64) ENGINE = MySQL(${nc}_mysql_settings) SETTINGS connection_pool_size = 0;
    CREATE TABLE override_access_queue (value UInt64) ENGINE = PostgreSQL(${nc}_pg_settings) SETTINGS postgresql_connection_pool_size = 0;
    SELECT * FROM s3(${nc}_s3, url = 'http://127.0.0.1:2/bucket/data', structure = 'value UInt64');
    SELECT * FROM azureBlobStorage(${nc}_azure, connection_string = 'http://127.0.0.1:2/account', structure = 'value UInt64');
    BACKUP TABLE override_access_source TO S3(${nc}_s3, url = 'http://127.0.0.1:2/bucket/backup');
    BACKUP TABLE override_access_source TO S3(${nc}_s3, \`url[0]\` = 'http://127.0.0.1:2/bucket/backup');

    CREATE TABLE override_access_queue (value UInt64) ENGINE = NATS(${nc}_nats, nats_server_list = '127.0.0.1:2');
    CREATE TABLE override_access_queue (value UInt64) ENGINE = NATS(${nc}_nats, \`nats_url[0]\` = '127.0.0.1:2');
    CREATE TABLE override_access_queue (value UInt64) ENGINE = NATS(${nc}_nats, \`nats_server_list[0]\` = '127.0.0.1:2');
    CREATE TABLE override_access_queue (value UInt64) ENGINE = NATS(${nc}_nats, \`nats_server_list.value\` = '127.0.0.1:2');
    CREATE TABLE override_access_queue (value UInt64) ENGINE = NATS(${nc}_nats) SETTINGS nats_url = '127.0.0.1:2';
    CREATE TABLE override_access_queue (value UInt64) ENGINE = NATS(${nc}_nats) SETTINGS nats_server_list = '127.0.0.1:2';
    CREATE TABLE override_access_queue (value UInt64) ENGINE = NATS(${nc}_nats) SETTINGS nats_credentials = 'replacement';
    CREATE TABLE override_access_queue (value UInt64) ENGINE = Kafka(${nc}_kafka) SETTINGS kafka_broker_list = '127.0.0.1:2';
    CREATE TABLE override_access_queue (value UInt64) ENGINE = RabbitMQ(${nc}_rabbitmq) SETTINGS rabbitmq_host_port = '127.0.0.1:2';
    CREATE TABLE override_access_queue (value UInt64) ENGINE = RabbitMQ(${nc}_rabbitmq) SETTINGS rabbitmq_address = 'amqp://127.0.0.1:2';
SQL

# A grant cannot bypass the explicit lock through an alias.
${CLICKHOUSE_CLIENT} --multiquery --query "
    GRANT SHOW NAMED COLLECTIONS SECRETS ON ${nc}_db TO $user;
    ALTER NAMED COLLECTION ${nc}_db SET host = '127.0.0.1' NOT OVERRIDABLE;
"
if error=$(${CLICKHOUSE_CLIENT} --user "$user" --query "SELECT * FROM mysql(${nc}_db, addresses_expr = '127.0.0.1:2')" 2>&1); then
    echo 'Expected the locked host to reject its alias'
    exit 1
fi
if [[ "$error" != *"Override not allowed for 'host'"* ]]; then
    echo "$error"
    exit 1
fi
