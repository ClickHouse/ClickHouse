#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no NATS, RabbitMQ, Kafka, MySQL, S3 or DataLakeCatalog, whose arguments it then hides whole

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse-format` hides the secrets that each table function, engine, function, backup locator and
# dictionary source declares, in their arguments and in the `SETTINGS` clause.
while read -r query; do
    $CLICKHOUSE_FORMAT --oneline --query "$query"
done <<'EOF'
CREATE DATABASE test_unity ENGINE = DataLakeCatalog('http://localhost:8181') SETTINGS aws_access_key_id = 'AKIA_PLAIN', aws_secret_access_key = 'plain_secret', storage_aws_access_key_id = 'AKIA_STORAGE', storage_aws_secret_access_key = 'storage_secret'
CREATE TABLE test_nats (key UInt64) ENGINE = NATS(nats1, nats_password = 'plain_password', nats_token = 'plain_token', nats_credential_file = '/plain/credential/file', nats_credentials = 'plain_user_jwt_and_seed')
CREATE TABLE test_nats_settings (key UInt64) ENGINE = NATS SETTINGS nats_credentials = 'plain_settings_user_jwt_and_seed'
CREATE TABLE test_nats (key UInt64) ENGINE = NATS(nats1, nats_url = 'nats://plain_user:plain_password@example.com:4222')
CREATE TABLE test_nats (key UInt64) ENGINE = NATS(nats1, nats_server_list = 'nats://plain_user:plain_password@example.com:4222,nats://plain_user2:plain_password2@example.org:4222')
CREATE TABLE test_nats_settings (key UInt64) ENGINE = NATS SETTINGS nats_server_list = 'nats://plain_user:plain_settings_password@example.com:4222'
CREATE TABLE test_nats (key UInt64) ENGINE = NATS(nats1, concat('nats_', 'credentials') = 'plain_user_jwt_and_seed', nats_url = concat('nats://plain_user:plain_password@', 'example.com:4222'))
CREATE TABLE test_nats (key UInt64) ENGINE = NATS(nats1, '/plain/credential/file', 'nats://plain_user:plain_password@example.com:4222')
CREATE TABLE test_jdbc (key UInt64) ENGINE = JDBC(jdbc1, 'DSN=mydb;Uid=user;Pwd=plain_password', 'mydb', 'mytable')
CREATE TABLE test_jdbc (key UInt64) ENGINE = JDBC(jdbc1, datasource = 'DSN=mydb;Uid=user;Pwd=plain_named_password', external_database = 'mydb', external_table = 'mytable')
CREATE TABLE test_jdbc (key UInt64) ENGINE = JDBC(external_database = 'mydb', datasource = 'DSN=mydb;Uid=user;Pwd=plain_password')
CREATE TABLE test_rabbitmq (key UInt64) ENGINE = RabbitMQ(rabbitmq1, rabbitmq_password = 'plain_password', rabbitmq_address = 'amqp://plain_user:plain_address_password@example.com:5672/vhost')
CREATE TABLE test_rabbitmq (key UInt64) ENGINE = RabbitMQ(rabbitmq1, rabbitmq_address = 'amqp://example.com:5672/vhost')
CREATE TABLE test_rabbitmq_settings (key UInt64) ENGINE = RabbitMQ SETTINGS rabbitmq_password = 'plain_settings_password'
CREATE TABLE test_kafka (key UInt64) ENGINE = Kafka(kafka1, kafka_sasl_password = 'plain_password')
CREATE TABLE test_kafka (key UInt64) ENGINE = Kafka('broker:9092', 'topic', 'group', 'JSONEachRow')
CREATE TABLE test_kafka (key UInt64) ENGINE = Kafka(kafka_sasl_password = 'plain_first_password', 'clickhouse')
CREATE TABLE test_kafka_settings (key UInt64) ENGINE = Kafka SETTINGS kafka_sasl_password = 'plain_settings_password'
ALTER TABLE test_kafka MODIFY SETTING kafka_sasl_password = 'plain_password'
CREATE TABLE test_mysql (key UInt64) ENGINE = MySQL('host:3306', 'db', 'table', 'user', 'plain_password')
CREATE TABLE test_mysql (key UInt64) ENGINE = MySQL(mysql_creds, 'plain_password')
SELECT * FROM mysql(localhost, 'db', 'table', 'user', 'plain_password')
CREATE TABLE test_url (key UInt64) ENGINE = URL(url_creds, 'https://user:plain_password@example.com/data')
CREATE TABLE test_unknown (key UInt64) ENGINE = NoSuchEngine('plain_password')
CREATE DATABASE test_mysql ENGINE = MySQL('host:3306', 'db', 'user', 'plain_password')
SELECT * FROM mysql('host:3306', 'db', 'table', 'user', 'plain_password')
SELECT * FROM s3('http://bucket.s3.amazonaws.com/file.csv', 'access_key_id', 'plain_secret_key', 'CSV')
SELECT * FROM arrowflight('host:8815', 'dataset', 'user', 'plain_password')
SELECT * FROM url('http://example.com/data.csv', 'CSV', extra_credentials(role_arn = 'arn', external_id = 'plain_external_id'))
CREATE TABLE test_iceberg (key UInt64) ENGINE = Iceberg('http://bucket.s3.amazonaws.com/table/') SETTINGS auth_header = 'plain_auth_header'
SELECT * FROM iceberg('http://bucket.s3.amazonaws.com/table/', SETTINGS auth_header = 'plain_auth_header')
CREATE TABLE test_iceberg_azure (key UInt64) ENGINE = IcebergAzure('https://account.blob.core.windows.net', 'container', 'path', 'account', 'plain_account_key')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net/?sig=plain_sas_signature', 'container', 'blob')
SELECT * FROM azureBlobStorage(azure_conf, storage_account_url = 'https://account.blob.core.windows.net/?sig=plain_sas_signature', container = 'container', blob_path = 'blob')
SELECT * FROM azureBlobStorage('https://account.blob.core.windows.net', 'container', 'blob', extra_credentials(client_id = 'id', tenant_id = 'tid'), 'account', 'plain_account_key')
SELECT * FROM azureBlobStorage(azure_conf, 'plain_secret')
SELECT encrypt('aes-256-ofb', 'plain_text', 'plain_key'), hmac('sha256', 'message', 'plain_key')
BACKUP TABLE t TO S3('http://bucket.s3.amazonaws.com/backup', 'access_key_id', 'plain_secret_key')
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(CLICKHOUSE(HOST 'localhost' USER 'user' PASSWORD 'plain_password' TABLE 't')) LIFETIME(0) LAYOUT(FLAT())
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(MONGODB(URI 'mongodb://user:plain_password@localhost:27017/db' COLLECTION 'c')) LIFETIME(0) LAYOUT(FLAT())
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(ODBC(CONNECTION_STRING 'DSN=mydb;UID=user;PWD=plain_password' TABLE 't')) LIFETIME(0) LAYOUT(FLAT())
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(HTTP(URL 'http://user:plain_password@localhost/data' FORMAT 'TSV')) LIFETIME(0) LAYOUT(FLAT())
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(HTTP(URL 'https://localhost/data?format=tsv&access_token=plain_token' FORMAT 'TSV')) LIFETIME(0) LAYOUT(FLAT())
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(HTTP(URL format('http://user:{}@localhost/data', 'plain_password') FORMAT 'TSV')) LIFETIME(0) LAYOUT(FLAT())
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(MONGODB(URI format('mongodb://user:{}@localhost/db', 'plain_password') COLLECTION 'c')) LIFETIME(0) LAYOUT(FLAT())
CREATE DICTIONARY test_dict (key UInt64, value String) PRIMARY KEY key SOURCE(YTSAURUS(HTTP_PROXY_URLS 'http://localhost:8000' CYPRESS_PATH '//tmp/t' OAUTH_TOKEN 'plain_token')) LIFETIME(0) LAYOUT(FLAT())
EOF

# A dictionary source key that is only partly secret still makes the logged query masked.
$CLICKHOUSE_CLIENT -q "CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict_mongodb_uri (key UInt64, value String) PRIMARY KEY key SOURCE(MONGODB(URI 'mongodb://user:plain_mongodb_password@localhost:27017/db' COLLECTION 'c')) LIFETIME(0) LAYOUT(FLAT())"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "SELECT countIf(query LIKE '%plain_mongodb_password%'), countIf(query LIKE '%[HIDDEN]%') FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'CREATE DICTIONARY%dict_mongodb_uri%' AND type = 'QueryFinish' AND is_initial_query"
