#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# `Kafka` and `S3Queue` are not built in fast test. A named collection exists only on the server that created it, so a
# `Replicated` database cannot create a table on it on its other replicas. A shell test for a collection name unique to
# the run: `CREATE NAMED COLLECTION` takes no query parameter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="nc_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -n -q "
-- A secret is shown as [HIDDEN], as in SHOW CREATE TABLE; an unset one is not reported as hidden.
CREATE TABLE kafka_secret (a Int) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'JSONEachRow',
    kafka_sasl_password = 'secret_password_05296';
CREATE TABLE kafka_no_secret (a Int) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'JSONEachRow';
SELECT table, name, value FROM system.table_settings
WHERE database = currentDatabase() AND name IN ('kafka_broker_list', 'kafka_sasl_password')
ORDER BY table, name;

-- S3Queue accepts its settings with an s3queue_ prefix, which the engine strips.
CREATE TABLE queue (a String) ENGINE = S3Queue('http://localhost:11111/test/05296/*.csv', NOSIGN, 'CSV')
SETTINGS s3queue_mode = 'unordered';
SELECT name, value, changed FROM system.table_settings WHERE database = currentDatabase() AND table = 'queue' AND name = 'mode';

-- A value a named collection supplies is hidden unless the server lets the reader see the collection's secrets, which
-- it does not here; a literal override in the engine arguments and the SETTINGS clause are shown. A format setting
-- the collection supplies has a row, hidden too, although the engine's rows leave the format settings out.
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g',
    kafka_format = 'JSONEachRow', kafka_max_block_size = '777', format_csv_delimiter = ';';
CREATE TABLE kafka_collection (a Int) ENGINE = Kafka(${collection}, kafka_group_name = 'g_override')
SETTINGS kafka_poll_timeout_ms = 1234;
SELECT name, value, changed FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kafka_collection'
    AND name IN ('format_csv_delimiter', 'kafka_broker_list', 'kafka_group_name', 'kafka_max_block_size', 'kafka_poll_timeout_ms')
ORDER BY name;

DROP TABLE kafka_collection;
DROP NAMED COLLECTION ${collection};
DROP TABLE queue;
DROP TABLE kafka_no_secret;
DROP TABLE kafka_secret;
"

# A collection can be changed after the table was created, to a value the setting cannot take. The error names the
# collection and the table, not the value, which the reader may not see.
$CLICKHOUSE_CLIENT -n -q "
DROP NAMED COLLECTION IF EXISTS ${collection}_changed;
CREATE NAMED COLLECTION ${collection}_changed AS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't',
    kafka_group_name = 'g', kafka_format = 'JSONEachRow', kafka_max_block_size = '100';
CREATE TABLE kafka_changed (a Int) ENGINE = Kafka(${collection}_changed);
ALTER NAMED COLLECTION ${collection}_changed SET kafka_max_block_size = 'not_a_number_05296';
"
error=$($CLICKHOUSE_CLIENT -q "SELECT * FROM system.table_settings WHERE database = currentDatabase() AND table = 'kafka_changed'" 2>&1)
echo "$error" | grep -o -m1 "holds a value that cannot be applied to the engine's settings"
echo "$error" | grep -c 'not_a_number_05296'
$CLICKHOUSE_CLIENT -n -q "
DROP TABLE kafka_changed;
DROP NAMED COLLECTION ${collection}_changed;
"
