-- Tags: no-fasttest
-- The `Kafka` and `NATS` engines apply the format settings of their definition to every `INSERT`,
-- so an invalid Parquet `field_id` map in the definition is rejected at `CREATE` time instead of
-- producing a table that fails every `INSERT`.

-- Suppress the errors of the background consumers that try to connect to non-existent brokers.
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_kafka_field_ids;

-- Unknown column.
CREATE TABLE t_kafka_field_ids (x Int64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'Parquet',
    output_format_parquet_column_field_ids = {'missing': '1', 'x': '2'}; -- { serverError BAD_ARGUMENTS }
-- The map must cover every column when auto-assign is off.
CREATE TABLE t_kafka_field_ids (x Int64, y String) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'Parquet',
    output_format_parquet_column_field_ids = {'x': '1'}; -- { serverError BAD_ARGUMENTS }
-- Ambiguous flattened paths are rejected with auto-assign, too.
CREATE TABLE t_kafka_field_ids (t Tuple(a Int64), `t.a` Int64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'Parquet',
    output_format_parquet_auto_assign_field_ids = 1; -- { serverError BAD_ARGUMENTS }

-- Columns mapped to the message metadata are not part of the Parquet payload, so the map must not name them.
CREATE TABLE t_kafka_field_ids (_key String, x Int64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'Parquet',
    kafka_map_virtual_columns_on_write = 1, output_format_parquet_column_field_ids = {'_key': '1', 'x': '2'}; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_kafka_field_ids (_key String, x Int64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'Parquet',
    kafka_map_virtual_columns_on_write = 1, output_format_parquet_column_field_ids = {'x': '2'};
SELECT 'kafka valid definition accepted';
DROP TABLE t_kafka_field_ids;

-- The settings only affect Parquet output.
CREATE TABLE t_kafka_field_ids (x Int64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'JSONEachRow',
    output_format_parquet_column_field_ids = {'missing': '1'};
SELECT 'kafka non-Parquet definition accepted';
DROP TABLE t_kafka_field_ids;

-- An ambient value is not part of the definition and is validated only when Parquet is written.
SET output_format_parquet_column_field_ids = {'missing': '1'};
CREATE TABLE t_kafka_field_ids (x Int64) ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'Parquet';
SELECT 'kafka ambient value not validated at CREATE';
DROP TABLE t_kafka_field_ids;
SET output_format_parquet_column_field_ids = {};

CREATE TABLE t_nats_field_ids (x Int64) ENGINE = NATS
SETTINGS nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'Parquet',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1,
    output_format_parquet_column_field_ids = {'missing': '1', 'x': '2'}; -- { serverError BAD_ARGUMENTS }

-- `RabbitMQ` does not apply the format settings of its definition on `INSERT`, so the settings would be
-- silently ignored there and are rejected instead.
CREATE TABLE t_rabbitmq_field_ids (x Int64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = '127.0.0.1:1', rabbitmq_exchange_name = 'exchange', rabbitmq_format = 'Parquet',
    output_format_parquet_column_field_ids = {'x': '1'}; -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_rabbitmq_field_ids (x Int64) ENGINE = RabbitMQ
SETTINGS rabbitmq_host_port = '127.0.0.1:1', rabbitmq_exchange_name = 'exchange', rabbitmq_format = 'Parquet',
    output_format_parquet_auto_assign_field_ids = 1; -- { serverError BAD_ARGUMENTS }
