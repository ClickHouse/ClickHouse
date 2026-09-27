#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has neither `Kafka` nor `RabbitMQ`.
#
# The `source` of settings an engine assigns outside its `SETTINGS` clause and named collection.
#
# `Kafka`'s positional engine arguments are part of the table's definition as surely as its `SETTINGS` clause, and
# are reported as `definition`, not as a value the engine chose.
#
# `RabbitMQ` names a queue base after the table when none is given: a value nothing but the engine set, so `other`,
# as `Kafka`'s generated client id is - not `default`, which would claim the compiled-in empty string. And a table
# given `rabbitmq_address` takes the vhost from the address, so `rabbitmq_vhost` is reported as the definition
# states it rather than as a value the table uses.
#
# `clickhouse-local` with `message_queue_disable_insertion`, a server setting: a `RabbitMQ` table is otherwise created
# only once its broker answers, and one that never does costs seconds of retries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

RABBITMQ_SETTINGS="rabbitmq_exchange_name = 'x', rabbitmq_format = 'CSV'"

$CLICKHOUSE_LOCAL -q "
SELECT '-- Kafka positional arguments';
CREATE TABLE k (a UInt64) ENGINE = Kafka('localhost:9092', 'topic', 'group', 'CSV') SETTINGS kafka_max_block_size = 17;
SELECT name, value, source FROM system.table_settings
WHERE table = 'k' AND name IN ('kafka_broker_list', 'kafka_topic_list', 'kafka_group_name', 'kafka_format', 'kafka_max_block_size')
ORDER BY name;

SELECT '-- RabbitMQ queue base and vhost';
CREATE TABLE generated (a UInt64) ENGINE = RabbitMQ
    SETTINGS rabbitmq_host_port = '127.0.0.1:1', rabbitmq_username = 'u', rabbitmq_password = 'p', ${RABBITMQ_SETTINGS};
CREATE TABLE stated (a UInt64) ENGINE = RabbitMQ
    SETTINGS rabbitmq_host_port = '127.0.0.1:1', rabbitmq_username = 'u', rabbitmq_password = 'p', ${RABBITMQ_SETTINGS},
    rabbitmq_queue_base = 'qb';
CREATE TABLE address (a UInt64) ENGINE = RabbitMQ
    SETTINGS rabbitmq_address = 'amqp://127.0.0.1:1/from_address', ${RABBITMQ_SETTINGS}, rabbitmq_vhost = 'stated';
SELECT table, name, value, source FROM system.table_settings
WHERE table IN ('generated', 'stated', 'address') AND name IN ('rabbitmq_queue_base', 'rabbitmq_vhost')
ORDER BY table, name;" -- --message_queue_disable_insertion=1
