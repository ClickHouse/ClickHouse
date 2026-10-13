#!/usr/bin/env bash
# Tags: no-fasttest
# Tag justification: the `Kafka`, `NATS` and `RabbitMQ` engines are not available in a build without libraries.

# The message-queue engines take the format settings of their definition from a named collection as
# well as from the `SETTINGS` clause, so Parquet `field_id` settings coming from a named collection are
# validated at `CREATE` time too. The collection names include the database name, so concurrent runs
# of the test do not collide on these server-global objects.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC_KAFKA="nc_kafka_field_ids_${CLICKHOUSE_DATABASE}"
NC_NATS="nc_nats_field_ids_${CLICKHOUSE_DATABASE}"
NC_RABBITMQ="nc_rabbitmq_field_ids_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --send_logs_level=fatal -n -q "
DROP NAMED COLLECTION IF EXISTS ${NC_KAFKA};
DROP NAMED COLLECTION IF EXISTS ${NC_NATS};
DROP NAMED COLLECTION IF EXISTS ${NC_RABBITMQ};

CREATE NAMED COLLECTION ${NC_KAFKA} AS
    kafka_broker_list = 'localhost:10000', kafka_topic_list = 'foo', kafka_group_name = 'foo', kafka_format = 'Parquet',
    output_format_parquet_column_field_ids = '{''missing'': ''1'', ''x'': ''2''}';
CREATE NAMED COLLECTION ${NC_NATS} AS
    nats_url = '127.0.0.1:1', nats_subjects = 'subject', nats_format = 'Parquet',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1,
    output_format_parquet_column_field_ids = '{''missing'': ''1'', ''x'': ''2''}';
CREATE NAMED COLLECTION ${NC_RABBITMQ} AS
    rabbitmq_host_port = '127.0.0.1:1', rabbitmq_exchange_name = 'exchange', rabbitmq_format = 'Parquet',
    output_format_parquet_auto_assign_field_ids = 1;
"

# Each `CREATE` fails before the engine connects anywhere.
for query in \
    "CREATE TABLE t_kafka_nc_field_ids (x Int64) ENGINE = Kafka(${NC_KAFKA})" \
    "CREATE TABLE t_nats_nc_field_ids (x Int64) ENGINE = NATS(${NC_NATS})" \
    "CREATE TABLE t_rabbitmq_nc_field_ids (x Int64) ENGINE = RabbitMQ(${NC_RABBITMQ})"
do
    $CLICKHOUSE_CLIENT --send_logs_level=fatal -q "$query" 2>&1 | grep -o -m1 -E 'references unknown column|cannot be set in a RabbitMQ table definition' || echo "not rejected: $query"
done

$CLICKHOUSE_CLIENT -n -q "
DROP NAMED COLLECTION ${NC_KAFKA};
DROP NAMED COLLECTION ${NC_NATS};
DROP NAMED COLLECTION ${NC_RABBITMQ};
"
