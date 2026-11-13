import asyncio
import csv
import io
import json
import logging
import random
import subprocess
import time
import traceback
import uuid
from datetime import date, datetime
from decimal import Decimal

import pika
from kafka import KafkaAdminClient
from kafka.admin import ConfigResource, ConfigResourceType, NewPartitions, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
from nats.js import api
from pyspark.sql import Row

from tests.integration.helpers.cluster import docker_exec, nats_connect_ssl
import tests.integration.helpers.kafka.common as k

from .datagenerator import LakeDataGenerator, _to_json_safe
from .filetables import DATA_FILE_FORMATS


def _plain(value):
    """`value` from the data generator as a plain Python value: dates, decimals, bytes and rows flattened."""
    if isinstance(value, Row):
        return tuple(_plain(v) for v in value)
    if isinstance(value, (list, tuple)):
        return type(value)(_plain(v) for v in value)
    if isinstance(value, dict):
        return {_plain(k): _plain(v) for k, v in value.items()}
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (datetime, date, Decimal)):
        # `str` of a `datetime` separates with a space, as the basic parser of ClickHouse expects
        return str(value)
    return value


# Escape sequences of strings in the text formats of ClickHouse, so no raw separator ends a field or a row
_ESCAPES = str.maketrans({"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r", "\0": "\\0"})


def _text(value) -> str:
    """`value` in the text format of a CSV or TSV field, before the escaping of TSV."""
    if value is None:
        return "\\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (tuple, list, dict)):
        # Composite values take the text representation of ClickHouse, as SQL literals
        return _literal(value)
    return str(value)


def _literal(value) -> str:
    """`value` as a SQL literal, for the `Values` format and composite values."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, tuple):
        return "(" + ",".join(_literal(v) for v in value) + ")"
    if isinstance(value, list):
        return "[" + ",".join(_literal(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(f"{_literal(k)}:{_literal(v)}" for k, v in value.items()) + "}"
    return "'" + str(value).translate(_ESCAPES).replace("'", "\\'") + "'"


def _csv_fields(value) -> list:
    """The CSV fields of `value`: ClickHouse writes each element of a tuple as a field of its own."""
    if isinstance(value, tuple):
        return [field for v in value for field in _csv_fields(v)]
    return [_text(value)]


def _json_tuples(value):
    """`value` as ClickHouse reads it from JSON. The unnamed tuples are arrays: the type mapper
    names their fields `col1` to `colN`, and those of a named one as in ClickHouse."""
    if isinstance(value, Row):
        items = [_json_tuples(v) for v in value]
        fields = list(value.__fields__)
        if fields == [f"col{i + 1}" for i in range(len(fields))]:
            return items
        return dict(zip(fields, items))
    if isinstance(value, (list, tuple)):
        return [_json_tuples(v) for v in value]
    if isinstance(value, dict):
        return {k: _json_tuples(v) for k, v in value.items()}
    if isinstance(value, bytes):
        # Strings and enum elements, which `_to_json_safe` would write in hex
        return value.decode("utf-8", errors="replace")
    if isinstance(value, Decimal) and value == value.to_integral_value():
        # Wide integers come as decimals, which `_to_json_safe` would write as floats
        return int(value)
    return value


_JSON_ROW_FORMATS = ("JSONEachRow", "JSONLines", "NDJSON")
# The formats `_row` writes, ClickHouse writes the others
_RENDERED_FORMATS = _JSON_ROW_FORMATS + ("JSONCompactEachRow", "CSV", "TSV", "TabSeparated", "Values")


def _row(generator: LakeDataGenerator, columns: list, fmt: str) -> str:
    raw = generator.random_clickhouse_values([col["type"] for col in columns])
    names = [col["name"] for col in columns]
    if fmt == "JSONCompactEachRow":
        return json.dumps(_to_json_safe(_json_tuples(raw)))
    if fmt in _JSON_ROW_FORMATS:
        return json.dumps(_to_json_safe(dict(zip(names, _json_tuples(raw)))))
    values = [_plain(v) for v in raw]
    if fmt == "CSV":
        out = io.StringIO()
        csv.writer(out, lineterminator="").writerow(field for v in values for field in _csv_fields(v))
        return out.getvalue()
    if fmt in ("TSV", "TabSeparated"):
        return "\t".join(_text(v).translate(_ESCAPES) if isinstance(v, str) else _text(v) for v in values)
    return "(" + ",".join(_literal(v) for v in values) + ")"


class StreamingTable:

    def __init__(self, topic: str, group: str, fmt: str, columns: list):
        self.topic = topic
        self.group = group
        self.format = fmt
        self.columns = columns


class StreamingHandler:
    """Keeps the topic of each table, and produces to it or changes it on the broker."""

    def __init__(self, cluster, spark_handler):
        self.cluster = cluster
        # Back-reference for the data generator and the data files of the `File` tables
        self.spark = spark_handler
        self.tables: dict[str, StreamingTable] = {}
        self.logger = logging.getLogger(__name__)
        # The settings of the client binary that allow the suspicious and experimental types BuzzHouse creates
        names = subprocess.run(
            [
                cluster.client_bin_path,
                "local",
                "--query",
                "SELECT name FROM system.settings WHERE type = 'Bool' AND (name LIKE 'allow_suspicious%'"
                " OR (name LIKE '%type%' AND (name LIKE 'allow_experimental%' OR name LIKE 'enable_%')))",
            ],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=60,
            check=True,
        ).stdout.split()
        self.allow_all_types = [f"--{name}=1" for name in names]

    def next_messages(self, table: StreamingTable) -> list:
        """Messages for `table` in its format: mostly valid rows, some broken on purpose."""
        generator = self.spark.data_generator
        columns = list(table.columns) or [{"name": "c0", "type": "String"}]
        # BuzzHouse sends `any` when the table has no explicit format, which then defaults to `CSV`
        fmt = "CSV" if table.format in ("", "any") else table.format
        if random.randint(1, 10) == 1:
            # Fields the table does not declare, which ClickHouse skips or counts as broken
            names = {col["name"] for col in columns}
            columns += [
                {
                    "name": f"cx{i}",
                    "type": generator.type_generator.generate_random_clickhouse_type(
                        True, True, random.randint(1, 4), 0
                    ),
                }
                for i in range(random.randint(1, 3))
                if f"cx{i}" not in names
            ]
        # A binary format takes a whole data file per message, written as for the `File` tables
        is_file = fmt.lower() in DATA_FILE_FORMATS
        is_rendered = fmt in _RENDERED_FORMATS

        def valid():
            if is_file:
                return self.spark.file_handler.random_data_file(columns, fmt)
            if is_rendered:
                # Some formats take several rows in one message
                return "\n".join(_row(generator, columns, fmt) for _ in range(random.randint(1, 3)))
            return self.clickhouse_rows(columns, fmt)

        res = []
        # Data files and `clickhouse local` runs are slower, so fewer of them
        for _ in range(random.randint(1, 20) if is_rendered else random.randint(1, 3)):
            kind = random.randint(1, 100)
            if kind <= 2 and is_rendered:
                # Around the limits of the brokers and of ClickHouse (1 MiB by default for Kafka and NATS)
                row = _row(generator, columns, fmt)
                size = random.choice([2**20 - 1024, 2**20 + 1024, 4 * 2**20])
                res.append("\n".join([row] * max(1, size // (len(row) + 1))))
            elif kind <= 80:
                res.append(valid())
            elif kind <= 85:
                res.append("")
            elif kind <= 90:
                message = valid()
                res.append(message[: random.randint(0, max(0, len(message) - 1))])
            elif kind <= 95:
                res.append(generator._rand_string(random.randint(1, 64)))
            else:
                res.append(generator._rand_binary(random.randint(1, 64)))
        return res

    def clickhouse_rows(self, columns: list, fmt: str) -> bytes:
        """Random rows of `columns` in `fmt`, written by `clickhouse local` with the client binary."""
        structure = ", ".join(f"`{col['name']}` {col['type']}" for col in columns)
        query = (
            f"SELECT * FROM generateRandom({_literal(structure)}, {random.randint(1, 2**63 - 1)}, 64, 4) "
            f"LIMIT {random.randint(1, 10)} FORMAT {fmt}"
        )
        result = subprocess.run(
            [self.cluster.client_bin_path, "local", *self.allow_all_types, "--query", query],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            timeout=60,
            check=False,
        )
        if result.returncode != 0:
            raise Exception(f"clickhouse local failed for {query}: {result.stderr.decode(errors='replace')}")
        return result.stdout

    def create_table(
        self,
        database_name: str,
        table_name: str,
        topic: str,
        group: str,
        fmt: str,
        columns: list,
    ) -> bool:
        table = StreamingTable(topic, group, fmt, columns)
        self.tables[f"{database_name}.{table_name}"] = table
        try:
            self.on_create(table)
        except Exception as e:
            # The broker may be restarting, the table is created anyway
            traceback.print_exc()
            self.logger.exception(e)
        return True

    def update_table(self, database_name: str, table_name: str) -> bool:
        table = self.tables.get(f"{database_name}.{table_name}")
        if table is None or random.randint(1, 10) > 8:
            # Another topic, maybe of another table
            table = StreamingTable(f"t{random.randint(0, 19)}", "", "CSV", [])
        try:
            self.on_update(table)
        except Exception as e:
            # If an error happens, ignore it, but log it
            traceback.print_exc()
            self.logger.exception(e)
        return True

    def on_create(self, table: StreamingTable):
        if random.randint(1, 2) == 1:
            self.produce(table, self.next_messages(table))

    def on_update(self, table: StreamingTable):
        self.produce(table, self.next_messages(table))

    def produce(self, table: StreamingTable, messages: list):
        raise NotImplementedError


class KafkaHandler(StreamingHandler):
    """Topics are shared by name across tables, and the group of a table is its consumer group."""

    def __init__(self, cluster, spark_handler):
        super().__init__(cluster, spark_handler)
        self.admin_client = None
        self.producer = None

    def admin(self) -> KafkaAdminClient:
        if self.admin_client is None:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers="localhost:{}".format(self.cluster.kafka_port)
            )
        return self.admin_client

    def run_admin(self, action):
        """Run `action` on the admin client, dropping the client after a failure: the broker may have restarted."""
        try:
            action(self.admin())
        except (TopicAlreadyExistsError, UnknownTopicOrPartitionError):
            # The topic names are shared, so the topic routinely exists already or is gone
            pass
        except Exception:
            if self.admin_client is not None:
                self.admin_client.close()
            self.admin_client = None
            raise

    def create_topic(self, topic: str):
        num_partitions = 1 if random.randint(1, 2) == 1 else random.randint(1, 32)
        # A compacted topic keeps only the last message of each key, and drops a key at its tombstone
        configs = {"cleanup.policy": "compact"} if random.randint(1, 5) == 1 else None
        # Without the retries of `kafka_create_topic`, which spin on a topic that exists
        self.run_admin(
            lambda admin: admin.create_topics(
                [
                    NewTopic(
                        name=topic,
                        num_partitions=num_partitions,
                        replication_factor=1,
                        topic_configs=configs,
                    )
                ]
            )
        )

    def produce(self, table: StreamingTable, messages: list):
        try:
            if self.producer is None:
                self.producer = k.get_kafka_producer(self.cluster.kafka_port, k.producer_serializer, 15)
            now_ms = int(time.time() * 1000)
            for message in messages:
                key = f"k{random.randint(0, 9)}".encode() if random.randint(1, 2) == 1 else None
                headers = None
                if random.randint(1, 4) == 1:
                    headers = [(f"h{i}", str(random.randint(0, 9)).encode()) for i in range(random.randint(1, 3))]
                timestamp_ms = random.choice([None, None, now_ms, 0, now_ms - 86400000])
                # A tombstone: a message with a key and without a value
                value = None if key is not None and random.randint(1, 15) == 1 else message
                self.producer.send(
                    topic=table.topic, value=value, key=key, headers=headers, timestamp_ms=timestamp_ms
                )
            self.producer.flush()
        except Exception:
            if self.producer is not None:
                self.producer.close()
            self.producer = None
            raise

    def on_create(self, table: StreamingTable):
        self.create_topic(table.topic)
        super().on_create(table)
        if random.randint(1, 10) == 1:
            self.run_admin(lambda admin: admin.delete_topics([table.topic]))

    def on_update(self, table: StreamingTable):
        next_operation = random.randint(1, 1000)
        if next_operation <= 150:
            self.create_topic(table.topic)
        elif next_operation <= 200:
            self.run_admin(lambda admin: admin.delete_topics([table.topic]))
        elif next_operation <= 250:
            # More partitions rebalance the consumers, fewer than the current count fail
            self.run_admin(
                lambda admin: admin.create_partitions(
                    {table.topic: NewPartitions(total_count=random.randint(2, 32))}
                )
            )
        elif table.group and next_operation <= 280:
            # Fails while the group has members, otherwise its committed offsets are gone
            self.run_admin(lambda admin: admin.delete_consumer_groups([table.group]))
        elif next_operation <= 300:
            # A short retention deletes messages under the consumers, whose offsets go out of range,
            # and compaction drops all but the last message of each key
            configs = random.choice(
                [
                    {"retention.ms": random.choice(["100", "1000", "604800000"])},
                    {"cleanup.policy": random.choice(["compact", "delete", "compact,delete"])},
                ]
            )
            self.run_admin(
                lambda admin: admin.alter_configs(
                    [ConfigResource(ConfigResourceType.TOPIC, table.topic, configs=configs)]
                )
            )
        else:
            super().on_update(table)


def _encode(message) -> bytes:
    return message if isinstance(message, bytes) else message.encode()


class NATSHandler(StreamingHandler):
    """Core NATS subjects need no creation. A table with a group reads from the JetStream stream
    of that name through the durable consumer `c<group>`, both of which this handler creates."""

    async def _run(self, action):
        nc = await nats_connect_ssl(self.cluster)
        try:
            await action(nc, nc.jetstream())
        finally:
            await nc.close()

    def run(self, action):
        asyncio.run(self._run(action))

    @staticmethod
    async def _add_stream(js, table: StreamingTable):
        await js.add_stream(name=table.group, subjects=[table.topic])
        if random.randint(1, 10) < 10:
            consumer = f"c{table.group}"
            await js.add_consumer(
                stream=table.group,
                config=api.ConsumerConfig(name=consumer, durable_name=consumer),
            )

    def produce(self, table: StreamingTable, messages: list):
        async def publish(nc, _):
            for message in messages:
                headers = None
                if random.randint(1, 4) == 1:
                    headers = {f"h{i}": str(random.randint(0, 9)) for i in range(random.randint(1, 3))}
                await nc.publish(table.topic, _encode(message), headers=headers)
            await nc.flush()

        self.run(publish)

    def on_create(self, table: StreamingTable):
        if table.group and random.randint(1, 10) < 10:
            self.run(lambda _, js: self._add_stream(js, table))
        super().on_create(table)

    def on_update(self, table: StreamingTable):
        next_operation = random.randint(1, 1000)
        if table.group and next_operation <= 40:
            self.run(lambda _, js: js.purge_stream(table.group))
        elif table.group and next_operation <= 60:
            self.run(lambda _, js: js.delete_stream(table.group))
        elif table.group and next_operation <= 110:
            self.run(lambda _, js: self._add_stream(js, table))
        elif table.group and next_operation <= 130:
            self.run(lambda _, js: js.delete_consumer(table.group, f"c{table.group}"))
        else:
            super().on_update(table)


class RabbitMQHandler(StreamingHandler):
    """The table declares its exchange and queues itself, unless `rabbitmq_queue_consume` makes it read
    from the queue named by its group. Declaring or deleting them here also races with ClickHouse."""

    # The broker's default user in `docker_compose_rabbitmq.yml`
    CREDENTIALS = ("root", "clickhouse")
    # `fanout` is the type the table declares when `rabbitmq_exchange_type` is not set
    EXCHANGE_TYPES = ["fanout", "fanout", "fanout", "direct", "topic", "headers"]

    def run(self, action):
        parameters = pika.ConnectionParameters(
            self.cluster.rabbitmq_ip,
            self.cluster.rabbitmq_port,
            "/",
            pika.PlainCredentials(*self.CREDENTIALS),
        )
        connection = pika.BlockingConnection(parameters)
        try:
            action(connection.channel())
        finally:
            connection.close()

    @staticmethod
    def _queues(table: StreamingTable) -> list[str]:
        # A `consistent_hash` exchange prefixes the queue base with the queue number
        return [table.group, f"0_{table.group}", f"1_{table.group}"] if table.group else []

    def _on_each_queue(self, table: StreamingTable, action):
        # A missing queue closes the channel, so each queue gets its own
        for queue in self._queues(table):
            try:
                self.run(lambda channel: action(channel, queue))
            except pika.exceptions.ChannelClosedByBroker:
                pass

    def _declare_queue(self, table: StreamingTable):
        def declare(channel):
            channel.exchange_declare(exchange=table.topic, exchange_type="fanout")
            channel.queue_declare(queue=table.group, durable=random.choice([False, True]))
            channel.queue_bind(queue=table.group, exchange=table.topic)

        self.run(declare)

    def _close_all_connections(self):
        subprocess.run(
            docker_exec(
                "-e",
                f"RABBITMQ_ERLANG_COOKIE={self.cluster.rabbitmq_cookie}",
                self.cluster.rabbitmq_docker_id,
                "rabbitmqctl",
                "close_all_connections",
                "dolor",
            ),
            capture_output=True,
            timeout=60,
            check=False,
        )

    def produce(self, table: StreamingTable, messages: list):
        def publish(channel):
            for message in messages:
                properties = pika.BasicProperties(
                    message_id=str(uuid.uuid4()) if random.randint(1, 2) == 1 else None,
                    timestamp=int(time.time()) if random.randint(1, 2) == 1 else None,
                    headers={"h": str(random.randint(0, 9))} if random.randint(1, 4) == 1 else None,
                    delivery_mode=random.choice([1, 2]),
                )
                # Publishing to an exchange that does not exist closes the channel
                channel.basic_publish(
                    exchange=table.topic,
                    routing_key=random.choice(["", "0", "1", "a"]),
                    body=message,
                    properties=properties,
                )

        self.run(publish)

    def on_create(self, table: StreamingTable):
        if table.group and random.randint(1, 10) <= 3:
            self._declare_queue(table)
        super().on_create(table)

    def on_update(self, table: StreamingTable):
        next_operation = random.randint(1, 1000)
        if next_operation <= 100:
            self.run(
                lambda channel: channel.exchange_declare(
                    exchange=table.topic, exchange_type=random.choice(self.EXCHANGE_TYPES)
                )
            )
        elif next_operation <= 150:
            self.run(lambda channel: channel.exchange_delete(exchange=table.topic))
        elif table.group and next_operation <= 200:
            self._declare_queue(table)
        elif table.group and next_operation <= 250:
            self._on_each_queue(table, lambda channel, queue: channel.queue_purge(queue))
        elif table.group and next_operation <= 280:
            self._on_each_queue(table, lambda channel, queue: channel.queue_delete(queue))
        elif next_operation <= 300:
            # Every consumer of every table loses its connection at once
            self._close_all_connections()
        else:
            super().on_update(table)
