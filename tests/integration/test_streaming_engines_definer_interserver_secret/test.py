# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

"""
Pushes from streaming engines into a `Distributed` table must reach the shard under the definer.

`Kafka`, `RabbitMQ` and the other streaming engines never write anywhere themselves: a consumed
batch is pushed only into the dependent materialized views, from a copy of the global context that
has no user. A view with `SQL SECURITY DEFINER` switches that push to the definer. When the view's
target is a `Distributed` table over a cluster with an interserver `<secret>`, the definer's name
must travel with the query: before the fix `Context::setUser` left the `ClientInfo` user names
empty, the shard received an empty `initial_user`, treated the query as interserver mode and
executed it with full access. A tenant that could create tables only in its own database could
chain `Kafka -> MATERIALIZED VIEW -> Distributed('secure', default, secret_data)` and write into
a table on the shard it had no `INSERT` grant on.

The `secure` cluster consists of `node2` only, so a table on `node1` can reach `default.secret_data`
solely over the interserver connection. Every pipeline is built twice with the same statements: by
`tenant`, who has no grants on `node2` and whose rows must be rejected there, and by `writer`, who
has `INSERT` on the target and whose rows must arrive. In both cases the shard must see the
definer's name in `system.query_log`.
"""

import json
import time

import pika
import pytest

import helpers.kafka.common as k
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/rabbitmq.xml"],
    user_configs=["configs/users.xml"],
    with_kafka=True,
    with_rabbitmq=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/users.xml"],
)

WRITER = "writer"
TENANT = "tenant"
CREDENTIALS = {
    WRITER: {"user": WRITER, "password": "wpass"},
    TENANT: {"user": TENANT, "password": "tpass"},
}
ROWS_PER_PIPELINE = 2

# How long a streaming engine gets to consume the messages and push them to the shard. Consumer
# group setup in Kafka alone can take several seconds.
DELIVERY_WAIT_SECONDS = 60

users = pytest.mark.parametrize("user", [WRITER, TENANT])


def database(user):
    return f"{user}_db"


def query_as(user, sql):
    return node1.query(sql, **CREDENTIALS[user])


def remote_rows():
    return int(node2.query("SELECT count() FROM default.secret_data"))


def shard_inserts(user, condition):
    """Number of `INSERT` queries into the protected table that the shard executed under `user`."""
    node2.query("SYSTEM FLUSH LOGS")
    return int(
        node2.query(
            f"""
            SELECT count() FROM system.query_log
            WHERE user = '{user}' AND query LIKE 'INSERT INTO default.secret_data%' AND {condition}
            """
        )
    )


def shard_finished_inserts(user):
    return shard_inserts(user, "type = 'QueryFinish'")


def shard_denied_inserts(user):
    # The access check happens before the pipeline starts, so the shard logs the denial as
    # `ExceptionBeforeStart`.
    return shard_inserts(
        user,
        "type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing') AND exception_code = 497",
    )


def wait_until(condition, timeout=DELIVERY_WAIT_SECONDS):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(1)
    return False


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        # The protected table lives on the shard only. Both users exist on both nodes, but on
        # the shard only `writer` may insert into it.
        node2.query(
            """
            CREATE TABLE default.secret_data (key Int) ENGINE = MergeTree ORDER BY key;
            CREATE USER writer IDENTIFIED WITH plaintext_password BY 'wpass';
            CREATE USER tenant IDENTIFIED WITH plaintext_password BY 'tpass';
            GRANT INSERT ON default.secret_data TO writer;
            """
        )
        for user in (WRITER, TENANT):
            db = database(user)
            node1.query(
                f"""
                CREATE USER {user} IDENTIFIED WITH plaintext_password BY '{CREDENTIALS[user]["password"]}';
                CREATE DATABASE {db};
                GRANT ALL ON {db}.* TO {user};
                GRANT TABLE ENGINE ON Distributed, TABLE ENGINE ON Kafka, TABLE ENGINE ON RabbitMQ TO {user};
                """
            )
            query_as(
                user,
                f"CREATE TABLE {db}.dist (key Int) ENGINE = Distributed(secure, default, secret_data)",
            )

        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def clean_remote_table():
    node2.query("TRUNCATE TABLE default.secret_data")
    yield


def drop_pipeline(user, source):
    """Stop the streaming table, so that a rejected push is not retried into the next test."""
    db = database(user)
    query_as(user, f"DROP TABLE {db}.{source}_to_dist")
    query_as(user, f"DROP TABLE {db}.{source} SYNC")


def create_definer_view(user, source):
    """A view owned by `user` that pushes the source rows into the user's `Distributed` table."""
    db = database(user)
    query_as(
        user,
        f"""
        CREATE MATERIALIZED VIEW {db}.{source}_to_dist TO {db}.dist
        DEFINER = CURRENT_USER SQL SECURITY DEFINER
        AS SELECT key FROM {db}.{source}
        """,
    )


def check_delivery(user, finished_before, denied_before):
    """The shard must have executed the push under the user's name, with the outcome the user's
    grants dictate: the rows arrive for `writer` and are rejected for `tenant`."""
    if user == WRITER:
        assert wait_until(lambda: remote_rows() == ROWS_PER_PIPELINE)
        assert shard_finished_inserts(WRITER) > finished_before
    else:
        assert wait_until(
            lambda: shard_denied_inserts(TENANT) > denied_before or remote_rows() > 0
        )
        assert remote_rows() == 0
        assert shard_denied_inserts(TENANT) > denied_before


@users
def test_direct_insert(user):
    """The control case: a direct `INSERT` carries the user's name to the shard, where it is checked."""
    db = database(user)
    if user == WRITER:
        query_as(WRITER, f"INSERT INTO {db}.dist VALUES (1)")
        assert remote_rows() == 1
    else:
        error = node1.query_and_get_error(
            f"INSERT INTO {db}.dist VALUES (1)", **CREDENTIALS[TENANT]
        )
        assert "ACCESS_DENIED" in error
        assert remote_rows() == 0


@users
def test_kafka(user):
    db = database(user)
    topic = f"definer_interserver_{user}"
    k.kafka_create_topic(k.get_admin_client(cluster), topic)

    query_as(
        user,
        f"""
        CREATE TABLE {db}.kafka (key Int) ENGINE = Kafka
        SETTINGS kafka_broker_list = 'kafka1:19092',
                 kafka_topic_list = '{topic}',
                 kafka_group_name = '{topic}',
                 kafka_format = 'JSONEachRow',
                 kafka_flush_interval_ms = 1000
        """,
    )
    create_definer_view(user, "kafka")

    try:
        finished_before = shard_finished_inserts(user)
        denied_before = shard_denied_inserts(user)
        k.kafka_produce(
            cluster, topic, [json.dumps({"key": i}) for i in range(ROWS_PER_PIPELINE)]
        )
        check_delivery(user, finished_before, denied_before)
    finally:
        drop_pipeline(user, "kafka")


@users
def test_rabbitmq(user):
    db = database(user)
    exchange = f"definer_interserver_{user}"

    query_as(
        user,
        f"""
        CREATE TABLE {db}.rabbitmq (key Int) ENGINE = RabbitMQ
        SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                 rabbitmq_exchange_name = '{exchange}',
                 rabbitmq_format = 'JSONEachRow',
                 rabbitmq_flush_interval_ms = 1000,
                 rabbitmq_max_block_size = 100
        """,
    )
    create_definer_view(user, "rabbitmq")

    # The exchange and the queue are declared by the table in the background; messages
    # published before that are dropped by RabbitMQ.
    node1.wait_for_log_line(f"StorageRabbitMQ .{db}.rabbitmq.: RabbitMQ setup completed")
    node1.wait_for_log_line(f"StorageRabbitMQ .{db}.rabbitmq.: Queue .* is declared")

    try:
        finished_before = shard_finished_inserts(user)
        denied_before = shard_denied_inserts(user)

        credentials = pika.PlainCredentials("root", "clickhouse")
        parameters = pika.ConnectionParameters(
            cluster.rabbitmq_ip, cluster.rabbitmq_port, "/", credentials
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        for i in range(ROWS_PER_PIPELINE):
            channel.basic_publish(
                exchange=exchange, routing_key="", body=json.dumps({"key": i})
            )
        connection.close()

        check_delivery(user, finished_before, denied_before)
    finally:
        drop_pipeline(user, "rabbitmq")
