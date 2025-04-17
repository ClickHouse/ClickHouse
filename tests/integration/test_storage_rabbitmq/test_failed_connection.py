import logging
import time
import json

import pytest
import pika

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster


DEFAULT_TIMEOUT_SEC = 120

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=[
        "configs/rabbitmq.xml",
        "configs/macros.xml",
        "configs/named_collection.xml",
    ],
    user_configs=["configs/users.xml"],
    with_rabbitmq=True,
    stay_alive=True,
)

instance2 = cluster.add_instance(
    "instance2",
    user_configs=["configs/users.xml"],
    with_rabbitmq=True,
)

instance3 = cluster.add_instance(
    "instance3",
    user_configs=["configs/users.xml"],
    main_configs=[
        "configs/rabbitmq.xml",
        "configs/macros.xml",
        "configs/named_collection.xml",
        "configs/mergetree.xml",
    ],
    with_rabbitmq=True,
    stay_alive=True,
)

# Helpers


class RabbitMQMonitor:
    # The RabbitMQMonitor class aims to trace all published and delivered events of RabbitMQ
    # It servers as an additional check to see whether the error happens in ClickHouse or
    # in the RabbitMQ server itself.

    published = set()
    delivered = set()
    connection = None
    channel = None
    queue_name = None
    rabbitmq_cluster = None
    expected_published = 0
    expected_delivered = 0

    def _consume(self, timeout=180):
        logging.debug("RabbitMQMonitor: Consuming trace RabbitMQ messages...")
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            method, properties, body = self.channel.basic_get(self.queue_name, True)
            if method and properties and body:
                # logging.debug(f"Message received! method {method}, properties {properties}, body {body}")
                message = json.loads(body.decode("utf-8"))
                assert message["key"] == message["value"]
                value = int(message["key"])
                if "deliver" in method.routing_key:
                    self.delivered.add(value)
                    # logging.debug(f"Message delivered: {value}")
                elif "publish" in method.routing_key:
                    self.published.add(value)
                    # logging.debug(f"Message published: {value}")
            else:
                break
        logging.debug(f"RabbitMQMonitor: Consumed {len(self.published)} published messages and {len(self.delivered)} delivered messages")

    def set_expectations(self, published, delivered):
        self.expected_published = published
        self.expected_delivered = delivered

    def check(self):
        self._consume()

        def _get_non_present(my_set, amount):
            non_present = list()
            for i in range(amount):
                if i not in my_set:
                    non_present.append(i)
                    if (len(non_present) >= 10):
                        break
            return non_present

        if self.expected_published > 0 and self.expected_published != len(self.published):
            logging.warning(f"RabbitMQMonitor: {len(self.published)}/{self.expected_published} (got/expected) messages published. Sample of not published: {_get_non_present(self.published, self.expected_published)}")
        if self.expected_delivered > 0 and self.expected_delivered != len(self.delivered):
            logging.warning(f"RabbitMQMonitor: {len(self.delivered)}/{self.expected_delivered} (got/expected) messages delivered. Sample of not delivered: {_get_non_present(self.delivered, self.expected_delivered)}")

    def start(self, rabbitmq_cluster):
        self.rabbitmq_cluster = rabbitmq_cluster

        logging.debug("RabbitMQMonitor: Creating a new connection for RabbitMQ")
        credentials = pika.PlainCredentials("root", "clickhouse")
        parameters = pika.ConnectionParameters(
            self.rabbitmq_cluster.rabbitmq_ip, self.rabbitmq_cluster.rabbitmq_port, "/", credentials
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

        if not self.queue_name:
            queue_res = self.channel.queue_declare(queue="", durable=True)
            self.queue_name = queue_res.method.queue
            logging.debug(f"RabbitMQMonitor: Created debug queue to monitor RabbitMQ published and delivered messages: {self.queue_name}")

        self.channel.queue_bind(exchange="amq.rabbitmq.trace", queue=self.queue_name, routing_key="publish.#")
        self.channel.queue_bind(exchange="amq.rabbitmq.trace", queue=self.queue_name, routing_key="deliver.#")

    def stop(self):
        if self.connection:
            self._consume()
            self.channel.close()
            self.channel = None
            self.connection.close()
            self.connection = None


def suspend_rabbitmq(rabbitmq_cluster, rabbitmq_monitor):
    rabbitmq_monitor.stop()
    rabbitmq_cluster.stop_rabbitmq_app()


def resume_rabbitmq(rabbitmq_cluster, rabbitmq_monitor):
    rabbitmq_cluster.start_rabbitmq_app()
    rabbitmq_cluster.wait_rabbitmq_to_start()
    rabbitmq_monitor.start(rabbitmq_cluster)


# Fixtures

@pytest.fixture(scope="module")
def rabbitmq_cluster():
    try:
        cluster.start()
        cluster.run_rabbitmqctl("trace_on")
        logging.debug("rabbitmq_id is {}".format(instance.cluster.rabbitmq_docker_id))
        logging.getLogger("pika").propagate = False
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def rabbitmq_monitor():
    logging.debug("RabbitMQ is available - running test")
    instance.query("CREATE DATABASE test")
    instance3.query("CREATE DATABASE test")
    monitor = RabbitMQMonitor()
    monitor.start(cluster)
    yield monitor
    instance.query("DROP DATABASE test SYNC")
    instance3.query("DROP DATABASE test SYNC")
    monitor.check()
    monitor.stop()
    cluster.reset_rabbitmq()


# Tests

@pytest.mark.skip(reason="Too flaky. Disable for now")
def test_rabbitmq_restore_failed_connection_without_losses_1(rabbitmq_cluster, rabbitmq_monitor):
    """
    This test checks that after inserting through a RabbitMQ Engine, we can keep consuming from it
    automatically after suspending and resuming the RabbitMQ server. To do that, we need the
    consumption to be slow enough (hence, the rabbitmq_max_block_size = 1) so that we can check that
    something has already been consumed before suspending RabbitMQ server, but not so fast so that
    everything is consumed before suspending and resuming the RabbitMQ server.
    """
    instance.query(
        """
        DROP TABLE IF EXISTS test.consume;
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE TABLE test.consume (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                    rabbitmq_flush_interval_ms=1000,
                    rabbitmq_max_block_size = 1,
                    rabbitmq_exchange_name = 'producer_reconnect',
                    rabbitmq_format = 'JSONEachRow',
                    rabbitmq_num_consumers = 2,
                    rabbitmq_row_delimiter = '\\n';
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consume;
        DROP TABLE IF EXISTS test.producer_reconnect;
        CREATE TABLE test.producer_reconnect (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                    rabbitmq_exchange_name = 'producer_reconnect',
                    rabbitmq_persistent = '1',
                    rabbitmq_flush_interval_ms=1000,
                    rabbitmq_max_block_size = 1,
                    rabbitmq_format = 'JSONEachRow',
                    rabbitmq_row_delimiter = '\\n';
    """
    )

    messages_num = 10000
    rabbitmq_monitor.set_expectations(published=messages_num, delivered=messages_num)
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            instance.query(
                f"INSERT INTO test.producer_reconnect SELECT number, number FROM numbers({messages_num})"
            )
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The query could not be executed successfully."
        )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        number = int(instance.query("SELECT count() FROM test.view"))
        if number != 0:
            logging.debug(f"{number}/{messages_num} before suspending RabbitMQ")
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The count is still 0.")

    suspend_rabbitmq(rabbitmq_cluster, rabbitmq_monitor)

    number = int(instance.query("SELECT count() FROM test.view"))
    logging.debug(f"{number}/{messages_num} after suspending RabbitMQ")
    if number == messages_num:
        pytest.fail("All RabbitMQ messages have been consumed before resuming the RabbitMQ server")

    resume_rabbitmq(rabbitmq_cluster, rabbitmq_monitor)

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view")
        if int(result) == messages_num:
            break
        logging.debug(f"Result: {result} / {messages_num}")
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        DROP TABLE test.consume;
        DROP TABLE test.producer_reconnect;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )


@pytest.mark.skip(reason="Too flaky. Disable for now")
def test_rabbitmq_restore_failed_connection_without_losses_2(rabbitmq_cluster, rabbitmq_monitor):
    """
    This test checks that after inserting through a RabbitMQ Engine, we can keep consuming from it
    automatically after suspending and resuming the RabbitMQ server. To do that, we need the
    consumption to be slow enough (hence, the rabbitmq_max_block_size = 1) so that we can check that
    something has already been consumed before suspending RabbitMQ server, but not so fast so that
    everything is consumed before suspending and resuming the RabbitMQ server.
    """
    instance.query(
        """
        DROP TABLE IF EXISTS test.consumer_reconnect;
        CREATE TABLE test.consumer_reconnect (key UInt64, value UInt64)
            ENGINE = RabbitMQ
            SETTINGS rabbitmq_host_port = 'rabbitmq1:5672',
                    rabbitmq_exchange_name = 'consumer_reconnect',
                    rabbitmq_num_consumers = 2,
                    rabbitmq_flush_interval_ms = 1000,
                    rabbitmq_max_block_size = 1,
                    rabbitmq_num_queues = 10,
                    rabbitmq_format = 'JSONEachRow',
                    rabbitmq_row_delimiter = '\\n';
        CREATE TABLE test.view (key UInt64, value UInt64)
            ENGINE = MergeTree
            ORDER BY key;
        CREATE MATERIALIZED VIEW test.consumer TO test.view AS
            SELECT * FROM test.consumer_reconnect;
    """
    )

    messages_num = 10000
    rabbitmq_monitor.set_expectations(published=messages_num, delivered=messages_num)
    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        try:
            instance.query(
                f"INSERT INTO test.consumer_reconnect SELECT number, number FROM numbers({messages_num})"
            )
            break
        except QueryRuntimeException as e:
            if "Local: Timed out." in str(e):
                continue
            else:
                raise
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The query could not be executed successfully."
        )

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        number = int(instance.query("SELECT count() FROM test.view"))
        if number != 0:
            logging.debug(f"{number}/{messages_num} before suspending RabbitMQ")
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The count is still 0.")

    suspend_rabbitmq(rabbitmq_cluster, rabbitmq_monitor)

    number = int(instance.query("SELECT count() FROM test.view"))
    logging.debug(f"{number}/{messages_num} after suspending RabbitMQ")
    if number == messages_num:
        pytest.fail("All RabbitMQ messages have been consumed before resuming the RabbitMQ server")

    resume_rabbitmq(rabbitmq_cluster, rabbitmq_monitor)

    # while int(instance.query('SELECT count() FROM test.view')) == 0:
    #    time.sleep(0.1)

    # kill_rabbitmq()
    # revive_rabbitmq()

    deadline = time.monotonic() + DEFAULT_TIMEOUT_SEC
    while time.monotonic() < deadline:
        result = instance.query("SELECT count(DISTINCT key) FROM test.view").strip()
        if int(result) == messages_num:
            break
        logging.debug(f"Result: {result} / {messages_num}")
        time.sleep(1)
    else:
        pytest.fail(
            f"Time limit of {DEFAULT_TIMEOUT_SEC} seconds reached. The result did not match the expected value."
        )

    instance.query(
        """
        DROP TABLE test.consumer;
        DROP TABLE test.consumer_reconnect;
    """
    )

    assert int(result) == messages_num, "ClickHouse lost some messages: {}".format(
        result
    )
