import json
import re
import time

import pytest
import requests

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


pytestmark = pytest.mark.timeout(600)


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_elasticsearch=True,
    with_zookeeper=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    with_elasticsearch=True,
    with_zookeeper=True,
    with_remote_database_disk=False,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture()
def database(request):
    name = "db_" + re.sub(r"[^0-9a-zA-Z_]", "_", request.node.name)
    node1.query(f"CREATE DATABASE {name}")
    node2.query(f"CREATE DATABASE {name}")
    try:
        yield name
    finally:
        node1.query(f"DROP DATABASE IF EXISTS {name} SYNC")
        node2.query(f"DROP DATABASE IF EXISTS {name} SYNC")


def elasticsearch_url():
    return f"http://localhost:{cluster.elasticsearch_port}"


def clickhouse_elasticsearch_url():
    return f"http://{cluster.elasticsearch_host}:9200"


def put_elasticsearch_documents(index, rows):
    requests.delete(f"{elasticsearch_url()}/{index}", timeout=10)
    response = requests.put(
        f"{elasticsearch_url()}/{index}",
        json={
            "mappings": {
                "properties": {
                    "seq": {"type": "long"},
                    "message": {"type": "keyword"},
                }
            }
        },
        timeout=10,
    )
    response.raise_for_status()

    body = []
    for row in rows:
        body.append(json.dumps({"index": {"_index": index, "_id": str(row["seq"])}}))
        body.append(json.dumps(row))

    response = requests.post(
        f"{elasticsearch_url()}/_bulk",
        data="\n".join(body) + "\n",
        headers={"Content-Type": "application/x-ndjson"},
        timeout=10,
    )
    response.raise_for_status()
    assert not response.json()["errors"]

    response = requests.post(f"{elasticsearch_url()}/{index}/_refresh", timeout=10)
    response.raise_for_status()


def wait_combined_sequences(database_name, expected):
    deadline = time.monotonic() + 60
    last_value = []
    while time.monotonic() < deadline:
        last_value = []
        for node in (node1, node2):
            values = node.query(f"SELECT seq FROM {database_name}.dst ORDER BY seq").strip()
            if values:
                last_value.extend(int(value) for value in values.splitlines())

        if sorted(last_value) == expected:
            return

        time.sleep(0.5)

    pytest.fail(f"Expected combined ElasticsearchQueue rows {expected}, got {sorted(last_value)}")


def create_target_and_queue(node, database_name, index, extra_settings):
    node.query(
        f"""
        CREATE TABLE {database_name}.dst
        (
            seq UInt64,
            message String
        )
        ENGINE = MergeTree
        ORDER BY seq
        """
    )
    node.query(
        f"""
        CREATE TABLE {database_name}.queue
        (
            seq UInt64,
            message String
        )
        ENGINE = ElasticsearchQueue('{clickhouse_elasticsearch_url()}', '{index}', 'seq')
        SETTINGS
            elasticsearch_poll_max_batch_size = 2,
            elasticsearch_consumer_reschedule_ms = 100
            {extra_settings}
        """
    )
    node.query(
        f"""
        CREATE MATERIALIZED VIEW {database_name}.mv TO {database_name}.dst AS
        SELECT seq, message FROM {database_name}.queue
        """
    )


def test_point_in_time_lifecycle(started_cluster, database):
    index = f"pit_{database}"
    put_elasticsearch_documents(index, [{"seq": i, "message": f"value-{i}"} for i in range(1, 6)])

    create_target_and_queue(
        node1,
        database,
        index,
        """
            , elasticsearch_use_point_in_time = 1
            , elasticsearch_pit_keep_alive = '30s'
        """,
    )

    assert_eq_with_retry(
        node1,
        f"SELECT count(), sum(seq), uniqExact(seq) FROM {database}.dst",
        "5\t15\t5\n",
        retry_count=80,
        sleep_time=0.5,
    )


def test_keeper_checkpoint_is_shared_between_replicas(started_cluster, database):
    index = f"keeper_{database}"
    put_elasticsearch_documents(index, [{"seq": i, "message": f"value-{i}"} for i in range(1, 7)])

    keeper_path = f"/clickhouse/elasticsearch_queue/{database}"
    extra_settings = f"""
            , elasticsearch_keeper_path = '{keeper_path}'
            , elasticsearch_keeper_checkpoint_name = 'checkpoint'
        """
    create_target_and_queue(node1, database, index, extra_settings)
    create_target_and_queue(node2, database, index, extra_settings)

    wait_combined_sequences(database, [1, 2, 3, 4, 5, 6])


def test_authentication_settings_validation(started_cluster, database):
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            f"""
            CREATE TABLE {database}.bad_auth
            (
                seq UInt64,
                message String
            )
            ENGINE = ElasticsearchQueue('{clickhouse_elasticsearch_url()}', 'missing-index', 'seq')
            SETTINGS
                elasticsearch_auth_type = 'none',
                elasticsearch_api_key = 'secret'
            """
        )

    assert "elasticsearch_auth_type=none" in str(exc.value)
