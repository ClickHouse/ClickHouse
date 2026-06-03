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
    stay_alive=True,
    cpu_limit=False,
)
node2 = cluster.add_instance(
    "node2",
    with_elasticsearch=True,
    with_zookeeper=True,
    with_remote_database_disk=False,
    cpu_limit=False,
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
                    "tie": {"type": "long"},
                    "message": {"type": "keyword"},
                }
            }
        },
        timeout=10,
    )
    response.raise_for_status()

    body = []
    for position, row in enumerate(rows):
        source = dict(row)
        document_id = str(source.pop("_id", position))
        body.append(json.dumps({"index": {"_index": index, "_id": document_id}}))
        body.append(json.dumps(source))

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
        """,
        settings={"allow_experimental_elasticsearch_queue": 1},
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


def test_user_query_cannot_override_internal_cursor_or_pit(started_cluster, database):
    index = f"query_reserved_{database}"
    put_elasticsearch_documents(
        index,
        [
            {"seq": 1, "message": "keep"},
            {"seq": 2, "message": "skip"},
            {"seq": 3, "message": "keep"},
        ],
    )

    query_body = json.dumps(
        {
            "query": {"term": {"message": "keep"}},
            "pit": {"id": "stale-pit", "keep_alive": "1m"},
            "search_after": [999],
            "size": 1,
            "sort": [{"seq": "desc"}],
        }
    )

    create_target_and_queue(
        node1,
        database,
        index,
        f"""
            , elasticsearch_query = '{query_body}'
        """,
    )

    assert_eq_with_retry(
        node1,
        f"SELECT seq FROM {database}.dst ORDER BY seq",
        "1\n3\n",
        retry_count=80,
        sleep_time=0.5,
    )


def test_direct_select_is_rejected_while_materialized_view_is_attached(started_cluster, database):
    index = f"direct_select_{database}"
    put_elasticsearch_documents(index, [{"seq": 1, "message": "value-1"}])

    create_target_and_queue(node1, database, index, "")

    assert_eq_with_retry(
        node1,
        f"SELECT seq FROM {database}.dst ORDER BY seq",
        "1\n",
        retry_count=80,
        sleep_time=0.5,
    )

    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            f"SELECT * FROM {database}.queue",
            settings={"stream_like_engine_allow_direct_select": 1},
        )

    assert "attached materialized views" in str(exc.value)


def test_local_checkpoint_is_loaded_after_restart(started_cluster, database):
    index = f"restart_{database}"
    put_elasticsearch_documents(index, [{"seq": i, "message": f"value-{i}"} for i in range(1, 4)])

    create_target_and_queue(node1, database, index, "")

    assert_eq_with_retry(
        node1,
        f"SELECT seq FROM {database}.dst ORDER BY seq",
        "1\n2\n3\n",
        retry_count=80,
        sleep_time=0.5,
    )

    node1.restart_clickhouse()

    put_elasticsearch_documents(index, [{"seq": i, "message": f"value-{i}"} for i in range(1, 6)])

    assert_eq_with_retry(
        node1,
        f"SELECT seq, count() FROM {database}.dst GROUP BY seq ORDER BY seq",
        "1\t1\n2\t1\n3\t1\n4\t1\n5\t1\n",
        retry_count=80,
        sleep_time=0.5,
    )


def test_tiebreaker_prevents_duplicate_cursor_skip_across_batch_boundary(started_cluster, database):
    index = f"tiebreaker_{database}"
    put_elasticsearch_documents(
        index,
        [
            {"_id": "1", "seq": 1, "tie": 1, "message": "value-1"},
            {"_id": "2", "seq": 1, "tie": 2, "message": "value-2"},
            {"_id": "3", "seq": 1, "tie": 3, "message": "value-3"},
            {"_id": "4", "seq": 2, "tie": 1, "message": "value-4"},
        ],
    )

    create_target_and_queue(
        node1,
        database,
        index,
        """
            , elasticsearch_tiebreaker_field = 'tie'
        """,
    )

    assert_eq_with_retry(
        node1,
        f"SELECT message FROM {database}.dst ORDER BY message",
        "value-1\nvalue-2\nvalue-3\nvalue-4\n",
        retry_count=80,
        sleep_time=0.5,
    )


def test_experimental_setting_is_required(started_cluster, database):
    with pytest.raises(QueryRuntimeException) as exc:
        node1.query(
            f"""
            CREATE TABLE {database}.disabled_engine
            (
                seq UInt64,
                message String
            )
            ENGINE = ElasticsearchQueue('{clickhouse_elasticsearch_url()}', 'missing-index', 'seq')
            """
        )

    assert "allow_experimental_elasticsearch_queue" in str(exc.value)


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
            """,
            settings={"allow_experimental_elasticsearch_queue": 1},
        )

    assert "elasticsearch_auth_type=none" in str(exc.value)
