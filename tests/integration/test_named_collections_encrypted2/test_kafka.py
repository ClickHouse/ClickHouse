import json
import logging
import time
import uuid

import pytest
from helpers.cluster import ClickHouseCluster


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/config.d/named_collections_with_zookeeper_encrypted.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            stay_alive=True,
            with_zookeeper=True,
            with_kafka=True,
        )
        logging.info("Starting Kafka cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_kafka_with_encrypted_named_collection(cluster):
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic

    node = cluster.instances["node"]
    kafka_port = cluster.kafka_port
    topic_name = f"test_enc_nc_{uuid.uuid4().hex[:8]}"
    group_name = f"test_group_{uuid.uuid4().hex[:8]}"

    admin_client = KafkaAdminClient(bootstrap_servers=f"localhost:{kafka_port}")
    try:
        admin_client.delete_topics([topic_name])
        time.sleep(1)
    except Exception:
        pass

    admin_client.create_topics([NewTopic(name=topic_name, num_partitions=1, replication_factor=1)])

    node.query("DROP NAMED COLLECTION IF EXISTS kafka_encrypted")
    node.query(f"""
        CREATE NAMED COLLECTION kafka_encrypted AS
        kafka_broker_list = 'kafka1:19092',
        kafka_topic_list = '{topic_name}',
        kafka_group_name = '{group_name}',
        kafka_format = 'JSONEachRow'
    """)

    assert node.query("SELECT count() FROM system.named_collections WHERE name = 'kafka_encrypted'").strip() == "1"

    node.query("DROP TABLE IF EXISTS kafka_test")
    node.query("CREATE TABLE kafka_test (id UInt64, value String) ENGINE = Kafka(kafka_encrypted)")

    node.query("DROP TABLE IF EXISTS kafka_dst")
    node.query("CREATE TABLE kafka_dst (id UInt64, value String) ENGINE = MergeTree() ORDER BY id")

    node.query("DROP TABLE IF EXISTS kafka_mv")
    node.query("CREATE MATERIALIZED VIEW kafka_mv TO kafka_dst AS SELECT * FROM kafka_test")

    producer = KafkaProducer(
        bootstrap_servers=f"localhost:{kafka_port}",
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    for i in range(5):
        producer.send(topic_name, {"id": i, "value": f"msg_{i}"})
    producer.flush()

    for _ in range(30):
        count = int(node.query("SELECT count() FROM kafka_dst").strip())
        if count == 5:
            break
        time.sleep(1)

    assert node.query("SELECT count() FROM kafka_dst").strip() == "5"
    assert "msg_2" in node.query("SELECT value FROM kafka_dst WHERE id = 2").strip()

    node.query("DROP TABLE IF EXISTS kafka_mv")
    node.query("DROP TABLE IF EXISTS kafka_test")
    node.query("DROP TABLE IF EXISTS kafka_dst")
    node.query("DROP NAMED COLLECTION kafka_encrypted")

    try:
        admin_client.delete_topics([topic_name])
    except Exception:
        pass
