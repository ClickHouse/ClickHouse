import logging
import random
import traceback

from integration.helpers.kafka.common_direct import KafkaAdminClient
import integration.helpers.kafka.common as k


class KafkaTable:

    def __init__(
        self,
        _database_name: str,
        _table_name: str,
        _columns: dict[str, str],
        _topic: str,
        _format: str,
    ):
        self.database_name = _database_name
        self.table_name = _table_name
        self.columns = _columns
        self.topic = _topic
        self.format = _format


class KafkaHandler:

    def __init__(self, cluster):
        self.table_topics: dict[str, KafkaTable] = {}
        self.logger = logging.getLogger(__name__)
        self.admin_client = KafkaAdminClient(
            bootstrap_servers="localhost:{}".format(cluster.kafka_port)
        )

    def create_kafka_table(
        self,
        cluster,
        database_name: str,
        table_name: str,
        topic: str,
        format: str,
        columns: dict[str, str],
    ) -> bool:
        num_partitions = 1 if random.randint(1, 2) == 1 else random.randint(1, 32)
        next_tbl = f"{database_name}.{table_name}"

        self.table_topics[next_tbl] = KafkaTable(
            database_name,
            table_name,
            columns,
            topic,
            format,
        )
        k.kafka_create_topic(self.admin_client, topic, num_partitions)
        if random.randint(1, 2) == 1:
            # TODO produce some messages
            k.kafka_produce(cluster, topic, ["aa"])
        if random.randint(1, 10) == 1:
            k.kafka_delete_topic(self.admin_client, topic)
        return True

    def update_table(self, cluster, database_name: str, table_name: str) -> bool:
        next_operation = random.randint(1, 1000)
        next_topic = f"t{random.randint(0, 19)}"
        next_tbl = f"{database_name}.{table_name}"
        if next_tbl in self.table_topics and random.randint(1, 10) < 9:
            next_topic = self.table_topics[next_tbl].topic

        try:
            if next_operation <= 150:
                # Create topic
                num_partitions = (
                    1 if random.randint(1, 2) == 1 else random.randint(1, 32)
                )
                k.kafka_create_topic(self.admin_client, next_topic, num_partitions)
            elif next_operation <= 200:
                # Delete topic
                k.kafka_delete_topic(self.admin_client, next_topic)
            else:
                # Produce
                # TODO produce some messages
                k.kafka_produce(cluster, next_topic, ["aa"])
        except Exception as e:
            # If an error happens, ignore it, but log it
            traceback.print_exc()
            self.logger.exception(e)
        return True
