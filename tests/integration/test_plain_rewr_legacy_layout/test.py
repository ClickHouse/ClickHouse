import os
import random
import string

import pytest

from minio.commonconfig import CopySource

from helpers.cluster import ClickHouseCluster
from helpers.s3_tools import get_file_contents

cluster = ClickHouseCluster(__file__)

NUM_WORKERS = 2
MAX_ROWS = 100

key_prefix = "data/"


def gen_insert_values(size):
    return ",".join(
        f"({i},'{''.join(random.choices(string.ascii_lowercase, k=5))}')"
        for i in range(size)
    )


def randomize_table_name(table_name, suffix_length=10):
    return f"{table_name}{''.join(random.choice(string.ascii_letters) for _ in range(suffix_length))}"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    node_names = ["source", "dest"]
    for i in range(NUM_WORKERS):
        cluster.add_instance(
            node_names[i],
            main_configs=["configs/storage_conf.xml"],
            with_minio=True,
            env_variables={"ENDPOINT_SUBPATH": node_names[i]},
            stay_alive=True,
            # Override ENDPOINT_SUBPATH.
            instance_env_variables=i > 0,
        )

    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    "storage_policy",
    [
        pytest.param("s3_plain_rewritable"),
    ],
)
def test(storage_policy):
    def create_insert(node, table_name, insert_values):
        node.query(
            """
            CREATE TABLE {} (
                id Int64,
                data String
            ) ENGINE=MergeTree()
            ORDER BY id
            SETTINGS storage_policy='{}'
            """.format(table_name, storage_policy)
        )
        if insert_values:
            node.query("INSERT INTO {} VALUES {}".format(table_name, insert_values))

    insert_values = gen_insert_values(random.randint(1, MAX_ROWS))

    source = cluster.instances["source"]
    table_name = randomize_table_name("test")
    create_insert(source, table_name, insert_values)

    def copy_to_legacy_layout(source, dest):
        metadata_it = cluster.minio_client.list_objects(
            cluster.minio_bucket, f"{key_prefix}{source}/__meta/", recursive=True
        )
        remote_to_local = {}
        for obj in metadata_it:
            remote_path = obj.object_name
            assert remote_path.endswith("prefix.path")

            local_path = get_file_contents(
                cluster.minio_client, cluster.minio_bucket, remote_path
            ).strip()
            remote_dir = os.path.dirname(remote_path).replace("__meta/", "")
            remote_to_local[remote_dir] = local_path

        data_it = cluster.minio_client.list_objects(
            cluster.minio_bucket, f"{key_prefix}{source}/", recursive=True
        )

        for obj in data_it:
            remote_path = obj.object_name
            remote_dir = os.path.dirname(remote_path).replace("__meta/", "")
            assert remote_dir in remote_to_local
            filename = os.path.basename(remote_path)

            destination_key = (
                f"{key_prefix}{dest}/{remote_to_local[remote_dir]}{filename}"
            )

            cluster.minio_client.copy_object(
                cluster.minio_bucket,
                destination_key,
                CopySource(cluster.minio_bucket, obj.object_name),
            )

    copy_to_legacy_layout("source", "dest")

    dest = cluster.instances["dest"]
    dest.restart_clickhouse()

    assert (
        int(
            dest.query(
                "SELECT value FROM system.events WHERE name = 'DiskPlainRewritableLegacyLayoutDiskCount'"
            ).strip()
        )
        > 0
    )

    source.query(f"DROP TABLE {table_name}")
