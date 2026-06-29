import logging
import pytest

from helpers.cluster import ClickHouseCluster


def get_cluster(with_minio):
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node",
        main_configs=["configs/storage_conf.xml"],
        user_configs=["configs/users.xml"],
        with_minio=with_minio,
        stay_alive=True,
        # remote database disk adds MinIO implicitly
        # FIXME: disable with_remote_database_disk if with_minio set to False explicitly
        with_remote_database_disk=False,
    )
    logging.info("Starting cluster...")
    cluster.start()
    logging.info("Cluster started")

    return cluster


# ClickHouse checks extra (AKA orpahned) parts on different disks, in order to not allow to miss data parts at undefined disks.
# The test verifies how the search of orphaned parts works if there is no connection to MinIO.
# The following is expected
# * search_orphaned_parts_disks is `none` - does not search s3, the query is successful
# * search_orphaned_parts_disks is `local` - does not search s3, the query is successful
# * search_orphaned_parts_disks is `any` - searches s3, the query throws if no MinIO
# Note that disk_s3_plain is configured disk that is not used either in n_s3 or local_cache policies.
@pytest.mark.parametrize("with_minio", [True, False])
def test_search_orphaned_parts(with_minio):
    table_name = "t1"

    try:
        cluster = get_cluster(with_minio)

        node = cluster.instances["node"]

        for search_mode in ["any", "local", "none"]:
            for storage_policy in ["no_s3", "local_cache"]:
                node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

                if search_mode == "any" and not with_minio:
                    assert "Code: 499. DB::Exception" in node.query_and_get_error(
                        f"""
                        CREATE TABLE {table_name} (
                        id Int64,
                        data String
                        ) ENGINE=MergeTree()
                        PARTITION BY id % 10
                        ORDER BY id
                        SETTINGS storage_policy='{storage_policy}', search_orphaned_parts_disks='{search_mode}'
                        """
                    )
                else:
                    node.query(
                        f"""
                        CREATE TABLE {table_name} (
                        id Int64,
                        data String
                        ) ENGINE=MergeTree()
                        PARTITION BY id % 10
                        ORDER BY id
                        SETTINGS storage_policy='{storage_policy}', search_orphaned_parts_disks='{search_mode}'
                        """
                    )
                node.query(f"DROP TABLE IF EXISTS {table_name} SYNC")

    finally:
        cluster.shutdown()
