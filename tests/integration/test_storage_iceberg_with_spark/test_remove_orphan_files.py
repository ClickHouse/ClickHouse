import time

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
)


ICEBERG_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "allow_iceberg_remove_orphan_files": 1,
    # The orphans these tests create are fresh.
    "iceberg_orphan_files_min_retention_seconds": 0,
}

AZURE_TABLE_PREFIX = "/var/lib/clickhouse/user_files/iceberg_data/default"


# ---------------------------------------------------------------------------
# Test environment — binds cluster/instance/storage_type/table_name once
# so individual tests only express the orphan-specific logic.
# ---------------------------------------------------------------------------

class OrphanTestEnv:
    def __init__(self, cluster, storage_type, table_prefix):
        self.cluster = cluster
        self.instance = cluster.instances["node1"]
        self.storage_type = storage_type
        self.table_name = f"{table_prefix}_{storage_type}_{get_uuid_str()}"

    # -- table lifecycle -----------------------------------------------------

    def populate(self, n_rows, format_version=2, **kwargs):
        create_iceberg_table(
            self.storage_type, self.instance, self.table_name,
            self.cluster, "(x Int)", format_version, **kwargs,
        )
        for val in range(1, n_rows + 1):
            self.instance.query(
                f"INSERT INTO {self.table_name} VALUES ({val});",
                settings=ICEBERG_SETTINGS,
            )
        self._n_rows = n_rows

    def assert_data_intact(self):
        expected = "".join(f"{i}\n" for i in range(1, self._n_rows + 1))
        assert self.instance.query(
            f"SELECT * FROM {self.table_name} ORDER BY x"
        ) == expected

    # -- orphan file manipulation (Azure blob storage) -----------------------

    def _blob_client(self, subdir, filename):
        return self.cluster.blob_service_client.get_blob_client(
            self.cluster.azure_container_name,
            f"{AZURE_TABLE_PREFIX}/{self.table_name}/{subdir}/{filename}",
        )

    def add_orphan(self, subdir="data", filename="orphan.parquet"):
        self._blob_client(subdir, filename).upload_blob(b"orphan_data", overwrite=True)

    def exists(self, subdir, filename):
        try:
            self._blob_client(subdir, filename).get_blob_properties()
            return True
        except Exception:
            return False

    # -- command execution --------------------------------------------------

    def remove_orphans(self, older_than, dry_run=0):
        raw = self.instance.query(
            f"ALTER TABLE {self.table_name} EXECUTE remove_orphan_files(older_than = '{older_than}', dry_run = {dry_run});",
            settings=ICEBERG_SETTINGS,
        )
        counts = {}
        for line in raw.strip().split("\n"):
            parts = line.split("\t")
            if len(parts) == 2:
                counts[parts[0]] = int(parts[1])
        return counts

    @staticmethod
    def now_ts():
        return time.strftime("%Y-%m-%d %H:%M:%S")


def make_env(cluster, storage_type, prefix):
    return OrphanTestEnv(cluster, storage_type, prefix)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("storage_type", ["azure"])
def test_remove_orphan_files_azure(started_cluster_iceberg_with_spark, storage_type):
    """Orphan removal on Azure (Azurite) backend: create orphans, verify deletion."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_azure")
    env.populate(2)

    env.add_orphan("data", "orphan-azure-001.parquet")
    env.add_orphan("data", "orphan-azure-002.parquet")
    time.sleep(2)

    assert env.exists("data", "orphan-azure-001.parquet")
    assert env.exists("data", "orphan-azure-002.parquet")

    counts = env.remove_orphans(older_than=env.now_ts())
    assert len(counts) == 9, f"Expected 9 metrics, got {counts}"
    assert counts["deleted_data_files_count"] >= 2, f"Expected >= 2 deleted data files, got {counts}"

    assert not env.exists("data", "orphan-azure-001.parquet")
    assert not env.exists("data", "orphan-azure-002.parquet")
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["azure"])
def test_remove_orphan_files_azure_directory_entries(started_cluster_iceberg_with_spark, storage_type):
    """An account with a hierarchical namespace lists each directory as an empty blob with
    `hdi_isfolder=true` metadata. Azurite has no such namespace, so the entries are made by hand."""
    cluster = started_cluster_iceberg_with_spark
    env = make_env(cluster, storage_type, "test_orphan_azure_dirs")
    env.populate(2)

    table_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{env.table_name}"

    def blob(path):
        return cluster.blob_service_client.get_blob_client(cluster.azure_container_name, path)

    directory_entries = [f"{table_path}/data", f"{table_path}/metadata"]
    for path in directory_entries:
        blob(path).upload_blob(b"", overwrite=True, metadata={"hdi_isfolder": "true"})
    env.add_orphan("data", "orphan-azure-dirs.parquet")
    blob(f"{table_path}/data/empty-orphan.parquet").upload_blob(b"", overwrite=True)
    time.sleep(2)

    counts = env.remove_orphans(older_than=env.now_ts(), dry_run=1)
    assert counts["deleted_data_files_count"] == 2, counts

    counts = env.remove_orphans(older_than=env.now_ts())
    assert counts["deleted_data_files_count"] == 2, counts
    assert counts["failed_deletions_count"] == 0, counts
    assert all(blob(path).exists() for path in directory_entries)
    assert not env.exists("data", "orphan-azure-dirs.parquet")
    assert not env.exists("data", "empty-orphan.parquet")
    env.assert_data_intact()
