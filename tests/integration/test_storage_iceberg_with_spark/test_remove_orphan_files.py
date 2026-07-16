import io
import json
import time

import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    drop_iceberg_table,
    get_uuid_str,
)


ICEBERG_SETTINGS = {
    "allow_insert_into_iceberg": 1,
    "allow_iceberg_remove_orphan_files": 1,
}

LOCAL_TABLE_PREFIX = "/var/lib/clickhouse/user_files/iceberg_data/default"
S3_TABLE_PREFIX = "var/lib/clickhouse/user_files/iceberg_data/default"


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

    def populate(self, n_rows, format_version=2):
        create_iceberg_table(
            self.storage_type, self.instance, self.table_name,
            self.cluster, "(x Int)", format_version,
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

    # -- orphan file manipulation -------------------------------------------

    def add_orphan(self, subdir="data", filename="orphan.parquet"):
        if self.storage_type == "local":
            table_dir = f"{LOCAL_TABLE_PREFIX}/{self.table_name}"
            target_dir = f"{table_dir}/{subdir}"
            self.instance.exec_in_container(
                ["bash", "-c",
                 f"mkdir -p {target_dir} && echo 'orphan_data' > {target_dir}/{filename}"]
            )
        elif self.storage_type == "azure":
            blob_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{self.table_name}/{subdir}/{filename}"
            blob_client = self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, blob_path,
            )
            blob_client.upload_blob(b"orphan_data", overwrite=True)
        else:
            key = f"{S3_TABLE_PREFIX}/{self.table_name}/{subdir}/{filename}"
            data = b"orphan_data"
            self.cluster.minio_client.put_object(
                self.cluster.minio_bucket, key, io.BytesIO(data), len(data),
            )

    def add_orphan_metadata(self, filename="v0.metadata.json"):
        self.add_orphan(subdir="metadata", filename=filename)

    # -- storage queries ----------------------------------------------------

    def exists(self, subdir, filename):
        if self.storage_type == "local":
            path = f"{LOCAL_TABLE_PREFIX}/{self.table_name}/{subdir}/{filename}"
            ret = self.instance.exec_in_container(
                ["bash", "-c", f"test -f {path} && echo 'exists' || echo 'missing'"]
            ).strip()
            return ret == "exists"
        elif self.storage_type == "azure":
            blob_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{self.table_name}/{subdir}/{filename}"
            blob_client = self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, blob_path,
            )
            try:
                blob_client.get_blob_properties()
                return True
            except Exception:
                return False
        else:
            key = f"{S3_TABLE_PREFIX}/{self.table_name}/{subdir}/{filename}"
            try:
                self.cluster.minio_client.stat_object(self.cluster.minio_bucket, key)
                return True
            except Exception:
                return False

    def list_files(self):
        if self.storage_type == "local":
            table_dir = f"{LOCAL_TABLE_PREFIX}/{self.table_name}"
            output = self.instance.exec_in_container(
                ["bash", "-c", f"find {table_dir} -type f 2>/dev/null | sort"]
            ).strip()
            return output.split("\n") if output else []
        elif self.storage_type == "azure":
            prefix = f"/var/lib/clickhouse/user_files/iceberg_data/default/{self.table_name}/"
            container_client = self.cluster.blob_service_client.get_container_client(
                self.cluster.azure_container_name,
            )
            return sorted(b.name for b in container_client.list_blobs(name_starts_with=prefix))
        else:
            prefix = f"{S3_TABLE_PREFIX}/{self.table_name}/"
            return sorted(
                obj.object_name
                for obj in self.cluster.minio_client.list_objects(
                    self.cluster.minio_bucket, prefix=prefix, recursive=True,
                )
            )

    # -- command execution --------------------------------------------------

    def remove_orphans(self, **kwargs):
        args_parts = []
        if "older_than" in kwargs:
            args_parts.append(f"older_than = '{kwargs['older_than']}'")
        if "location" in kwargs:
            args_parts.append(f"location = '{kwargs['location']}'")
        if "dry_run" in kwargs:
            args_parts.append(f"dry_run = {kwargs['dry_run']}")
        if "positional_ts" in kwargs:
            args_str = f"'{kwargs['positional_ts']}'"
            if args_parts:
                args_str += ", " + ", ".join(args_parts)
        else:
            args_str = ", ".join(args_parts)

        raw = self.instance.query(
            f"ALTER TABLE {self.table_name} EXECUTE remove_orphan_files({args_str});",
            settings=ICEBERG_SETTINGS,
        )
        counts = {}
        for line in raw.strip().split("\n"):
            if not line:
                continue
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

@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_basic(started_cluster_iceberg_with_spark, storage_type):
    """Create orphan files, run remove_orphan_files, verify they are removed
    and legitimate files are preserved."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_basic")
    env.populate(3)

    env.add_orphan("data", "orphan-data-001.parquet")
    env.add_orphan("data", "orphan-data-002.parquet")
    time.sleep(2)

    files_before = env.list_files()
    assert any("orphan-data-001.parquet" in f for f in files_before)
    assert any("orphan-data-002.parquet" in f for f in files_before)

    counts = env.remove_orphans(older_than=env.now_ts())
    assert len(counts) == 9, f"Expected 9 metrics, got {counts}"
    assert counts["deleted_data_files_count"] >= 2, f"Expected >= 2 deleted data files, got {counts}"

    assert not env.exists("data", "orphan-data-001.parquet")
    assert not env.exists("data", "orphan-data-002.parquet")
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_no_orphans(started_cluster_iceberg_with_spark, storage_type):
    """When there are no user-created orphan files, data/manifest/statistics counts should be zero.
    Metadata files (old v*.metadata.json) may legitimately be orphaned after multiple inserts."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_no_orphans")
    env.populate(2)
    time.sleep(2)

    counts = env.remove_orphans(older_than=env.now_ts())
    non_metadata = {k: v for k, v in counts.items() if k not in ("deleted_metadata_files_count", "skipped_missing_metadata_count")}
    assert all(v == 0 for v in non_metadata.values()), f"Expected data/manifest/stat zeros, got {counts}"
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_default_older_than(started_cluster_iceberg_with_spark, storage_type):
    """Zero-argument form: older_than defaults to now - iceberg_orphan_files_older_than_seconds."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_default")
    env.populate(1)

    env.add_orphan("data", "orphan-default.parquet")
    time.sleep(2)

    settings_with_short_threshold = {
        **ICEBERG_SETTINGS,
        "iceberg_orphan_files_older_than_seconds": 1,
    }

    raw = env.instance.query(
        f"ALTER TABLE {env.table_name} EXECUTE remove_orphan_files();",
        settings=settings_with_short_threshold,
    )
    counts = {}
    for line in raw.strip().split("\n"):
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) == 2:
            counts[parts[0]] = int(parts[1])

    assert counts["deleted_data_files_count"] >= 1, \
        f"Zero-arg form with 1s threshold should delete orphan, got {counts}"
    assert not env.exists("data", "orphan-default.parquet"), \
        "Orphan file should be deleted via setting-driven default older_than"
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_older_than(started_cluster_iceberg_with_spark, storage_type):
    """Orphan files newer than older_than threshold should be preserved."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_older_than")
    env.populate(1)

    past_ts = env.now_ts()
    time.sleep(2)

    env.add_orphan("data", "orphan-new.parquet")
    time.sleep(1)

    env.remove_orphans(older_than=past_ts)
    assert env.exists("data", "orphan-new.parquet"), \
        "Orphan newer than older_than should NOT be deleted"

    time.sleep(1)
    counts = env.remove_orphans(older_than=env.now_ts())
    assert counts["deleted_data_files_count"] >= 1
    assert not env.exists("data", "orphan-new.parquet"), \
        "Orphan older than threshold should be deleted"

    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_dry_run(started_cluster_iceberg_with_spark, storage_type):
    """dry_run=1 should report counts but not delete files."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_dry_run")
    env.populate(1)

    env.add_orphan("data", "orphan-dry.parquet")
    time.sleep(2)

    counts = env.remove_orphans(older_than=env.now_ts(), dry_run=1)
    assert counts["deleted_data_files_count"] >= 1
    assert env.exists("data", "orphan-dry.parquet"), "dry_run should NOT delete files"

    counts = env.remove_orphans(older_than=env.now_ts(), dry_run=0)
    assert counts["deleted_data_files_count"] >= 1
    assert not env.exists("data", "orphan-dry.parquet"), "Without dry_run, orphan should be deleted"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_location(started_cluster_iceberg_with_spark, storage_type):
    """location parameter should restrict the scan to a subdirectory."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_location")
    env.populate(1)

    env.add_orphan("data", "orphan-data.parquet")
    env.add_orphan_metadata("v0.metadata.json")
    time.sleep(2)

    now_ts = env.now_ts()

    counts = env.remove_orphans(older_than=now_ts, location="data/")
    assert counts["deleted_data_files_count"] >= 1
    assert not env.exists("data", "orphan-data.parquet"), \
        "Data orphan in scanned location should be deleted"
    assert env.exists("metadata", "v0.metadata.json"), \
        "Metadata orphan outside scanned location should survive"

    counts = env.remove_orphans(older_than=now_ts, location="metadata/")
    assert counts["deleted_metadata_files_count"] >= 1
    assert not env.exists("metadata", "v0.metadata.json"), \
        "Metadata orphan in scanned location should be deleted"

    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_many_orphans(started_cluster_iceberg_with_spark, storage_type):
    """remove_orphan_files should delete multiple orphan files in one run."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_many")
    env.populate(1)

    for i in range(10):
        env.add_orphan("data", f"orphan-par-{i:03d}.parquet")
    time.sleep(2)

    counts = env.remove_orphans(older_than=env.now_ts())
    assert counts["deleted_data_files_count"] >= 10

    for i in range(10):
        assert not env.exists("data", f"orphan-par-{i:03d}.parquet")

    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_multiple_snapshots(started_cluster_iceberg_with_spark, storage_type):
    """Files referenced by any snapshot are not considered orphans."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_multi_snap")
    env.populate(3)

    env.add_orphan("data", "orphan-multi.parquet")
    time.sleep(2)

    counts = env.remove_orphans(older_than=env.now_ts())
    assert counts["deleted_data_files_count"] >= 1
    assert not env.exists("data", "orphan-multi.parquet")
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_positional_arg(started_cluster_iceberg_with_spark, storage_type):
    """Positional older_than argument should work the same as named."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_positional")
    env.populate(1)

    env.add_orphan("data", "orphan-pos.parquet")
    time.sleep(2)

    counts = env.remove_orphans(positional_ts=env.now_ts())
    assert counts["deleted_data_files_count"] >= 1
    assert not env.exists("data", "orphan-pos.parquet")
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_gate_setting(started_cluster_iceberg_with_spark, storage_type):
    """Without allow_iceberg_remove_orphan_files, the command should fail."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_gate")
    env.populate(1)

    error = env.instance.query_and_get_error(
        f"ALTER TABLE {env.table_name} EXECUTE remove_orphan_files();",
        settings={"allow_insert_into_iceberg": 1, "allow_iceberg_remove_orphan_files": 0},
    )
    assert "SUPPORT_IS_DISABLED" in error, f"Expected SUPPORT_IS_DISABLED error, got: {error}"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_future_timestamp_rejected(started_cluster_iceberg_with_spark, storage_type):
    """Passing an older_than in the future should be rejected with BAD_ARGUMENTS."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_future_ts")
    env.populate(1)

    error = env.instance.query_and_get_error(
        f"ALTER TABLE {env.table_name} EXECUTE remove_orphan_files('2099-01-01 00:00:00');",
        settings=ICEBERG_SETTINGS,
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS error, got: {error}"


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_location_validation(started_cluster_iceberg_with_spark, storage_type):
    """Path-traversal and absolute location values should be rejected."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_loc_val")
    env.populate(1)

    for bad_loc in ["../escape", "/absolute/path"]:
        error = env.instance.query_and_get_error(
            f"ALTER TABLE {env.table_name} EXECUTE remove_orphan_files("
            f"older_than = '{env.now_ts()}', location = '{bad_loc}');",
            settings=ICEBERG_SETTINGS,
        )
        assert "BAD_ARGUMENTS" in error, f"location='{bad_loc}' should fail, got: {error}"

    env.add_orphan("data", "orphan-dotslash.parquet")
    time.sleep(2)
    counts = env.remove_orphans(older_than=env.now_ts(), location="./data/")
    assert counts["deleted_data_files_count"] >= 1, \
        "location='./data/' should work the same as 'data/'"


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_rejected_on_v1(started_cluster_iceberg_with_spark, storage_type):
    """remove_orphan_files must reject Iceberg format-version 1 tables."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_v1")
    env.populate(1, format_version=1)

    error = env.instance.query_and_get_error(
        f"ALTER TABLE {env.table_name} EXECUTE remove_orphan_files();",
        settings=ICEBERG_SETTINGS,
    )
    assert "BAD_ARGUMENTS" in error, f"Expected BAD_ARGUMENTS error, got: {error}"
    assert "format version" in error.lower(), f"Error should mention format version, got: {error}"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_delete_file_categories(started_cluster_iceberg_with_spark, storage_type):
    """Equality-delete and position-delete orphan files must be tallied
    under their respective metrics, not misclassified."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_del_cat")
    env.populate(1)

    env.add_orphan("data", "eq-delete-001.parquet")
    env.add_orphan("data", "00000-0-eq-del-00001.parquet")
    env.add_orphan("data", "00000-0-deletes.parquet")
    env.add_orphan("data", "00000-0-delete-00001.parquet")
    time.sleep(2)

    counts = env.remove_orphans(older_than=env.now_ts())

    assert counts["deleted_equality_delete_files_count"] == 2, \
        f"Expected 2 equality-delete files, got {counts}"
    assert counts["deleted_position_delete_files_count"] == 2, \
        f"Expected 2 position-delete files, got {counts}"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_location_scoped_deletion(started_cluster_iceberg_with_spark, storage_type):
    """Files outside the specified location folder must NOT be deleted."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_loc_scope")
    env.populate(1)

    env.add_orphan("data", "orphan-in-data.parquet")
    env.add_orphan("data/subdir", "orphan-nested.parquet")
    env.add_orphan_metadata("v0.metadata.json")
    time.sleep(2)

    now_ts = env.now_ts()
    counts = env.remove_orphans(older_than=now_ts, location="data/")

    assert counts["deleted_data_files_count"] >= 1, \
        f"Orphan in data/ should be deleted, got {counts}"
    assert not env.exists("data", "orphan-in-data.parquet"), \
        "Orphan inside scanned location should be deleted"

    assert env.exists("metadata", "v0.metadata.json"), \
        "Orphan in metadata/ must survive when scanning data/"

    counts2 = env.remove_orphans(older_than=now_ts, location="metadata/")
    assert counts2["deleted_metadata_files_count"] >= 1
    assert not env.exists("metadata", "v0.metadata.json"), \
        "Metadata orphan should be deleted when scanning metadata/"

    env.assert_data_intact()


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


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_ignores_pinned_metadata(started_cluster_iceberg_with_spark, storage_type):
    """When iceberg_metadata_file_path pins the table to an older metadata
    version, remove_orphan_files must still use the *latest* metadata to
    determine reachable files.  Otherwise it would treat data belonging to
    newer (valid) snapshots as orphans and delete them."""

    cluster = started_cluster_iceberg_with_spark
    instance = cluster.instances["node1"]
    table_name = f"test_orphan_pinned_{storage_type}_{get_uuid_str()}"
    metadata_dir = f"{LOCAL_TABLE_PREFIX}/{table_name}/metadata"

    insert_settings = {"allow_insert_into_iceberg": 1}

    create_iceberg_table(
        storage_type, instance, table_name, cluster, "(x Int)", format_version=2,
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (1);",
        settings=insert_settings,
    )
    instance.query(
        f"INSERT INTO {table_name} VALUES (2);",
        settings=insert_settings,
    )

    metadata_files_before = instance.exec_in_container(
        ["bash", "-c",
         f"ls -v {metadata_dir}/v*.metadata.json"]
    ).strip().split("\n")
    old_metadata_path = "metadata/" + metadata_files_before[-1].split("/")[-1]

    instance.query(
        f"INSERT INTO {table_name} VALUES (3);",
        settings=insert_settings,
    )

    data_files_before = instance.exec_in_container(
        ["bash", "-c",
         f"find {LOCAL_TABLE_PREFIX}/{table_name}/data -type f 2>/dev/null | sort"]
    ).strip().split("\n")
    assert len(data_files_before) >= 3, (
        f"Expected at least 3 data files (3 inserts), got {data_files_before}"
    )

    drop_iceberg_table(instance, table_name)

    create_iceberg_table(
        storage_type, instance, table_name, cluster, "(x Int)",
        format_version=2,
        if_not_exists=True,
        explicit_metadata_path=old_metadata_path,
    )

    pinned_result = instance.query(f"SELECT count() FROM {table_name}")
    assert pinned_result.strip() == "2", (
        f"Pinned table should see 2 rows (first two inserts), got {pinned_result.strip()}"
    )

    time.sleep(2)
    raw = instance.query(
        f"ALTER TABLE {table_name} EXECUTE remove_orphan_files('{OrphanTestEnv.now_ts()}');",
        settings=ICEBERG_SETTINGS,
    )

    data_files_after = instance.exec_in_container(
        ["bash", "-c",
         f"find {LOCAL_TABLE_PREFIX}/{table_name}/data -type f 2>/dev/null | sort"]
    ).strip().split("\n")
    assert data_files_after == data_files_before, (
        f"remove_orphan_files must not delete data from newer snapshots.\n"
        f"  Before: {data_files_before}\n"
        f"  After:  {data_files_after}"
    )

    drop_iceberg_table(instance, table_name)
    create_iceberg_table(
        storage_type, instance, table_name, cluster, "(x Int)",
        format_version=2,
        if_not_exists=True,
    )
    full_result = instance.query(
        f"SELECT * FROM {table_name} ORDER BY x"
    )
    assert full_result == "1\n2\n3\n", (
        f"All data should be intact when reading latest metadata, got: {full_result}"
    )
