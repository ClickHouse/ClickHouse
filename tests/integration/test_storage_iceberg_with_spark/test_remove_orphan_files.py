import base64
import io
import json
import re
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

    def write_version_hint(self, value):
        """Overwrite metadata/version-hint.text with `value`.

        A hint naming an older version than the newest committed metadata file is the
        steady state left behind whenever the hint advance does not happen: the PUT of
        version-hint.text failed (its failure is swallowed, the INSERT is still acked),
        or another engine committed without touching ClickHouse's hint."""
        payload = str(value)
        if self.storage_type == "local":
            path = f"{LOCAL_TABLE_PREFIX}/{self.table_name}/metadata/version-hint.text"
            self.instance.exec_in_container(
                ["bash", "-c", f"printf '%s' '{payload}' > {path}"]
            )
        elif self.storage_type == "azure":
            blob_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{self.table_name}/metadata/version-hint.text"
            self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, blob_path,
            ).upload_blob(payload.encode(), overwrite=True)
        else:
            key = f"{S3_TABLE_PREFIX}/{self.table_name}/metadata/version-hint.text"
            data = payload.encode()
            self.cluster.minio_client.put_object(
                self.cluster.minio_bucket, key, io.BytesIO(data), len(data),
            )

    def read_version_hint(self):
        if self.storage_type == "local":
            path = f"{LOCAL_TABLE_PREFIX}/{self.table_name}/metadata/version-hint.text"
            return self.instance.exec_in_container(["bash", "-c", f"cat {path}"]).strip()
        elif self.storage_type == "azure":
            blob_path = f"/var/lib/clickhouse/user_files/iceberg_data/default/{self.table_name}/metadata/version-hint.text"
            return self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, blob_path,
            ).download_blob().readall().decode().strip()
        else:
            key = f"{S3_TABLE_PREFIX}/{self.table_name}/metadata/version-hint.text"
            return self.cluster.minio_client.get_object(
                self.cluster.minio_bucket, key,
            ).read().decode().strip()

    def newest_metadata_version(self):
        """Highest N among the vN.metadata.json objects present in storage."""
        versions = []
        for path in self.list_files():
            name = path.rsplit("/", 1)[-1]
            match = re.fullmatch(r"v(\d+)\.metadata\.json", name)
            if match:
                versions.append(int(match.group(1)))
        assert versions, "No vN.metadata.json objects found in storage"
        return max(versions)

    def metadata_files_with_version(self, version):
        """Metadata object names that the server parses as `version`.

        Mirrors the three accepted shapes: vN.metadata.json, vN-<uuid>.metadata.json
        and N-<uuid>.metadata.json."""
        names = []
        for path in self.list_files():
            name = path.rsplit("/", 1)[-1]
            if not name.endswith(".metadata.json"):
                continue
            match = re.match(r"v?(\d+)[.-]", name)
            if match and int(match.group(1)) == version:
                names.append(name)
        return sorted(names)

    def copy_metadata_file(self, src_name, dst_name):
        """Byte-copy an existing metadata object to a second name."""
        if self.storage_type == "local":
            metadata_dir = f"{LOCAL_TABLE_PREFIX}/{self.table_name}/metadata"
            self.instance.exec_in_container(
                ["bash", "-c", f"cp {metadata_dir}/{src_name} {metadata_dir}/{dst_name}"]
            )
        elif self.storage_type == "azure":
            base = f"/var/lib/clickhouse/user_files/iceberg_data/default/{self.table_name}/metadata"
            payload = self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, f"{base}/{src_name}",
            ).download_blob().readall()
            self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, f"{base}/{dst_name}",
            ).upload_blob(payload, overwrite=True)
        else:
            base = f"{S3_TABLE_PREFIX}/{self.table_name}/metadata"
            payload = self.cluster.minio_client.get_object(
                self.cluster.minio_bucket, f"{base}/{src_name}",
            ).read()
            self.cluster.minio_client.put_object(
                self.cluster.minio_bucket, f"{base}/{dst_name}",
                io.BytesIO(payload), len(payload),
            )

    def read_metadata_file(self, name):
        if self.storage_type == "local":
            path = f"{LOCAL_TABLE_PREFIX}/{self.table_name}/metadata/{name}"
            return self.instance.exec_in_container(["bash", "-c", f"cat {path}"])
        elif self.storage_type == "azure":
            blob_path = (
                f"/var/lib/clickhouse/user_files/iceberg_data/default/"
                f"{self.table_name}/metadata/{name}"
            )
            return self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, blob_path,
            ).download_blob().readall().decode()
        else:
            return self.cluster.minio_client.get_object(
                self.cluster.minio_bucket,
                f"{S3_TABLE_PREFIX}/{self.table_name}/metadata/{name}",
            ).read().decode()

    def write_metadata_file(self, name, payload):
        """Overwrite a metadata object out of band.

        The parsed JSON is cached per (table uuid, path) with no invalidation, so a rewrite
        of an existing name stays invisible until the cache is dropped."""
        data = payload.encode()
        if self.storage_type == "local":
            path = f"{LOCAL_TABLE_PREFIX}/{self.table_name}/metadata/{name}"
            # base64 via argv: the JSON carries quotes and braces that no amount of shell
            # quoting survives intact, and exec_in_container has no stdin channel.
            encoded = base64.b64encode(data).decode()
            self.instance.exec_in_container(
                ["bash", "-c", f"printf '%s' '{encoded}' | base64 -d > {path}"]
            )
        elif self.storage_type == "azure":
            blob_path = (
                f"/var/lib/clickhouse/user_files/iceberg_data/default/"
                f"{self.table_name}/metadata/{name}"
            )
            self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, blob_path,
            ).upload_blob(data, overwrite=True)
        else:
            self.cluster.minio_client.put_object(
                self.cluster.minio_bucket,
                f"{S3_TABLE_PREFIX}/{self.table_name}/metadata/{name}",
                io.BytesIO(data), len(data),
            )
        self.instance.query("SYSTEM DROP ICEBERG METADATA CACHE")

    def remove_metadata_file(self, name):
        if self.storage_type == "local":
            path = f"{LOCAL_TABLE_PREFIX}/{self.table_name}/metadata/{name}"
            self.instance.exec_in_container(["bash", "-c", f"rm {path}"])
        elif self.storage_type == "azure":
            blob_path = (
                f"/var/lib/clickhouse/user_files/iceberg_data/default/"
                f"{self.table_name}/metadata/{name}"
            )
            self.cluster.blob_service_client.get_blob_client(
                self.cluster.azure_container_name, blob_path,
            ).delete_blob()
        else:
            self.cluster.minio_client.remove_object(
                self.cluster.minio_bucket,
                f"{S3_TABLE_PREFIX}/{self.table_name}/metadata/{name}",
            )

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
    instance.query(
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


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_preserves_version_hint(started_cluster_iceberg_with_spark, storage_type):
    """remove_orphan_files must treat metadata/version-hint.text as reachable.

    The hint is a fixed object under the storage root, not a path stored in
    metadata, so the reachable set must contain its storage key.  If that key is
    malformed the live hint is classified as an orphan and deleted, and with
    iceberg_use_version_hint = 1 the table then stops reading entirely."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_version_hint")
    env.populate(2, use_version_hint=True)

    # Guard against a fixture that never wrote the hint: without this the test
    # would pass on a binary that deletes it.
    assert env.exists("metadata", "version-hint.text"), \
        "Fixture did not create metadata/version-hint.text"

    env.add_orphan("data", "orphan-version-hint.parquet")
    time.sleep(2)

    env.remove_orphans(older_than=env.now_ts())

    assert env.exists("metadata", "version-hint.text"), \
        "remove_orphan_files deleted the live metadata/version-hint.text"
    # The user-visible consequence: with the hint gone the table stops reading.
    env.assert_data_intact()
    assert not env.exists("data", "orphan-version-hint.parquet"), \
        "The orphan data file should still have been deleted"


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_remove_orphan_files_stale_version_hint_keeps_committed_snapshot(
    started_cluster_iceberg_with_spark, storage_type
):
    """A version hint naming an older version must not make a committed snapshot orphan.

    remove_orphan_files roots reachability at the authoritative current metadata. When the
    hint lags -- its PUT failed, or another engine committed without touching it -- the
    newest committed snapshot is still live and every one of its objects must survive."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_stale_hint")
    env.populate(3, use_version_hint=True)

    newest = env.newest_metadata_version()
    assert newest >= 2, f"Need at least two metadata versions to stale the hint, got v{newest}"
    assert env.exists("metadata", "version-hint.text"), \
        "Fixture did not create metadata/version-hint.text"

    # A planted orphan keeps the case honest: a binary that simply stops deleting passes
    # the survival assertions below but fails this one.
    env.add_orphan("data", "orphan-stale-hint.parquet")

    env.write_version_hint(newest - 1)
    # Guard: without an effective rewrite that differs from the newest version present,
    # "stale hint" is a fiction and the case would pass on the unfixed binary.
    assert env.read_version_hint() == str(newest - 1), \
        "version-hint.text rewrite did not take effect"

    data_before = [f for f in env.list_files()
                   if "/data/" in f and f.endswith(".parquet")
                   and "orphan-stale-hint" not in f]
    assert data_before, "Fixture produced no committed data files"

    time.sleep(2)
    # older_than must be pinned to now: with the default 3-day window the age gate spares
    # every fresh object and the case passes without the fix.
    env.remove_orphans(older_than=env.now_ts())

    data_after = [f for f in env.list_files()
                  if "/data/" in f and f.endswith(".parquet")
                  and "orphan-stale-hint" not in f]
    assert sorted(data_after) == sorted(data_before), (
        "remove_orphan_files deleted data files of the committed snapshot that the stale "
        f"version hint does not name.\n  Before: {sorted(data_before)}\n  After:  {sorted(data_after)}"
    )
    assert env.exists("metadata", f"v{newest}.metadata.json"), (
        f"remove_orphan_files deleted v{newest}.metadata.json, the committed metadata file "
        "that the stale version hint does not name"
    )
    assert not env.exists("data", "orphan-stale-hint.parquet"), \
        "The planted orphan should still have been deleted"

    # Restore the hint so the read path resolves the committed version, then assert the
    # user-visible consequence directly: no rows may be missing.
    env.write_version_hint(newest)
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_stale_version_hint_dry_run_reports_no_orphans(
    started_cluster_iceberg_with_spark, storage_type
):
    """dry_run under a stale hint must not report the committed snapshot as orphan.

    dry_run returns before the metadata-version recheck, so it is the arm that shows the
    reachability root itself is authoritative rather than the recheck compensating. It
    deletes nothing, so the reported counts depend on the root alone and not on any
    backend's removal path; one backend therefore covers it."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_stale_hint_dry")
    env.populate(3, use_version_hint=True)
    env.add_orphan("data", "orphan-stale-hint-dry.parquet")

    newest = env.newest_metadata_version()
    env.write_version_hint(newest - 1)
    assert env.read_version_hint() == str(newest - 1), \
        "version-hint.text rewrite did not take effect"

    time.sleep(2)
    counts = env.remove_orphans(older_than=env.now_ts(), dry_run=1)

    assert counts["deleted_data_files_count"] == 1, (
        "dry_run must report exactly the planted orphan-stale-hint-dry.parquet, so a "
        "classification that returned nothing at all cannot pass this arm: "
        f"{counts}"
    )
    assert counts["deleted_manifest_files_count"] == 0, f"got {counts}"
    assert counts["deleted_manifest_lists_count"] == 0, f"got {counts}"
    assert env.exists("data", "orphan-stale-hint-dry.parquet"), \
        "dry_run deleted the file it only reported"
    env.write_version_hint(newest)
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local", "s3"])
def test_insert_under_stale_version_hint_succeeds(
    started_cluster_iceberg_with_spark, storage_type
):
    """A write must advance past the newest committed metadata, not past the hint.

    A writer that resolves its next version through a lagging hint keeps aiming at a
    version already present in storage, so its conditional commit is refused every time
    and the retry budget runs out. The write path therefore resolves the authoritative
    current version, the same root the cleanup path uses."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_stale_hint_insert")
    env.populate(3, use_version_hint=True)

    newest = env.newest_metadata_version()
    assert newest >= 2, f"Need at least two metadata versions to stale the hint, got v{newest}"

    env.write_version_hint(newest - 1)
    # Guard: without an effective rewrite that differs from the newest version present,
    # "stale hint" is a fiction and the case would pass on the unfixed binary.
    assert env.read_version_hint() == str(newest - 1), \
        "version-hint.text rewrite did not take effect"

    # Pre-fix this exhausts the writer's retry budget and raises DATALAKE_DATABASE_ERROR
    # ("Write into iceberg was not successful"); each attempt regenerates the same target
    # version, which already exists.
    env.instance.query(
        f"INSERT INTO {env.table_name} VALUES (4);", settings=ICEBERG_SETTINGS
    )
    env._n_rows = 4

    # Not throwing is not enough: the commit has to have produced a newer metadata file.
    assert env.newest_metadata_version() > newest, (
        f"INSERT reported success but no metadata file newer than v{newest} exists, so "
        "nothing was committed"
    )

    # The hint is advanced by the successful write, so the read path already resolves the
    # new version; assert the user-visible consequence directly.
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_refuses_ambiguous_metadata_version(
    started_cluster_iceberg_with_spark, storage_type
):
    """Two metadata files the resolver ranks equal must make the cleanup refuse, not guess.

    Without a catalog the cleanup root comes from a listing, and max_element returns the
    first of equal elements -- so listing order, which has no relation to which state is
    live, picks the root. An external engine committing NNNNN-<uuid>.metadata.json from the
    same base as ClickHouse's vN.metadata.json is enough to produce the tie. Deleting on a
    guess is unrecoverable, so refuse and name the files. The verdict comes from the
    resolver itself, so it is backend-independent; one backend covers it."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_ambiguous")
    env.populate(3, use_version_hint=True)

    newest = env.newest_metadata_version()
    twin_name = f"{newest:05d}-{get_uuid_str()}.metadata.json"
    env.copy_metadata_file(f"v{newest}.metadata.json", twin_name)

    # Without an actual tie at the chosen root the case would pass on the unfixed binary.
    same_version = env.metadata_files_with_version(newest)
    assert same_version == sorted([f"v{newest}.metadata.json", twin_name]), (
        f"Fixture did not produce exactly two files parsing to v{newest}: {same_version}"
    )

    env.add_orphan("data", "orphan-ambiguous.parquet")
    committed_before = sorted(env.list_files())

    time.sleep(2)
    for extra in ("", ", dry_run = 1"):
        error = env.instance.query_and_get_error(
            f"ALTER TABLE {env.table_name} EXECUTE remove_orphan_files("
            f"older_than = '{env.now_ts()}'{extra});",
            settings=ICEBERG_SETTINGS,
        )
        assert "BAD_ARGUMENTS" in error, f"dry_run='{extra}' should refuse, got: {error}"
        assert "ranks equal to 1 other metadata file(s)" in error, (
            f"Refusal should name the ambiguity, got: {error}"
        )
        assert twin_name in error, f"Refusal should name the offending files, got: {error}"

    # The planted orphan surviving is what shows the refusal happened before any deletion
    # rather than after part of it.
    assert sorted(env.list_files()) == committed_before, (
        "A refused remove_orphan_files must not have deleted anything.\n"
        f"  Before: {committed_before}\n  After:  {sorted(env.list_files())}"
    )
    env.assert_data_intact()

    # Positive control: with the ambiguity resolved the command runs and does its job, so
    # the case cannot pass by the command being broken outright.
    env.remove_metadata_file(twin_name)
    counts = env.remove_orphans(older_than=env.now_ts())
    assert counts["deleted_data_files_count"] == 1, (
        f"After removing the twin the planted orphan should be deleted: {counts}"
    )
    assert not env.exists("data", "orphan-ambiguous.parquet")
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_refuses_last_updated_ms_tie(
    started_cluster_iceberg_with_spark, storage_type
):
    """A tie under `iceberg_recent_metadata_file_by_last_updated_ms_field` must also refuse.

    That policy ranks candidates by the last-updated-ms field rather than by the version
    number, so two files with DIFFERENT versions and one timestamp are the ambiguous pair
    and a version-keyed check sees nothing wrong at all. A copy of the newest file under the
    next version number is such a pair: the timestamp it carries is equal by construction."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_ms_tie")
    env.populate(
        3,
        additional_settings=["iceberg_recent_metadata_file_by_last_updated_ms_field = true"],
    )

    newest = env.newest_metadata_version()
    newest_name = f"v{newest}.metadata.json"
    twin_name = f"v{newest + 1}.metadata.json"
    env.copy_metadata_file(newest_name, twin_name)

    # The two versions differ, so a version-keyed check finds no ambiguity here at all.
    assert env.metadata_files_with_version(newest) == [newest_name]
    assert env.metadata_files_with_version(newest + 1) == [twin_name]

    env.add_orphan("data", "orphan-ms-tie.parquet")
    committed_before = sorted(env.list_files())

    time.sleep(2)
    error = env.instance.query_and_get_error(
        f"ALTER TABLE {env.table_name} EXECUTE remove_orphan_files("
        f"older_than = '{env.now_ts()}');",
        settings=ICEBERG_SETTINGS,
    )
    assert "BAD_ARGUMENTS" in error, f"A last-updated-ms tie should refuse, got: {error}"
    assert "ranks equal to 1 other metadata file(s)" in error, error
    assert twin_name in error or newest_name in error, (
        f"Refusal should name the tied file, got: {error}"
    )

    assert sorted(env.list_files()) == committed_before, (
        "A refused remove_orphan_files must not have deleted anything.\n"
        f"  Before: {committed_before}\n  After:  {sorted(env.list_files())}"
    )
    env.assert_data_intact()


@pytest.mark.parametrize("storage_type", ["local"])
def test_remove_orphan_files_allows_shared_directory_with_table_uuid(
    started_cluster_iceberg_with_spark, storage_type
):
    """A same-version file belonging to ANOTHER table must not block the cleanup.

    With `iceberg_metadata_table_uuid` the resolver keeps only the candidates carrying that
    uuid, so a neighbour table sharing the metadata directory is never a candidate and
    nothing about it is ambiguous. Refusing on it would break a supported layout, which is
    what a check keyed on the version number alone does."""
    env = make_env(started_cluster_iceberg_with_spark, storage_type, "test_orphan_shared_dir")
    env.populate(3)

    newest = env.newest_metadata_version()
    newest_name = f"v{newest}.metadata.json"
    our_uuid = json.loads(env.read_metadata_file(newest_name))["table-uuid"]

    # A neighbour table's metadata: same parsed version, different table-uuid.
    neighbour = json.loads(env.read_metadata_file(newest_name))
    neighbour["table-uuid"] = get_uuid_str()
    neighbour_name = f"{newest:05d}-{get_uuid_str()}.metadata.json"
    env.write_metadata_file(neighbour_name, json.dumps(neighbour))

    # Fixture check: the two DO collide on version, so a version-keyed guard would refuse.
    assert env.metadata_files_with_version(newest) == sorted([newest_name, neighbour_name])

    # Re-attach to the same data with the uuid pinned. `if_not_exists` is what makes this an
    # attach rather than a create: a create refuses a path that already holds metadata.
    env.instance.query(f"DROP TABLE {env.table_name};")
    create_iceberg_table(
        env.storage_type, env.instance, env.table_name, env.cluster, "(x Int)", 2,
        if_not_exists=True,
        additional_settings=[f"iceberg_metadata_table_uuid = '{our_uuid}'"],
    )

    env.add_orphan("data", "orphan-shared-dir.parquet")

    time.sleep(2)
    counts = env.remove_orphans(older_than=env.now_ts())
    assert counts["deleted_data_files_count"] == 1, (
        f"The planted orphan should have been deleted, not refused: {counts}"
    )
    assert not env.exists("data", "orphan-shared-dir.parquet")
    env.assert_data_intact()
