"""Managers for Paimon e2e tests against real S3 and Azure.

Tables are written by Spark with the Paimon catalog into a local temporary
warehouse and uploaded file-by-file to the real cloud — Paimon table
directories are relocatable (metadata stores relative paths), the same
property the MinIO/Azurite suite (test_storage_paimon_with_spark) and the
checked-in stateless fixtures rely on. This mirrors the local-then-upload
strategy of IcebergAzureCatalogManager.

Credentials come from the same env vars as the other data-lake e2e suites
(E2E_AWS_* and E2E_ICEBERG_AZURE_* — the Azure storage account is shared
across all data-lake e2e tests, hence the ICEBERG prefix). No new secrets.
"""

import logging
import os
import shutil
import tempfile
import uuid
from abc import ABC, abstractmethod

import pyspark

from helpers.paimon_utils import (
    compact_paimon_table,
    create_spark_paimon_table,
    upload_paimon_dir,
)

logger = logging.getLogger(__name__)


class PaimonE2ECredentialsMissing(Exception):
    """Required E2E_* environment variables are absent (skippable); any other
    from_env failure is a real setup regression and must fail loudly."""


class PaimonManagerBase(ABC):
    backend = None  # "s3" or "azure"

    def __init__(self):
        self._run_id = uuid.uuid4().hex[:8]
        self._dir_name = f"paimon_e2e_{self._run_id}"
        # Per-run Spark catalog name: the Spark session is a JVM singleton and
        # may pre-exist with other Paimon catalogs bound; catalogs are
        # instantiated lazily on first use, so a runtime-set unique name is safe.
        self._catalog = f"paimon_e2e_{self._run_id}"
        self._local_warehouse = tempfile.mkdtemp(prefix=f"paimon_e2e_{self._run_id}_")
        self._tables_created = []
        self._spark = None

    def _get_spark(self):
        if self._spark is None:
            session = (
                pyspark.sql.SparkSession.builder.appName(
                    f"paimon_e2e_{self._run_id}"
                )
                .config(
                    "spark.sql.extensions",
                    "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
                )
                .master("local")
                .getOrCreate()
            )
            session.conf.set(
                f"spark.sql.catalog.{self._catalog}",
                "org.apache.paimon.spark.SparkCatalog",
            )
            session.conf.set(
                f"spark.sql.catalog.{self._catalog}.warehouse",
                f"file:{self._local_warehouse}",
            )
            session.conf.set("spark.sql.session.timeZone", "UTC")
            self._spark = session
        return self._spark

    def spark_table(self, table_name):
        return f"{self._catalog}.default.{table_name}"

    def table_remote_prefix(self, table_name):
        """Remote path of the table dir, relative to the backend's root (bucket prefix / container)."""
        return f"{self._dir_name}/default.db/{table_name}/"

    def create_table(
        self,
        columns_sql,
        values_sql_batches,
        table_name=None,
        partitioned_by="",
        tblproperties=None,
    ):
        """Create an append-only Paimon table, run one INSERT per batch (one
        snapshot each), and upload it. Returns the table name."""
        table_name = table_name or f"tbl_{uuid.uuid4().hex[:8]}"
        spark = self._get_spark()
        create_spark_paimon_table(
            spark,
            table_name,
            columns_sql,
            partitioned_by=partitioned_by,
            tblproperties=tblproperties,
            catalog=self._catalog,
        )
        for values_sql in values_sql_batches:
            spark.sql(f"INSERT INTO {self.spark_table(table_name)} {values_sql}")
        self._upload_table(table_name)
        self._tables_created.append(table_name)
        return table_name

    def append(self, table_name, values_sql):
        """One more snapshot commit, then re-upload (metadata/data files are
        immutable, so the full re-upload only overwrites the snapshot hints)."""
        self._get_spark().sql(f"INSERT INTO {self.spark_table(table_name)} {values_sql}")
        self._upload_table(table_name)

    def run_spark_sql(self, table_name, sql):
        """Run one Spark SQL statement (e.g. ALTER TABLE), then re-upload."""
        self._get_spark().sql(sql)
        self._upload_table(table_name)

    def compact(self, table_name):
        compact_paimon_table(self._get_spark(), table_name, catalog=self._catalog)
        self._upload_table(table_name)

    def _upload_table(self, table_name):
        local_path = os.path.join(self._local_warehouse, "default.db", table_name)
        if not os.path.isdir(local_path):
            raise FileNotFoundError(f"Local Paimon table directory not found: {local_path}")
        # Commit order (data -> manifests -> snapshots -> hints), so a
        # concurrent reader never sees a snapshot with missing files.
        remote_prefix = self.table_remote_prefix(table_name)
        upload_paimon_dir(
            local_path,
            lambda local_file, rel_path: self._upload_file(
                local_file, f"{remote_prefix}{rel_path}"
            ),
        )

    def cleanup_table(self, table_name):
        try:
            self._delete_prefix(self.table_remote_prefix(table_name))
        except Exception:
            logger.warning("Failed to clean up Paimon e2e table %s", table_name, exc_info=True)

    def cleanup_all(self):
        for table_name in self._tables_created:
            self.cleanup_table(table_name)
        try:
            self._delete_prefix(f"{self._dir_name}/")
        except Exception:
            logger.warning("Failed to clean up Paimon e2e prefix %s", self._dir_name, exc_info=True)
        shutil.rmtree(self._local_warehouse, ignore_errors=True)
        if self._spark is not None:
            # getOrCreate returns the process-global JVM session: stop it so
            # the next backend's manager builds a fresh session it owns
            # instead of inheriting this one's catalogs, config and threads.
            self._spark.stop()
            self._spark = None

    @abstractmethod
    def _upload_file(self, local_file, remote_rel_path): ...

    @abstractmethod
    def _delete_prefix(self, remote_rel_prefix): ...

    @abstractmethod
    def table_function_sql(self, table_name): ...

    @abstractmethod
    def table_function_cluster_sql(self, cluster_name, table_name): ...

    @abstractmethod
    def create_engine_sql(self, ch_table_name, table_name): ...


class PaimonS3Manager(PaimonManagerBase):
    backend = "s3"

    def __init__(self, region, bucket, prefix, access_key_id, secret_access_key):
        super().__init__()
        import boto3

        self.region = region
        self.bucket = bucket
        self.prefix = prefix
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self._s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
        )

    @classmethod
    def from_env(cls):
        region = os.getenv("E2E_AWS_REGION", "")
        bucket = os.getenv("E2E_AWS_S3_BUCKET", "")
        prefix = os.getenv("E2E_AWS_S3_PREFIX", "clickhouse-e2e-paimon")
        access_key_id = os.getenv("E2E_AWS_ACCESS_KEY_ID", "")
        secret_access_key = os.getenv("E2E_AWS_SECRET_ACCESS_KEY", "")
        required = {
            "E2E_AWS_REGION": region,
            "E2E_AWS_S3_BUCKET": bucket,
            "E2E_AWS_ACCESS_KEY_ID": access_key_id,
            "E2E_AWS_SECRET_ACCESS_KEY": secret_access_key,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise PaimonE2ECredentialsMissing(
                "Missing Paimon S3 e2e settings: " + ", ".join(missing)
            )
        return cls(region, bucket, prefix, access_key_id, secret_access_key)

    def _key(self, remote_rel_path):
        return f"{self.prefix}/{remote_rel_path}"

    def _upload_file(self, local_file, remote_rel_path):
        self._s3.upload_file(local_file, self.bucket, self._key(remote_rel_path))

    def _delete_prefix(self, remote_rel_prefix):
        paginator = self._s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._key(remote_rel_prefix)):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        for i in range(0, len(keys), 1000):
            response = self._s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": key} for key in keys[i : i + 1000]]},
            )
            for error in response.get("Errors", []):
                logger.warning("Failed to delete %s: %s", error.get("Key"), error.get("Message"))

    def s3_url(self, table_name):
        return f"s3://{self.bucket}/{self.prefix}/{self.table_remote_prefix(table_name)}"

    def table_function_sql(self, table_name):
        return f"paimonS3('{self.s3_url(table_name)}', '{self.access_key_id}', '{self.secret_access_key}')"

    def table_function_cluster_sql(self, cluster_name, table_name):
        return (
            f"paimonS3Cluster('{cluster_name}', '{self.s3_url(table_name)}', "
            f"'{self.access_key_id}', '{self.secret_access_key}')"
        )

    def create_engine_sql(self, ch_table_name, table_name):
        return (
            f"CREATE TABLE IF NOT EXISTS {ch_table_name} ENGINE = PaimonS3("
            f"'{self.s3_url(table_name)}', '{self.access_key_id}', '{self.secret_access_key}')"
        )


class PaimonAzureManager(PaimonManagerBase):
    backend = "azure"

    def __init__(self, account_name, account_key, container_name):
        super().__init__()
        from azure.storage.blob import BlobServiceClient

        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.storage_account_url = f"https://{account_name}.blob.core.windows.net"
        self._container = BlobServiceClient(
            account_url=self.storage_account_url, credential=account_key
        ).get_container_client(container_name)

    @classmethod
    def from_env(cls):
        account_name = os.getenv("E2E_ICEBERG_AZURE_ACCOUNT_NAME", "")
        account_key = os.getenv("E2E_ICEBERG_AZURE_ACCOUNT_KEY", "")
        container_name = os.getenv("E2E_ICEBERG_AZURE_CONTAINER_NAME", "")
        required = {
            "E2E_ICEBERG_AZURE_ACCOUNT_NAME": account_name,
            "E2E_ICEBERG_AZURE_ACCOUNT_KEY": account_key,
            "E2E_ICEBERG_AZURE_CONTAINER_NAME": container_name,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise PaimonE2ECredentialsMissing(
                "Missing Paimon Azure e2e settings: " + ", ".join(missing)
            )
        return cls(account_name, account_key, container_name)

    def _upload_file(self, local_file, remote_rel_path):
        with open(local_file, "rb") as f:
            self._container.upload_blob(name=remote_rel_path, data=f, overwrite=True)

    def _delete_prefix(self, remote_rel_prefix):
        blob_names = [blob.name for blob in self._container.list_blobs(name_starts_with=remote_rel_prefix)]
        # Deepest blobs first (matters for hierarchical-namespace accounts).
        for name in sorted(blob_names, key=lambda n: n.count("/"), reverse=True):
            try:
                self._container.delete_blob(name)
            except Exception:
                logger.warning("Failed to delete blob %s", name, exc_info=True)

    @property
    def connection_string(self):
        return (
            f"DefaultEndpointsProtocol=https;AccountName={self.account_name};"
            f"AccountKey={self.account_key};EndpointSuffix=core.windows.net"
        )

    def table_function_connection_string_sql(self, table_name):
        return (
            f"paimonAzure('{self.connection_string}', '{self.container_name}', "
            f"'{self.table_remote_prefix(table_name)}')"
        )

    def _credentials_sql(self, table_name):
        return (
            f"'{self.storage_account_url}', '{self.container_name}', "
            f"'{self.table_remote_prefix(table_name)}', '{self.account_name}', '{self.account_key}'"
        )

    def table_function_sql(self, table_name):
        return f"paimonAzure({self._credentials_sql(table_name)})"

    def table_function_cluster_sql(self, cluster_name, table_name):
        return f"paimonAzureCluster('{cluster_name}', {self._credentials_sql(table_name)})"

    def create_engine_sql(self, ch_table_name, table_name):
        return (
            f"CREATE TABLE IF NOT EXISTS {ch_table_name} "
            f"ENGINE = PaimonAzure({self._credentials_sql(table_name)})"
        )
