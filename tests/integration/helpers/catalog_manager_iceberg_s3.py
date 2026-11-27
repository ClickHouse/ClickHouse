import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import List, Optional, Tuple

import boto3
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from helpers.catalog_manager import arrow_to_iceberg_schema

log = logging.getLogger(__name__)


@dataclass
class IcebergS3Config:
    bucket: str
    prefix: str
    region: str
    access_key_id: str
    secret_access_key: str

    @property
    def warehouse_path(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"


class IcebergS3CatalogManager:
    """Manages Iceberg table lifecycle for e2e tests against S3.

    Uses pyiceberg's SqlCatalog with S3FileIO to write Iceberg tables
    directly to S3.  The catalog DB (SQLite) stays local; table data
    and metadata go straight to the S3 bucket so that all paths in
    Iceberg metadata are native s3:// URIs.
    """

    def __init__(self, config: IcebergS3Config):
        self.config = config
        self._run_id = uuid.uuid4().hex[:8]
        self._dir_name = f"iceberg_e2e_{self._run_id}"
        self._catalog_dir = f"/tmp/{self._dir_name}"
        os.makedirs(self._catalog_dir, exist_ok=True)

        s3_warehouse = (
            f"s3://{config.bucket}/{config.prefix}/{self._dir_name}"
        )
        self._catalog = SqlCatalog(
            "e2e_s3",
            **{
                "uri": f"sqlite:///{self._catalog_dir}/catalog.db",
                "warehouse": s3_warehouse,
                "s3.access-key-id": config.access_key_id,
                "s3.secret-access-key": config.secret_access_key,
                "s3.region": config.region,
            },
        )
        try:
            self._catalog.create_namespace("default")
        except Exception:
            pass

        self._s3 = boto3.client(
            "s3",
            region_name=config.region,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
        )
        self._tables_created: List[str] = []

    @classmethod
    def from_env(cls) -> "IcebergS3CatalogManager":
        region = os.getenv("E2E_AWS_REGION", "")
        bucket = os.getenv("E2E_AWS_S3_BUCKET", "")
        prefix = os.getenv("E2E_AWS_S3_PREFIX", "clickhouse-e2e-iceberg-s3")
        access_key_id = os.getenv("E2E_AWS_ACCESS_KEY_ID", "")
        secret_access_key = os.getenv("E2E_AWS_SECRET_ACCESS_KEY", "")

        required = {
            "E2E_AWS_REGION": region,
            "E2E_AWS_S3_BUCKET": bucket,
            "E2E_AWS_ACCESS_KEY_ID": access_key_id,
            "E2E_AWS_SECRET_ACCESS_KEY": secret_access_key,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise Exception(
                "Missing Iceberg S3 e2e settings: " + ", ".join(missing)
            )

        return cls(
            IcebergS3Config(
                bucket=bucket,
                prefix=prefix,
                region=region,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
            )
        )

    def s3_url(self, table_name: str) -> str:
        return (
            f"s3://{self.config.bucket}/{self.config.prefix}"
            f"/{self._dir_name}/default/{table_name}/"
        )

    def create_table(
        self,
        data: pa.Table,
        table_name: Optional[str] = None,
    ) -> str:
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"

        table = self._catalog.create_table(
            identifier=f"default.{table_name}",
            schema=arrow_to_iceberg_schema(data),
        )
        table.append(data)

        self._tables_created.append(table_name)
        log.info("Created Iceberg table '%s' on S3", table_name)
        return table_name

    def create_table_multi_snapshot(
        self,
        batches: List[pa.Table],
        table_name: Optional[str] = None,
    ) -> Tuple[str, List[int]]:
        """Create a table with multiple snapshots (one per batch).

        Returns (table_name, list_of_snapshot_ids).
        """
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"

        table = self._catalog.create_table(
            identifier=f"default.{table_name}",
            schema=arrow_to_iceberg_schema(batches[0]),
        )

        snapshot_ids = []
        for batch in batches:
            table.append(batch)
            table = self._catalog.load_table(f"default.{table_name}")
            snapshot_ids.append(table.metadata.current_snapshot_id)

        self._tables_created.append(table_name)
        log.info(
            "Created multi-snapshot Iceberg table '%s' (%d snapshots)",
            table_name,
            len(snapshot_ids),
        )
        return table_name, snapshot_ids

    def create_table_with_schema_evolution(
        self,
        initial_data: pa.Table,
        evolved_data: pa.Table,
        new_column_name: str,
        new_column_type,
        table_name: Optional[str] = None,
    ) -> str:
        """Create a table, evolve its schema by adding a column, then append new data."""
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"

        table = self._catalog.create_table(
            identifier=f"default.{table_name}",
            schema=arrow_to_iceberg_schema(initial_data),
        )
        table.append(initial_data)

        with table.update_schema() as update:
            update.add_column(new_column_name, new_column_type)

        table = self._catalog.load_table(f"default.{table_name}")
        table.append(evolved_data)

        self._tables_created.append(table_name)
        log.info("Created schema-evolved Iceberg table '%s'", table_name)
        return table_name

    def create_partitioned_table(
        self,
        data: pa.Table,
        partition_column: str,
        table_name: Optional[str] = None,
    ) -> str:
        """Create an Iceberg table partitioned by identity transform on the given column."""
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"

        iceberg_schema = arrow_to_iceberg_schema(data)
        partition_field_id = None
        for field in iceberg_schema.fields:
            if field.name == partition_column:
                partition_field_id = field.field_id
                break
        if partition_field_id is None:
            raise ValueError(
                f"Partition column '{partition_column}' not found in schema"
            )

        partition_spec = PartitionSpec(
            PartitionField(
                source_id=partition_field_id,
                field_id=1000,
                transform=IdentityTransform(),
                name=f"{partition_column}_identity",
            )
        )

        table = self._catalog.create_table(
            identifier=f"default.{table_name}",
            schema=iceberg_schema,
            partition_spec=partition_spec,
        )
        table.append(data)

        self._tables_created.append(table_name)
        log.info(
            "Created partitioned Iceberg table '%s' (partition by %s)",
            table_name,
            partition_column,
        )
        return table_name

    # ------------------------------------------------------------------
    # SQL generation
    # ------------------------------------------------------------------

    def table_function_sql(self, table_name: str, fmt: str = "auto") -> str:
        cfg = self.config
        url = self.s3_url(table_name)
        return (
            f"icebergS3("
            f"'{url}', "
            f"'{cfg.access_key_id}', "
            f"'{cfg.secret_access_key}', "
            f"'{fmt}')"
        )

    def table_function_cluster_sql(
        self,
        cluster_name: str,
        table_name: str,
        fmt: str = "auto",
    ) -> str:
        cfg = self.config
        url = self.s3_url(table_name)
        return (
            f"icebergS3Cluster("
            f"'{cluster_name}', "
            f"'{url}', "
            f"'{cfg.access_key_id}', "
            f"'{cfg.secret_access_key}', "
            f"'{fmt}')"
        )

    def create_engine_sql(self, ch_table_name: str, table_name: str) -> str:
        cfg = self.config
        url = self.s3_url(table_name)
        return (
            f"CREATE TABLE IF NOT EXISTS {ch_table_name} "
            f"ENGINE = IcebergS3("
            f"'{url}', "
            f"'{cfg.access_key_id}', "
            f"'{cfg.secret_access_key}')"
        )

    def create_writable_table_sql(
        self,
        ch_table_name: str,
        schema_sql: str,
        table_name: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Build SQL to CREATE a new empty IcebergS3 table for writes.

        Returns (sql, table_name).
        """
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"
        cfg = self.config
        url = self.s3_url(table_name)
        sql = (
            f"CREATE TABLE {ch_table_name} ({schema_sql}) "
            f"ENGINE = IcebergS3("
            f"'{url}', "
            f"'{cfg.access_key_id}', "
            f"'{cfg.secret_access_key}')"
        )
        self._tables_created.append(table_name)
        return sql, table_name

    def get_snapshot_timestamps(self, table_name: str) -> dict:
        """Return {snapshot_id: timestamp_ms} for all snapshots of a table."""
        table = self._catalog.load_table(f"default.{table_name}")
        return {s.snapshot_id: s.timestamp_ms for s in table.metadata.snapshots}

    def create_engine_with_glue_sql(
        self, ch_table_name: str, table_name: str
    ) -> str:
        """IcebergS3 with Glue catalog SETTINGS."""
        cfg = self.config
        url = self.s3_url(table_name)
        return (
            f"CREATE TABLE IF NOT EXISTS {ch_table_name} "
            f"ENGINE = IcebergS3("
            f"'{url}', "
            f"'{cfg.access_key_id}', "
            f"'{cfg.secret_access_key}') "
            f"SETTINGS "
            f"storage_catalog_type = 'glue', "
            f"storage_warehouse = '{cfg.warehouse_path}', "
            f"object_storage_endpoint = 's3://{cfg.bucket}/', "
            f"storage_region = '{cfg.region}', "
            f"storage_catalog_url = "
            f"'https://glue.{cfg.region}.amazonaws.com/iceberg/v1'"
        )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup_table(self, table_name: str) -> None:
        prefix = (
            f"{self.config.prefix}/{self._dir_name}/default/{table_name}/"
        )
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(
                Bucket=self.config.bucket, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    keys.append({"Key": obj["Key"]})
            if keys:
                self._s3.delete_objects(
                    Bucket=self.config.bucket, Delete={"Objects": keys}
                )
            log.info("Cleaned up table '%s' (%d objects)", table_name, len(keys))
        except Exception as exc:
            log.warning("Cleanup of '%s' failed: %s", table_name, exc)

    def cleanup_all(self) -> None:
        for name in self._tables_created:
            self.cleanup_table(name)
        self._tables_created.clear()
        run_prefix = f"{self.config.prefix}/{self._dir_name}/"
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(
                Bucket=self.config.bucket, Prefix=run_prefix
            ):
                for obj in page.get("Contents", []):
                    keys.append({"Key": obj["Key"]})
            if keys:
                self._s3.delete_objects(
                    Bucket=self.config.bucket, Delete={"Objects": keys}
                )
            log.info(
                "Deleted %d remaining objects under '%s'", len(keys), run_prefix
            )
        except Exception as exc:
            log.warning(
                "Failed to delete run prefix '%s': %s", run_prefix, exc
            )
        if os.path.exists(self._catalog_dir):
            shutil.rmtree(self._catalog_dir, ignore_errors=True)
