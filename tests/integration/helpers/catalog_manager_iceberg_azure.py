import logging
import os
import shutil
import uuid
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pyarrow as pa
from azure.storage.blob import BlobServiceClient
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.transforms import IdentityTransform

from helpers.catalog_manager import arrow_to_iceberg_schema

log = logging.getLogger(__name__)


@dataclass
class IcebergAzureConfig:
    account_name: str
    account_key: str
    container_name: str

    @property
    def storage_account_url(self) -> str:
        return f"https://{self.account_name}.blob.core.windows.net"

    @property
    def connection_string(self) -> str:
        return (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={self.account_name};"
            f"AccountKey={self.account_key};"
            f"EndpointSuffix=core.windows.net"
        )


class IcebergAzureCatalogManager:
    """Manages Iceberg table lifecycle for e2e tests against Azure Blob Storage.

    Creates Iceberg tables locally via pyiceberg's SqlCatalog, then uploads
    the generated metadata and data files to Azure Blob Storage.
    ClickHouse reads the uploaded tables via icebergAzure() table function
    or IcebergAzure table engine.
    """

    def __init__(self, config: IcebergAzureConfig):
        self.config = config
        self._run_id = uuid.uuid4().hex[:8]
        self._dir_name = f"iceberg_e2e_{self._run_id}"
        self._local_warehouse = f"/tmp/{self._dir_name}"
        os.makedirs(self._local_warehouse, exist_ok=True)

        self._catalog = SqlCatalog(
            "e2e_azure",
            **{
                "uri": f"sqlite:///{self._local_warehouse}/catalog.db",
                "warehouse": f"file://{self._local_warehouse}",
            },
        )
        try:
            self._catalog.create_namespace("default")
        except Exception:
            pass

        self._blob_service = BlobServiceClient(
            account_url=config.storage_account_url,
            credential=config.account_key,
        )
        self._container = self._blob_service.get_container_client(
            config.container_name
        )
        self._tables_created: List[str] = []

    @classmethod
    def from_env(cls) -> "IcebergAzureCatalogManager":
        account_name = os.getenv("E2E_ICEBERG_AZURE_ACCOUNT_NAME", "")
        account_key = os.getenv("E2E_ICEBERG_AZURE_ACCOUNT_KEY", "")
        container_name = os.getenv("E2E_ICEBERG_AZURE_CONTAINER_NAME", "")

        required = {
            "E2E_ICEBERG_AZURE_ACCOUNT_NAME": account_name,
            "E2E_ICEBERG_AZURE_ACCOUNT_KEY": account_key,
            "E2E_ICEBERG_AZURE_CONTAINER_NAME": container_name,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise Exception(
                "Missing Iceberg Azure e2e settings: " + ", ".join(missing)
            )

        return cls(
            IcebergAzureConfig(
                account_name=account_name,
                account_key=account_key,
                container_name=container_name,
            )
        )

    def blob_path(self, table_name: str) -> str:
        return f"{self._dir_name}/default/{table_name}/"

    def _table_location(self, table_name: str) -> str:
        return f"file://{self._local_warehouse}/default/{table_name}"

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
            location=self._table_location(table_name),
        )
        table.append(data)

        self._upload_table(table_name)
        self._tables_created.append(table_name)
        log.info("Created Iceberg table '%s' on Azure", table_name)
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
            location=self._table_location(table_name),
        )

        snapshot_ids = []
        for batch in batches:
            table.append(batch)
            table = self._catalog.load_table(f"default.{table_name}")
            snapshot_ids.append(table.metadata.current_snapshot_id)

        self._upload_table(table_name)
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
            location=self._table_location(table_name),
        )
        table.append(initial_data)

        with table.update_schema() as update:
            update.add_column(new_column_name, new_column_type)

        table = self._catalog.load_table(f"default.{table_name}")
        table.append(evolved_data)

        self._upload_table(table_name)
        self._tables_created.append(table_name)
        log.info(
            "Created schema-evolved Iceberg table '%s'", table_name
        )
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
            location=self._table_location(table_name),
        )
        table.append(data)

        self._upload_table(table_name)
        self._tables_created.append(table_name)
        log.info(
            "Created partitioned Iceberg table '%s' (partition by %s)",
            table_name,
            partition_column,
        )
        return table_name

    def table_function_cluster_sql(
        self,
        cluster_name: str,
        table_name: str,
        fmt: str = "auto",
    ) -> str:
        cfg = self.config
        bp = self.blob_path(table_name)
        return (
            f"icebergAzureCluster("
            f"'{cluster_name}', "
            f"'{cfg.storage_account_url}', "
            f"'{cfg.container_name}', "
            f"'{bp}', "
            f"'{cfg.account_name}', "
            f"'{cfg.account_key}', "
            f"'{fmt}')"
        )

    def get_snapshot_timestamps(self, table_name: str) -> dict:
        """Return {snapshot_id: timestamp_ms} for all snapshots of a table."""
        table = self._catalog.load_table(f"default.{table_name}")
        return {s.snapshot_id: s.timestamp_ms for s in table.metadata.snapshots}

    def table_function_sql(self, table_name: str, fmt: str = "auto") -> str:
        cfg = self.config
        bp = self.blob_path(table_name)
        return (
            f"icebergAzure("
            f"'{cfg.storage_account_url}', "
            f"'{cfg.container_name}', "
            f"'{bp}', "
            f"'{cfg.account_name}', "
            f"'{cfg.account_key}', "
            f"'{fmt}')"
        )

    def table_function_conn_string_sql(self, table_name: str) -> str:
        cfg = self.config
        bp = self.blob_path(table_name)
        return (
            f"icebergAzure("
            f"'{cfg.connection_string}', "
            f"'{cfg.container_name}', "
            f"'{bp}')"
        )

    def create_engine_sql(self, ch_table_name: str, table_name: str) -> str:
        cfg = self.config
        bp = self.blob_path(table_name)
        return (
            f"CREATE TABLE IF NOT EXISTS {ch_table_name} "
            f"ENGINE = IcebergAzure("
            f"'{cfg.storage_account_url}', "
            f"'{cfg.container_name}', "
            f"'{bp}', "
            f"'{cfg.account_name}', "
            f"'{cfg.account_key}')"
        )

    def create_writable_table_sql(
        self,
        ch_table_name: str,
        schema_sql: str,
        table_name: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Build SQL to CREATE a new empty IcebergAzure table for writes.

        Returns (sql, table_name).
        """
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"
        cfg = self.config
        bp = self.blob_path(table_name)
        sql = (
            f"CREATE TABLE {ch_table_name} ({schema_sql}) "
            f"ENGINE = IcebergAzure("
            f"'{cfg.storage_account_url}', "
            f"'{cfg.container_name}', "
            f"'{bp}', "
            f"'{cfg.account_name}', "
            f"'{cfg.account_key}')"
        )
        self._tables_created.append(table_name)
        return sql, table_name

    def _upload_table(self, table_name: str) -> None:
        local_path = os.path.join(self._local_warehouse, "default", table_name)
        remote_prefix = f"{self._dir_name}/default/{table_name}"
        self._upload_directory(local_path, remote_prefix)

    def _upload_directory(self, local_path: str, remote_prefix: str) -> None:
        if not os.path.isdir(local_path):
            raise FileNotFoundError(
                f"Local iceberg table directory not found: {local_path}"
            )
        count = 0
        for root, _dirs, files in os.walk(local_path):
            for filename in files:
                local_file = os.path.join(root, filename)
                rel_path = os.path.relpath(local_file, local_path)
                blob_name = f"{remote_prefix}/{rel_path}"
                with open(local_file, "rb") as f:
                    self._container.upload_blob(
                        name=blob_name,
                        data=f,
                        overwrite=True,
                    )
                count += 1
                log.debug("Uploaded %s → %s", local_file, blob_name)
        if count == 0:
            raise RuntimeError(
                f"No files found to upload in {local_path}"
            )

    def cleanup_table(self, table_name: str) -> None:
        prefix = f"{self._dir_name}/default/{table_name}/"
        try:
            blobs = list(self._container.list_blobs(name_starts_with=prefix))
            blobs.sort(key=lambda b: b.name.count("/"), reverse=True)
            for blob in blobs:
                try:
                    self._container.delete_blob(blob.name)
                except Exception:
                    pass
            try:
                self._container.delete_blob(
                    f"{self._dir_name}/default/{table_name}"
                )
            except Exception:
                pass
            log.info("Cleaned up table '%s' (%d blobs)", table_name, len(blobs))
        except Exception as exc:
            log.warning("Cleanup of '%s' failed: %s", table_name, exc)

    def cleanup_all(self) -> None:
        for name in self._tables_created:
            self.cleanup_table(name)
        self._tables_created.clear()
        for suffix in [f"{self._dir_name}/default", self._dir_name]:
            try:
                self._container.delete_blob(suffix)
            except Exception:
                pass
        if os.path.exists(self._local_warehouse):
            shutil.rmtree(self._local_warehouse, ignore_errors=True)
