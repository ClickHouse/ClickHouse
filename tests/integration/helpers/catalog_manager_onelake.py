import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pyarrow as pa
import requests
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pyiceberg.catalog.sql import SqlCatalog

from helpers.catalog_manager import CatalogManager, arrow_to_iceberg_schema

log = logging.getLogger(__name__)

ONELAKE_DFS_HOST = "onelake.dfs.fabric.microsoft.com"
ONELAKE_BLOB_HOST = "onelake.blob.fabric.microsoft.com"
ONELAKE_STORAGE_ENDPOINT = f"https://{ONELAKE_DFS_HOST}"
ONELAKE_CATALOG_URL = "https://onelake.table.fabric.microsoft.com/iceberg"

@dataclass
class OneLakeConfig:
    tenant_id: str
    client_id: str
    client_secret: str
    catalog_url: str
    storage_endpoint: str
    workspace_id: str
    lakehouse_id: str


class OneLakeCatalogManager(CatalogManager):
    """Manages table lifecycle inside an existing Fabric Lakehouse.

    Creates native Iceberg tables on OneLake DFS via PyIceberg's
    ``SqlCatalog`` with ``adlfs`` IO, so metadata contains correct
    ``abfss://`` paths.  Fabric automatically exposes them through
    an Iceberg REST API, which is what ClickHouse reads.
    """

    def __init__(self, config: OneLakeConfig):
        self.config = config
        self._credential = ClientSecretCredential(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret,
        )
        self._run_id = uuid.uuid4().hex[:8]
        self._local_db = f"/tmp/onelake_e2e_{self._run_id}"
        os.makedirs(self._local_db, exist_ok=True)

        warehouse_uri = (
            f"abfss://{config.workspace_id}@{ONELAKE_DFS_HOST}"
            f"/{config.lakehouse_id}/Tables"
        )
        self._catalog = SqlCatalog(
            "e2e_onelake",
            **{
                "uri": f"sqlite:///{self._local_db}/catalog.db",
                "warehouse": warehouse_uri,
                "adls.account-name": "onelake",
                "adls.account-host": ONELAKE_BLOB_HOST,
                "adls.tenant-id": config.tenant_id,
                "adls.client-id": config.client_id,
                "adls.client-secret": config.client_secret,
            },
        )
        try:
            self._catalog.create_namespace("dbo")
        except Exception:
            pass

        self._dfs_client = DataLakeServiceClient(
            account_url=config.storage_endpoint,
            credential=self._credential,
        )
        self._tables_created: List[str] = []

    @classmethod
    def from_env(cls) -> "OneLakeCatalogManager":
        tenant_id = os.getenv("E2E_ONELAKE_TENANT_ID", "")
        client_id = os.getenv("E2E_ONELAKE_CLIENT_ID", "")
        client_secret = os.getenv("E2E_ONELAKE_CLIENT_SECRET", "")
        workspace_id = os.getenv("E2E_ONELAKE_WORKSPACE_ID", "")
        lakehouse_id = os.getenv("E2E_ONELAKE_LAKEHOUSE_ID", "")

        required = {
            "E2E_ONELAKE_TENANT_ID": tenant_id,
            "E2E_ONELAKE_CLIENT_ID": client_id,
            "E2E_ONELAKE_CLIENT_SECRET": client_secret,
            "E2E_ONELAKE_WORKSPACE_ID": workspace_id,
            "E2E_ONELAKE_LAKEHOUSE_ID": lakehouse_id,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise Exception("Missing OneLake e2e settings: " + ", ".join(missing))

        return cls(
            OneLakeConfig(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                catalog_url=ONELAKE_CATALOG_URL,
                storage_endpoint=ONELAKE_STORAGE_ENDPOINT,
                workspace_id=workspace_id,
                lakehouse_id=lakehouse_id,
            )
        )

    @property
    def warehouse(self) -> str:
        return f"{self.config.workspace_id}/{self.config.lakehouse_id}"

    @staticmethod
    def make_database_name() -> str:
        return f"e2e_onelake_{uuid.uuid4().hex[:8]}"

    def create_db_sql(self, database_name: str, **overrides) -> str:
        """Build a ``CREATE DATABASE`` SQL string.

        Uses real credentials by default; pass keyword overrides
        (``tenant_id``, ``client_id``, ``client_secret``,
        ``catalog_url``, ``oauth_server_uri``, ``warehouse``,
        ``auth_scope``) to substitute individual values.
        """
        cfg = self.config
        t = overrides.get("tenant_id", cfg.tenant_id)
        c = overrides.get("client_id", cfg.client_id)
        s = overrides.get("client_secret", cfg.client_secret)
        u = overrides.get("catalog_url", cfg.catalog_url)
        o = overrides.get(
            "oauth_server_uri",
            f"https://login.microsoftonline.com/{t}/oauth2/v2.0/token",
        )
        w = overrides.get("warehouse", self.warehouse)
        a = overrides.get("auth_scope", "https://storage.azure.com/.default")
        return (
            f"CREATE DATABASE {database_name} ENGINE = DataLakeCatalog('{u}')\n"
            f"SETTINGS\n"
            f"    catalog_type='onelake',\n"
            f"    warehouse='{w}',\n"
            f"    onelake_tenant_id='{t}',\n"
            f"    onelake_client_id='{c}',\n"
            f"    onelake_client_secret='{s}',\n"
            f"    oauth_server_uri='{o}',\n"
            f"    auth_scope='{a}'"
        )

    def create_catalog(self, node, database_name: str) -> None:
        """Drop-and-create a DataLakeCatalog database with real credentials.

        Assumes ``allow_experimental_database_iceberg`` is enabled in the
        server's user config."""
        node.query(
            f"DROP DATABASE IF EXISTS {database_name};\n"
            + self.create_db_sql(database_name)
        )

    def try_create_database(self, node, database_name: str, **overrides) -> str:
        """Try ``CREATE DATABASE`` then ``SHOW TABLES``.

        Returns the first non-empty stderr encountered, or an empty
        string when both statements succeed.  Useful for negative tests
        where the error may surface at either stage.  Assumes
        ``allow_experimental_database_iceberg`` is enabled in the
        server's user config.
        """
        sql = self.create_db_sql(database_name, **overrides)
        _, err = node.query_and_get_answer_with_error(sql)
        if err.strip():
            return err
        _, err = node.query_and_get_answer_with_error(
            f"SHOW TABLES FROM {database_name}"
        )
        return err

    def resolve_table_name(self, node, database_name: str, short_name: str) -> str:
        """Resolve a short table name to its fully-qualified
        ``dbo.<table>`` form returned by ``SHOW TABLES``."""
        raw = node.query(f"SHOW TABLES FROM {database_name}").strip()
        for line in raw.splitlines():
            if short_name in line:
                return line.strip()
        raise AssertionError(
            f"Table '{short_name}' not found in SHOW TABLES output:\n{raw}"
        )

    def _table_location(self, table_name: str) -> str:
        return (
            f"abfss://{self.config.workspace_id}@{ONELAKE_DFS_HOST}"
            f"/{self.config.lakehouse_id}/Tables/dbo/{table_name}"
        )

    def create_table(
        self,
        data: pa.Table,
        table_name: Optional[str] = None,
    ) -> str:
        if table_name is None:
            table_name = f"e2e_{uuid.uuid4().hex[:10]}"

        table = self._catalog.create_table(
            identifier=f"dbo.{table_name}",
            schema=arrow_to_iceberg_schema(data),
            location=self._table_location(table_name),
        )
        table.append(data)

        self._tables_created.append(table_name)
        log.info("Created Iceberg table '%s' on OneLake", table_name)
        return table_name

    def create_sample_table(self) -> Tuple[str, pa.Table]:
        """Create a small ``(id Int64, value String)`` table.

        Returns ``(table_name, arrow_table)`` so callers can assert on
        the expected data.
        """
        data = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "value": pa.array(["one", "two"], type=pa.string()),
            }
        )
        name = self.create_table(data)
        return name, data

    def _table_api_url(self, table_name: str, namespace: str = "dbo") -> str:
        return (
            f"{self.config.catalog_url}/v1/"
            f"{self.config.workspace_id}/{self.config.lakehouse_id}"
            f"/namespaces/{namespace}/tables/{table_name}"
        )

    def _authed_headers(self) -> dict:
        token = self._credential.get_token(
            "https://storage.azure.com/.default"
        ).token
        return {"Authorization": f"Bearer {token}"}

    def wait_for_table_ready(
        self,
        table_name: str,
        namespace: str = "dbo",
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> None:
        """Poll Fabric Iceberg REST API until the Load-Table endpoint
        returns 200 **and** the metadata contains a valid snapshot,
        or raise after *timeout* seconds.

        Fabric's metadata indexing is asynchronous: a table can appear
        in the namespace listing (``SHOW TABLES``) and the Load-Table
        endpoint can return 200 before the actual Iceberg snapshot
        (with data file manifests) is populated.  We therefore also
        check that ``current-snapshot-id >= 0``.
        """
        url = self._table_api_url(table_name, namespace)
        deadline = time.monotonic() + timeout
        attempt = 0
        last_status = "no response"
        while True:
            attempt += 1
            try:
                resp = requests.get(url, headers=self._authed_headers(), timeout=30)
                last_status = f"{resp.status_code} {resp.text[:200]}"
                if resp.status_code == 200:
                    body = resp.json()
                    metadata = body.get("metadata", {})
                    snap_id = metadata.get("current-snapshot-id", -1)
                    if snap_id >= 0:
                        log.info(
                            "Table '%s' ready (snapshot %s) after %d attempts",
                            table_name, snap_id, attempt,
                        )
                        return
                    log.info(
                        "Table '%s' has no snapshot yet "
                        "(current-snapshot-id=%s, attempt %d)",
                        table_name, snap_id, attempt,
                    )
                else:
                    log.warning(
                        "Load-Table API returned %d for '%s' (attempt %d): %s",
                        resp.status_code, table_name, attempt,
                        resp.text[:200],
                    )
            except requests.RequestException as exc:
                last_status = str(exc)
                log.warning(
                    "Load-Table API request failed for '%s' (attempt %d): %s",
                    table_name, attempt, exc,
                )

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Table '{table_name}' not ready via Iceberg REST API "
                    f"after {timeout}s ({attempt} attempts). "
                    f"Last status: {last_status}"
                )
            time.sleep(poll_interval)

    def wait_for_table_gone(
        self,
        table_name: str,
        namespace: str = "dbo",
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> None:
        """Poll Fabric Iceberg REST API until the Load-Table endpoint
        returns 404 for *table_name*, or raise after *timeout* seconds.
        """
        url = self._table_api_url(table_name, namespace)
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = requests.get(url, headers=self._authed_headers(), timeout=30)
                if resp.status_code == 404:
                    log.info(
                        "Table '%s' gone after %d attempts", table_name, attempt
                    )
                    return
                log.info(
                    "Table '%s' still visible (status %d, attempt %d)",
                    table_name, resp.status_code, attempt,
                )
            except requests.RequestException as exc:
                log.info(
                    "Table '%s' API unreachable (attempt %d): %s",
                    table_name, attempt, exc,
                )
                return

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Table '{table_name}' still visible via Iceberg REST API "
                    f"after {timeout}s ({attempt} attempts)."
                )
            time.sleep(poll_interval)

    def cleanup_table(self, table_name: str) -> None:
        try:
            fs = self._dfs_client.get_file_system_client(
                self.config.workspace_id
            )
            base = f"{self.config.lakehouse_id}/Tables/dbo/{table_name}"
            paths = list(fs.get_paths(path=base, recursive=True))
            for p in sorted(paths, key=lambda x: x.name, reverse=True):
                if p.is_directory:
                    fs.get_directory_client(p.name).delete_directory()
                else:
                    fs.get_file_client(p.name).delete_file()
            fs.get_directory_client(base).delete_directory()
            log.info("Cleaned up table '%s'", table_name)
        except Exception as exc:
            log.warning("Cleanup of '%s' failed: %s", table_name, exc)

    def cleanup_all(self) -> None:
        for name in self._tables_created:
            self.cleanup_table(name)
        self._tables_created.clear()
        if os.path.exists(self._local_db):
            shutil.rmtree(self._local_db, ignore_errors=True)
