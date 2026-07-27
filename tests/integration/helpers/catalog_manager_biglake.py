import logging
import os
import time
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import gcsfs
import pyarrow as pa
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials as GoogleOAuth2Credentials
from pyiceberg.catalog import load_catalog

from helpers.catalog_manager import CatalogManager, arrow_to_iceberg_schema

log = logging.getLogger(__name__)

BIGLAKE_CATALOG_URL = "https://biglake.googleapis.com/iceberg/v1beta/restcatalog"
GOOGLE_OAUTH2_TOKEN_ENDPOINT = "https://oauth2.googleapis.com/token"
NAMESPACE_PREFIX = "ch_e2e_bl_"


@dataclass
class BigLakeConfig:
    project_id: str
    gcs_bucket: str
    gcs_prefix: str
    adc_client_id: str
    adc_client_secret: str
    adc_refresh_token: str


class BigLakeCatalogManager(CatalogManager):
    """Manages table lifecycle inside a BigLake Metastore Iceberg REST catalog.

    All tables for a test session share a single namespace to avoid
    ClickHouse's recursive namespace traversal hitting BigLake's
    flat-namespace limitation (multi-level namespaces unsupported).
    """

    def __init__(self, config: BigLakeConfig):
        self.config = config
        self._credential = GoogleOAuth2Credentials(
            token=None,
            refresh_token=config.adc_refresh_token,
            token_uri=GOOGLE_OAUTH2_TOKEN_ENDPOINT,
            client_id=config.adc_client_id,
            client_secret=config.adc_client_secret,
        )
        self._credential.refresh(GoogleAuthRequest())

        token_expiry_ms = str(
            int(self._credential.expiry.timestamp() * 1000)
        )
        self.catalog = load_catalog(
            "clickhouse_e2e_biglake",
            type="rest",
            uri=BIGLAKE_CATALOG_URL,
            warehouse=self._warehouse_path(),
            token=self._credential.token,
            **{
                "header.x-goog-user-project": config.project_id,
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "gcs.project-id": config.project_id,
                "gcs.oauth2.token": self._credential.token,
                "gcs.oauth2.token-expires-at": token_expiry_ms,
            },
        )

        # Encode creation timestamp in the name so stale cleanup can skip
        # namespaces from concurrently running sessions.
        self._session_namespace = (
            f"{NAMESPACE_PREFIX}{int(time.time())}_{uuid.uuid4().hex[:6]}"
        )
        self._tables_created: List[str] = []

        self._cleanup_stale_namespaces()

        self._refresh_token_if_needed()
        self.catalog.create_namespace(self._session_namespace)
        log.info("Created session namespace '%s'", self._session_namespace)

    def _warehouse_path(self) -> str:
        if self.config.gcs_prefix:
            return f"gs://{self.config.gcs_bucket}/{self.config.gcs_prefix}"
        return f"gs://{self.config.gcs_bucket}"

    def _refresh_token_if_needed(self) -> str:
        if not self._credential.valid:
            self._credential.refresh(GoogleAuthRequest())
        return self._credential.token

    # Only clean up namespaces older than this to avoid racing with other
    # concurrently running test sessions.
    _STALE_GRACE_SECONDS: int = 3600

    def _cleanup_stale_namespaces(self) -> None:
        """Remove leftover namespaces from previous test runs."""
        try:
            self._refresh_token_if_needed()
            now = time.time()
            namespaces = self.catalog.list_namespaces()
            for ns_tuple in namespaces:
                ns = ns_tuple[0] if isinstance(ns_tuple, tuple) else ns_tuple
                if not ns.startswith(NAMESPACE_PREFIX):
                    continue
                if ns == self._session_namespace:
                    continue
                # Skip namespaces that may belong to a concurrent session:
                # parse the timestamp encoded as ch_e2e_bl_{ts}_{hex}.
                parts = ns.split("_")
                try:
                    ns_ts = int(parts[3])
                    if now - ns_ts < self._STALE_GRACE_SECONDS:
                        log.debug(
                            "Skipping recently-created namespace '%s'", ns
                        )
                        continue
                except (IndexError, ValueError):
                    pass  # Old format without timestamp — proceed with cleanup
                try:
                    for tbl in self.catalog.list_tables(ns):
                        tbl_id = f"{ns}.{tbl[1]}"
                        try:
                            self.catalog.drop_table(tbl_id)
                        except Exception:
                            pass
                    self.catalog.drop_namespace(ns)
                    log.info("Cleaned up stale namespace '%s'", ns)
                    ns_gcs_path = (
                        f"{self.config.gcs_bucket}/{self.config.gcs_prefix}/{ns}"
                    )
                    self._delete_gcs_prefix(ns_gcs_path)
                except Exception as exc:
                    log.warning("Failed to clean up namespace '%s': %s", ns, exc)
        except Exception as exc:
            log.warning("Stale namespace cleanup failed: %s", exc)

    @classmethod
    def from_env(cls) -> "BigLakeCatalogManager":
        project_id = os.getenv("E2E_GCP_PROJECT", "") or os.getenv("E2E_BIGLAKE_PROJECT_ID", "")
        gcs_bucket = os.getenv("E2E_GCP_BUCKET", "") or os.getenv("E2E_BIGLAKE_GCS_BUCKET", "")
        gcs_prefix = os.getenv("E2E_BIGLAKE_GCS_PREFIX", "clickhouse-e2e-biglake")
        adc_client_id = os.getenv("E2E_BIGLAKE_ADC_CLIENT_ID", "")
        adc_client_secret = os.getenv("E2E_BIGLAKE_ADC_CLIENT_SECRET", "")
        adc_refresh_token = os.getenv("E2E_BIGLAKE_ADC_REFRESH_TOKEN", "")

        required = {
            "E2E_GCP_PROJECT": project_id,
            "E2E_GCP_BUCKET": gcs_bucket,
            "E2E_BIGLAKE_ADC_CLIENT_ID": adc_client_id,
            "E2E_BIGLAKE_ADC_CLIENT_SECRET": adc_client_secret,
            "E2E_BIGLAKE_ADC_REFRESH_TOKEN": adc_refresh_token,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise Exception("Missing BigLake e2e settings: " + ", ".join(missing))

        return cls(
            BigLakeConfig(
                project_id=project_id,
                gcs_bucket=gcs_bucket,
                gcs_prefix=gcs_prefix,
                adc_client_id=adc_client_id,
                adc_client_secret=adc_client_secret,
                adc_refresh_token=adc_refresh_token,
            )
        )

    # ------------------------------------------------------------------
    # CatalogManager interface
    # ------------------------------------------------------------------

    @staticmethod
    def make_database_name() -> str:
        return f"e2e_biglake_{uuid.uuid4().hex[:8]}"

    def create_table(
        self, data: pa.Table, table_name: Optional[str] = None
    ) -> str:
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"

        namespace = self._session_namespace
        table_identifier = f"{namespace}.{table_name}"
        table_location = (
            f"{self._warehouse_path()}/{namespace}/{table_name}"
        )

        self._refresh_token_if_needed()

        iceberg_schema = arrow_to_iceberg_schema(data)
        table = self.catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            location=table_location,
        )
        table.append(data)

        log.info(
            "Created BigLake Iceberg table '%s' at %s",
            table_identifier,
            table_location,
        )
        self._tables_created.append(table_name)
        return table_name

    def wait_for_table_ready(
        self,
        table_name: str,
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> None:
        """Poll until the table is both loadable AND visible in
        BigLake's namespace listing.
        """
        namespace = self._session_namespace
        identifier = f"{namespace}.{table_name}"
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            self._refresh_token_if_needed()
            try:
                self.catalog.load_table(identifier)
                listed = self.catalog.list_tables(namespace)
                listed_names = [t[1] for t in listed]
                if table_name in listed_names:
                    log.info(
                        "Table '%s' ready and listed after %d attempts",
                        identifier,
                        attempt,
                    )
                    return
                log.warning(
                    "Table '%s' loadable but not yet listed "
                    "(attempt %d, listed=%s)",
                    identifier,
                    attempt,
                    listed_names,
                )
            except Exception as exc:
                log.warning(
                    "load_table('%s') failed (attempt %d): %s",
                    identifier,
                    attempt,
                    exc,
                )

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Table '{identifier}' not ready/listed via pyiceberg "
                    f"after {timeout}s ({attempt} attempts)."
                )
            time.sleep(poll_interval)

    def wait_for_table_gone(
        self,
        table_name: str,
        timeout: float = 120,
        poll_interval: float = 5,
    ) -> None:
        """Poll until load_table raises, confirming the table is gone."""
        namespace = self._session_namespace
        identifier = f"{namespace}.{table_name}"
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            self._refresh_token_if_needed()
            try:
                self.catalog.load_table(identifier)
                log.info(
                    "Table '%s' still visible (attempt %d)",
                    identifier,
                    attempt,
                )
            except Exception:
                log.info(
                    "Table '%s' gone after %d attempts",
                    identifier,
                    attempt,
                )
                return

            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Table '{identifier}' still visible via pyiceberg "
                    f"after {timeout}s ({attempt} attempts)."
                )
            time.sleep(poll_interval)

    def cleanup_table(self, table_name: str) -> None:
        self._refresh_token_if_needed()
        namespace = self._session_namespace
        table_identifier = f"{namespace}.{table_name}"
        try:
            self.catalog.drop_table(table_identifier)
            log.info("Dropped table '%s'", table_identifier)
        except Exception as exc:
            log.warning("Cleanup of table '%s' failed: %s", table_identifier, exc)

    def _delete_gcs_prefix(self, gcs_path: str) -> None:
        try:
            fs = gcsfs.GCSFileSystem(token=self._credential.token)
            if fs.exists(gcs_path):
                fs.rm(gcs_path, recursive=True)
                log.info("Deleted GCS path '%s'", gcs_path)
        except Exception as exc:
            log.warning("GCS cleanup of '%s' failed: %s", gcs_path, exc)

    def cleanup_all(self) -> None:
        self._refresh_token_if_needed()
        for table_name in self._tables_created:
            table_identifier = f"{self._session_namespace}.{table_name}"
            try:
                self.catalog.drop_table(table_identifier)
            except Exception:
                pass
        self._tables_created.clear()

        self._refresh_token_if_needed()
        try:
            self.catalog.drop_namespace(self._session_namespace)
            log.info("Dropped session namespace '%s'", self._session_namespace)
        except Exception:
            pass

        ns_gcs_path = (
            f"{self.config.gcs_bucket}/{self.config.gcs_prefix}"
            f"/{self._session_namespace}"
        )
        self._delete_gcs_prefix(ns_gcs_path)

    def resolve_table_name(
        self,
        node,
        database_name: str,
        short_name: str,
        timeout: float = 60,
        poll_interval: float = 3,
    ) -> str:
        """Retry SHOW TABLES until the table appears.

        BigLake has a propagation delay between table creation and
        ClickHouse's SHOW TABLES seeing it via the REST API.
        """
        deadline = time.monotonic() + timeout
        attempt = 0
        while True:
            attempt += 1
            raw = node.query(f"SHOW TABLES FROM {database_name}").strip()
            for line in raw.splitlines():
                if short_name in line:
                    log.info(
                        "Resolved '%s' in '%s' after %d attempts",
                        short_name,
                        database_name,
                        attempt,
                    )
                    return line.strip()

            if time.monotonic() >= deadline:
                raise AssertionError(
                    f"Table '{short_name}' not found in "
                    f"SHOW TABLES output after {timeout}s "
                    f"({attempt} attempts):\n{raw}"
                )
            log.warning(
                "SHOW TABLES FROM %s: '%s' not visible (attempt %d)",
                database_name,
                short_name,
                attempt,
            )
            time.sleep(poll_interval)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def clickhouse_env_variables(self) -> Dict[str, str]:
        return {}

    def create_catalog(self, node, database_name: str) -> None:
        cfg = self.config
        node.query(
            f"""
DROP DATABASE IF EXISTS {database_name};
SET allow_experimental_database_iceberg=1;
CREATE DATABASE {database_name} ENGINE = DataLakeCatalog('{BIGLAKE_CATALOG_URL}')
SETTINGS
    catalog_type='biglake',
    warehouse='{self._warehouse_path()}',
    google_project_id='{cfg.project_id}',
    google_adc_client_id='{cfg.adc_client_id}',
    google_adc_client_secret='{cfg.adc_client_secret}',
    google_adc_refresh_token='{cfg.adc_refresh_token}'
"""
        )
