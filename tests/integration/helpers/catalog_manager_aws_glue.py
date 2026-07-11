import concurrent.futures
import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import boto3
import pyarrow as pa
from pyiceberg.catalog import load_catalog

from helpers.catalog_manager import CatalogManager, arrow_to_iceberg_schema

log = logging.getLogger(__name__)

NAMESPACE_PREFIX = "ch_e2e_glue_"


@dataclass
class AwsGlueConfig:
    region: str
    bucket: str
    prefix: str
    access_key_id: str
    secret_access_key: str
    session_token: str = ""

    @property
    def catalog_url(self) -> str:
        return f"https://glue.{self.region}.amazonaws.com"

    @property
    def warehouse_path(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"


class AwsGlueCatalogManager(CatalogManager):
    # Only clean up databases older than this to avoid racing with other
    # concurrently running test sessions.
    _STALE_GRACE_SECONDS: int = 43200  # 12 hours

    def __init__(self, config: AwsGlueConfig):
        self.config = config
        os.environ["AWS_DEFAULT_REGION"] = config.region
        os.environ["AWS_ACCESS_KEY_ID"] = config.access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = config.secret_access_key
        if config.session_token:
            os.environ["AWS_SESSION_TOKEN"] = config.session_token
        self.catalog = load_catalog(
            "clickhouse_e2e_glue",
            type="glue",
            **{"region_name": config.region},
        )
        self._namespaces_created: List[str] = []
        self._tables_created: List[Tuple[str, str]] = []
        self._glue = boto3.client(
            "glue",
            region_name=config.region,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            aws_session_token=config.session_token or None,
        )
        self._s3 = boto3.client(
            "s3",
            region_name=config.region,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            aws_session_token=config.session_token or None,
        )

        self._cleanup_stale_databases()

    def _cleanup_stale_databases(self) -> None:
        """Remove `ch_e2e_glue_*` Glue databases older than `_STALE_GRACE_SECONDS`.

        Leftovers accumulate in Glue whenever a test run crashes (the
        per-session `cleanup_all` only knows about namespaces created in
        *this* run). With many leftovers, ClickHouse's catalog refresh
        path -- which iterates every Glue database in the account on each
        query -- balloons to minutes per query. Trim aggressively at
        session start, skipping anything younger than the grace window so
        we don't race a concurrent session.
        """
        try:
            now = datetime.now(timezone.utc)
            stale: List[str] = []
            for page in self._glue.get_paginator("get_databases").paginate():
                for db in page.get("DatabaseList", []):
                    name = db["Name"]
                    if not name.startswith(NAMESPACE_PREFIX):
                        continue
                    create_time = db.get("CreateTime")
                    if create_time is None:
                        continue
                    age = (now - create_time).total_seconds()
                    if age >= self._STALE_GRACE_SECONDS:
                        stale.append(name)
            if not stale:
                return
            log.info(
                "Cleaning up %d stale Glue databases (older than %ds)",
                len(stale), self._STALE_GRACE_SECONDS,
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as pool:
                list(pool.map(self._delete_glue_database, stale))
        except Exception as exc:
            log.warning("Stale Glue database cleanup failed: %s", exc)

    def _delete_glue_database(self, db: str) -> None:
        """Drop all tables in `db`, the database itself, and its S3 prefix."""
        try:
            for page in self._glue.get_paginator("get_tables").paginate(DatabaseName=db):
                for t in page.get("TableList", []):
                    try:
                        self._glue.delete_table(DatabaseName=db, Name=t["Name"])
                    except Exception as exc:
                        log.debug("delete_table(%s.%s) failed: %s", db, t["Name"], exc)
            try:
                self._glue.delete_database(Name=db)
            except Exception as exc:
                log.debug("delete_database(%s) failed: %s", db, exc)
            self._delete_s3_prefix_dir(db)
        except Exception as exc:
            log.warning("Cleanup of stale Glue database '%s' failed: %s", db, exc)

    def _delete_s3_prefix_dir(self, namespace: str) -> None:
        prefix = f"{self.config.prefix}/{namespace}/"
        try:
            keys: List[Dict[str, str]] = []
            for page in self._s3.get_paginator("list_objects_v2").paginate(
                Bucket=self.config.bucket, Prefix=prefix
            ):
                for obj in page.get("Contents", []):
                    keys.append({"Key": obj["Key"]})
            for i in range(0, len(keys), 1000):
                self._s3.delete_objects(
                    Bucket=self.config.bucket,
                    Delete={"Objects": keys[i:i + 1000]},
                )
        except Exception as exc:
            log.debug("S3 cleanup of '%s' failed: %s", namespace, exc)

    @classmethod
    def from_env(cls) -> "AwsGlueCatalogManager":
        region = (

            os.getenv("E2E_AWS_REGION", "")
        )
        bucket = os.getenv("E2E_AWS_S3_BUCKET", "")
        prefix = os.getenv("E2E_AWS_S3_PREFIX", "clickhouse-e2e-glue")
        access_key_id = os.getenv("E2E_AWS_ACCESS_KEY_ID", "")
        secret_access_key = os.getenv("E2E_AWS_SECRET_ACCESS_KEY", "")
        session_token = os.getenv("AWS_SESSION_TOKEN", "")

        missing = []
        if not region:
            missing.append("E2E_AWS_REGION")
        if not bucket:
            missing.append("E2E_AWS_S3_BUCKET")
        if not access_key_id:
            missing.append("E2E_AWS_ACCESS_KEY_ID")
        if not secret_access_key:
            missing.append("E2E_AWS_SECRET_ACCESS_KEY")

        if missing:
            raise Exception("Missing AWS Glue e2e settings: " + ", ".join(missing))

        return cls(
            AwsGlueConfig(
                region=region,
                bucket=bucket,
                prefix=prefix,
                access_key_id=access_key_id,
                secret_access_key=secret_access_key,
                session_token=session_token,
            )
        )

    # ------------------------------------------------------------------
    # CatalogManager interface
    # ------------------------------------------------------------------

    @staticmethod
    def make_database_name() -> str:
        return f"e2e_glue_aws_{uuid.uuid4().hex[:8]}"

    def create_catalog(self, node, database_name: str) -> None:
        self.create_clickhouse_glue_database(
            node, database_name, credentials_mode="settings"
        )

    def create_table(
        self, data: pa.Table, table_name: Optional[str] = None
    ) -> str:
        namespace = f"{NAMESPACE_PREFIX}{uuid.uuid4().hex[:10]}"
        if table_name is None:
            table_name = f"tbl_{uuid.uuid4().hex[:8]}"

        table_identifier = f"{namespace}.{table_name}"
        table_location = (
            f"s3://{self.config.bucket}/{self.config.prefix}"
            f"/{namespace}/{table_name}"
        )

        iceberg_schema = arrow_to_iceberg_schema(data)

        self.catalog.create_namespace(namespace)
        table = self.catalog.create_table(
            identifier=table_identifier,
            schema=iceberg_schema,
            location=table_location,
        )
        table.append(data)

        self._namespaces_created.append(namespace)
        self._tables_created.append((namespace, table_name))
        log.info("Created Glue Iceberg table '%s'", table_identifier)
        return table_name

    def wait_for_table_ready(self, table_name: str) -> None:
        pass

    def wait_for_table_gone(self, table_name: str) -> None:
        pass

    def _delete_s3_prefix(self, namespace: str, table_name: str) -> None:
        prefix = f"{self.config.prefix}/{namespace}/{table_name}/"
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
            log.info(
                "Deleted %d S3 objects for '%s/%s'", len(keys), namespace, table_name
            )
        except Exception as exc:
            log.warning(
                "S3 cleanup of '%s/%s' failed: %s", namespace, table_name, exc
            )

    def cleanup_table(self, table_name: str) -> None:
        for ns, tn in self._tables_created:
            if tn == table_name:
                try:
                    self.catalog.drop_table(f"{ns}.{tn}")
                except Exception as exc:
                    log.warning("Cleanup of '%s.%s' failed: %s", ns, tn, exc)
                self._delete_s3_prefix(ns, tn)
                break

    def cleanup_all(self) -> None:
        for ns, tn in self._tables_created:
            try:
                self.catalog.drop_table(f"{ns}.{tn}")
            except Exception:
                pass
            self._delete_s3_prefix(ns, tn)
        self._tables_created.clear()
        for ns in self._namespaces_created:
            try:
                self.catalog.drop_namespace(ns)
            except Exception:
                pass
        self._namespaces_created.clear()

    def clickhouse_env_variables(self) -> Dict[str, str]:
        env = {
            "AWS_ACCESS_KEY_ID": self.config.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self.config.secret_access_key,
            "AWS_DEFAULT_REGION": self.config.region,
        }
        if self.config.session_token:
            env["AWS_SESSION_TOKEN"] = self.config.session_token
        return env

    # ------------------------------------------------------------------
    # Glue-specific helpers (used by glue-only tests)
    # ------------------------------------------------------------------

    def create_clickhouse_glue_database(
        self,
        node,
        database_name: str,
        *,
        credentials_mode: str = "none",
    ) -> None:
        """Create a Glue DataLakeCatalog database.

        credentials_mode:
          "none"     -- no credentials in DDL, rely on env vars
          "settings" -- credentials as SETTINGS keys
        """
        settings = [
            "catalog_type='glue'",
            f"warehouse='{self.config.warehouse_path}'",
            f"region='{self.config.region}'",
        ]

        if credentials_mode == "settings":
            settings.append(
                f"aws_access_key_id='{self.config.access_key_id}'"
            )
            settings.append(
                f"aws_secret_access_key='{self.config.secret_access_key}'"
            )

        settings_str = ",\n    ".join(settings)
        node.query(
            f"""
DROP DATABASE IF EXISTS {database_name};
SET allow_experimental_database_glue_catalog=1;
CREATE DATABASE {database_name} ENGINE = DataLakeCatalog()
SETTINGS
    {settings_str}
"""
        )
