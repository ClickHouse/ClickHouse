"""Everything a writer adapter and the oracle need from the integration cluster."""

import io
import logging
import os
import shutil
from dataclasses import dataclass, field
from typing import Optional

from helpers.config_cluster import minio_access_key, minio_secret_key

log = logging.getLogger("foreign_writer_fuzz")

# Settings every ClickHouse query of the harness runs with.
BASE_SETTINGS = {
    "output_format_arrow_string_as_string": 0,
    "output_format_arrow_low_cardinality_as_dictionary": 0,
    "output_format_arrow_fixed_string_as_fixed_byte_array": 1,
    "allow_experimental_paimon_storage_engine": 1,
    "allow_insert_into_iceberg": 1,
    "allow_delta_lake_writes": 1,
    "allow_experimental_iceberg_compaction": 1,
    "allow_database_iceberg": 1,
}


# A query of the harness touches a handful of small files; anything slower is a hang worth reporting.
QUERY_TIMEOUT = 300
HEALTH_TIMEOUT = 10


class ClickHouseQueryError(Exception):
    pass


@dataclass
class FuzzContext:
    cluster: object
    node: object
    spark: Optional[object] = None
    rest_catalog: Optional[object] = None
    rest_catalog_database: Optional[str] = None
    gates: Optional[object] = None
    known_settings: set = field(default_factory=set)
    table_functions: set = field(default_factory=set)

    # ----------------------------------------------------------------- object storage
    @property
    def bucket(self):
        return self.cluster.minio_bucket

    @property
    def minio_endpoint_from_runner(self):
        return f"http://{self.cluster.minio_ip}:{self.cluster.minio_port}"

    @property
    def minio_endpoint_from_clickhouse(self):
        return "http://minio1:9001"

    @property
    def minio_access_key(self):
        return minio_access_key

    @property
    def minio_secret_key(self):
        return minio_secret_key

    def s3_source_sql(self, bucket, path):
        """Arguments of an S3 table function or table engine reading the table under `path` in `bucket`.

        The credentials come from the `s3` named collection (`configs/config.d/named_collections.xml`) and are never in the query text: the server logs a statement before it resolves the table function, and the secret of a table function it does not know (not compiled in, such as `deltaLake` without Rust) is not masked, https://github.com/ClickHouse/ClickHouse/issues/128372.
        """
        return f"s3, filename = '{path}/', url = '{self.minio_endpoint_from_clickhouse}/{bucket}/'"

    def s3_credentials_sql(self):
        """Positional credentials, only for `DataLakeCatalog`, which takes no named collection."""
        return f"'{minio_access_key}', '{minio_secret_key}'"

    def delta_rs_storage_options(self):
        return {
            "AWS_ENDPOINT_URL": self.minio_endpoint_from_runner,
            "AWS_ACCESS_KEY_ID": minio_access_key,
            "AWS_SECRET_ACCESS_KEY": minio_secret_key,
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_REGION": "us-east-1",
        }

    def remove_prefix(self, prefix, bucket=None):
        bucket = bucket or self.bucket
        client = self.cluster.minio_client
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            client.remove_object(bucket, obj.object_name)

    def list_prefix(self, prefix, bucket=None):
        bucket = bucket or self.bucket
        return [
            o.object_name
            for o in self.cluster.minio_client.list_objects(
                bucket, prefix=prefix, recursive=True
            )
        ]

    def mirror_to_local(self, prefix, local_dir, bucket=None):
        """Replace `local_dir` with a copy of every object under `prefix` (for foreign readers that
        read a ClickHouse-written table from the local file system)."""
        bucket = bucket or self.bucket
        shutil.rmtree(local_dir, ignore_errors=True)
        os.makedirs(local_dir, exist_ok=True)
        client = self.cluster.minio_client
        for name in self.list_prefix(prefix, bucket):
            rel = os.path.relpath(name, prefix)
            path = os.path.join(local_dir, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            client.fget_object(bucket, name, path)

    # ----------------------------------------------------------------- ClickHouse
    def settings(self, extra=None):
        merged = {
            k: v
            for k, v in BASE_SETTINGS.items()
            if not self.known_settings or k in self.known_settings
        }
        if extra:
            merged.update(extra)
        return merged

    def query(self, sql, settings=None):
        """Text result; raises `ClickHouseQueryError` with the server message."""
        try:
            return self.node.http_query(
                sql,
                method="POST",
                params=self.settings(settings),
                timeout=QUERY_TIMEOUT,
            )
        except Exception as e:
            raise ClickHouseQueryError(
                f"{e}\nQuery: {sql}\nSettings: {settings}"
            ) from None

    def query_arrow(self, sql, settings=None):
        import pyarrow as pa

        try:
            # `wait_end_of_query` buffers the result, so an exception during reading is an HTTP error with
            # the server message rather than a truncated response.
            params = self.settings(settings)
            params["wait_end_of_query"] = 1
            data = self.node.http_query(
                sql + " FORMAT ArrowStream",
                method="POST",
                params=params,
                content=True,
                timeout=QUERY_TIMEOUT,
            )
        except Exception as e:
            raise ClickHouseQueryError(
                f"{e}\nQuery: {sql}\nSettings: {settings}"
            ) from None
        if not data:
            return None
        return pa.ipc.open_stream(io.BytesIO(data)).read_all()

    def insert_arrow(self, table_sql, arrow_table, settings=None):
        import pyarrow as pa

        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)
        try:
            self.node.http_query(
                f"INSERT INTO {table_sql} FORMAT ArrowStream",
                data=sink.getvalue(),
                params=self.settings(settings),
                timeout=QUERY_TIMEOUT,
            )
        except Exception as e:
            raise ClickHouseQueryError(f"{e}\nQuery: INSERT INTO {table_sql}") from None

    def load_known_settings(self):
        self.known_settings = set(
            self.node.query("SELECT name FROM system.settings").split()
        )
        self.table_functions = set(
            self.node.query("SELECT name FROM system.table_functions").split()
        )

    def server_log_lines(self):
        return self.node.count_log_lines()

    def server_errors_since(self, line):
        """Lines of the server log after `line` which indicate a server-side bug."""
        out = self.node.exec_in_container(
            [
                "bash",
                "-c",
                f"tail -n +{line + 1} /var/log/clickhouse-server/clickhouse-server.log | grep -a -E '<Fatal>|Logical error|LOGICAL_ERROR|Sanitizer|terminate called|Received signal' | head -20 || true",
            ]
        )
        return [l for l in out.splitlines() if l.strip()]

    def server_alive(self):
        try:
            return self.node.http_query("SELECT 1", timeout=HEALTH_TIMEOUT).strip() == "1"
        except Exception:
            return False
