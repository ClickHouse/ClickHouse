"""ClickHouse as the writer, foreign tools as the readers.

`ClickHouseIcebergWriter` creates an `IcebergS3` table and mutates it with `INSERT`, `ALTER ...
DELETE/UPDATE`, `ALTER ... ADD/DROP/RENAME/MODIFY COLUMN` and `OPTIMIZE`; PyIceberg and Spark read
the result. `ClickHouseDeltaWriter` inserts into a `DeltaLake` table created by delta-rs; delta-rs and
Spark read the result. Any disagreement, and any table a foreign reader cannot open, is a failure.
"""

import os
import re

from ..ltypes import LType
from ..model import ID_COLUMN, arrow_schema, rows_to_arrow
from ..predicates import clickhouse_literal, pred_from_json, render
from .base import Capabilities, ForeignReadError, Source, Writer
from .spark_common import q

ICEBERG_PREFIX = "var/lib/clickhouse/user_files/fwf_ch_iceberg"
DELTA_PREFIX = "fwf/ch_delta"


def _ch_columns(columns):
    return ", ".join(
        f"{q(n)} {t.clickhouse(nullable=(n != ID_COLUMN))}" for n, t in columns
    )


def _ch_partition(p):
    c = q(p.column)
    return {
        "identity": f"identity({c})",
        "bucket": f"icebergBucket({p.param}, {c})",
        "truncate": f"icebergTruncate({p.param}, {c})",
        "year": f"icebergYear({c})",
        "month": f"icebergMonth({c})",
        "day": f"icebergDay({c})",
        "hour": f"icebergHour({c})",
    }[p.transform]


def _iceberg_kinds(gates):
    kinds = ["int", "long", "float", "double", "decimal", "date", "timestamp", "string"]
    if not gates.active("clickhouse_iceberg_bool"):
        kinds.append("boolean")
    if not gates.active("clickhouse_iceberg_timestamp_ntz"):
        kinds.append("timestamp_ntz")
    return tuple(kinds)


def _iceberg_options(rng, gates):
    versions = [2] if gates.active("clickhouse_iceberg_v1_metadata") else [1, 2, 2]
    return {"format_version": rng.choice(versions)}


def _latest_metadata(metadata_dir):
    def version(name):
        m = re.match(r"v?(\d+)", name)
        return int(m.group(1)) if m else -1

    names = [n for n in os.listdir(metadata_dir) if n.endswith(".metadata.json")]
    if not names:
        raise ForeignReadError(
            f"no metadata file in {metadata_dir}: {os.listdir(metadata_dir)}"
        )
    return os.path.join(metadata_dir, max(names, key=version))


class ClickHouseIcebergWriter(Writer):
    name = "clickhouse_iceberg"
    clickhouse_writes = True
    caps = Capabilities(
        # `adjust_caps` replaces this according to the known-issue gates.
        primitive_kinds=(
            "int",
            "long",
            "float",
            "double",
            "decimal",
            "date",
            "timestamp",
            "string",
        ),
        max_nesting_depth=1,
        partition_transforms=(
            "identity",
            "bucket",
            "truncate",
            "year",
            "month",
            "day",
            "hour",
        ),
        partition_kinds=("int", "long", "string", "date", "timestamp", "timestamp_ntz"),
        predicate_kinds=(
            "boolean",
            "int",
            "long",
            "decimal",
            "date",
            "timestamp",
            "timestamp_ntz",
            "string",
        ),
        update_kinds=("boolean", "int", "long", "decimal", "date", "string"),
        promotion_targets=("long", "double", "decimal"),
        op_weights={
            "append": 10,
            "delete": 5,
            "update": 3,
            "add_column": 3,
            "drop_column": 2,
            "rename_column": 2,
            "promote_column": 2,
            "compact": 1,
        },
        options=_iceberg_options,
    )

    @staticmethod
    def adjust_caps(caps, gates):
        import dataclasses

        return dataclasses.replace(caps, primitive_kinds=_iceberg_kinds(gates))

    @property
    def ch_table(self):
        return f"fwf_{self.table_name}"

    @property
    def prefix(self):
        return f"{ICEBERG_PREFIX}/{self.table_name}"

    @property
    def local_dir(self):
        return "/" + self.prefix

    def create(self):
        partition = ""
        if self.model.partitioning:
            partition = (
                "PARTITION BY ("
                + ", ".join(_ch_partition(p) for p in self.model.partitioning)
                + ")"
            )
        self.ctx.query(
            f"CREATE TABLE {self.ch_table} ({_ch_columns(self.model.columns)}) "
            f"ENGINE = IcebergS3(s3, filename = '{self.prefix}/', format = Parquet, url = '{self.ctx.minio_endpoint_from_clickhouse}/{self.ctx.bucket}/') "
            f"{partition} SETTINGS iceberg_format_version = {self.model.options['format_version']}"
        )

    def execute(self, op, payload):
        kind = op["kind"]
        query = self.ctx.query
        if kind == "append":
            self.ctx.insert_arrow(
                self.ch_table, rows_to_arrow(self.model.columns, payload["rows"])
            )
        elif kind == "delete":
            query(
                f"ALTER TABLE {self.ch_table} DELETE WHERE {render(pred_from_json(op['predicate']), 'clickhouse')}"
            )
        elif kind == "update":
            value = clickhouse_literal(payload["type"], payload["value"])
            query(
                f"ALTER TABLE {self.ch_table} UPDATE {q(op['column'])} = {value} WHERE {render(pred_from_json(op['predicate']), 'clickhouse')}"
            )
        elif kind == "add_column":
            query(
                f"ALTER TABLE {self.ch_table} ADD COLUMN {q(op['name'])} {LType.from_json(op['type']).clickhouse()}"
            )
        elif kind == "drop_column":
            query(f"ALTER TABLE {self.ch_table} DROP COLUMN {q(op['name'])}")
        elif kind == "rename_column":
            query(
                f"ALTER TABLE {self.ch_table} RENAME COLUMN {q(op['old'])} TO {q(op['new'])}"
            )
        elif kind == "promote_column":
            query(
                f"ALTER TABLE {self.ch_table} MODIFY COLUMN {q(op['name'])} {LType.from_json(op['to']).clickhouse()}"
            )
        elif kind == "compact":
            query(f"OPTIMIZE TABLE {self.ch_table}")
        else:
            raise NotImplementedError(kind)

    def read(self):
        self.ctx.mirror_to_local(self.prefix + "/", self.local_dir)
        metadata = _latest_metadata(os.path.join(self.local_dir, "metadata"))
        names = [n for n, _ in self.model.columns]
        result = {}
        try:
            from pyiceberg.table import StaticTable

            result["pyiceberg"] = (
                StaticTable.from_metadata(metadata).scan().to_arrow().select(names)
            )
        except Exception as e:
            raise ForeignReadError(
                f"PyIceberg cannot read the ClickHouse-written table {metadata}: {type(e).__name__}: {e}"
            ) from None
        if self.ctx.spark is not None:
            from .spark_common import to_arrow

            try:
                result["spark"] = to_arrow(
                    self.ctx.spark.read.format("iceberg").load(metadata),
                    self.model.columns,
                )
            except Exception as e:
                raise ForeignReadError(
                    f"Spark cannot read the ClickHouse-written table {metadata}: {type(e).__name__}: {str(e)[:2000]}"
                ) from None
        return result

    def sources(self):
        return [Source("IcebergS3 engine", self.ch_table, "iceberg")]

    def cleanup(self):
        try:
            self.ctx.query(f"DROP TABLE IF EXISTS {self.ch_table} SYNC")
        finally:
            self.ctx.remove_prefix(self.prefix + "/")


def _delta_options(rng, gates):
    return {}


class ClickHouseDeltaWriter(Writer):
    name = "clickhouse_delta"
    required_table_functions = ("deltaLake",)
    clickhouse_writes = True
    caps = Capabilities(
        primitive_kinds=(
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "decimal",
            "date",
            "timestamp",
            "string",
        ),
        max_nesting_depth=1,
        partition_transforms=("identity",),
        partition_kinds=("int", "long", "string", "date"),
        partition_on_id=False,
        op_weights={"append": 10},
        options=_delta_options,
    )

    @property
    def ch_table(self):
        return f"fwf_{self.table_name}"

    @property
    def prefix(self):
        return f"{DELTA_PREFIX}/{self.table_name}"

    @property
    def uri(self):
        return f"s3://{self.ctx.bucket}/{self.prefix}"

    def create(self):
        from deltalake import DeltaTable

        DeltaTable.create(
            self.uri,
            schema=arrow_schema(self.model.columns),
            partition_by=[p.column for p in self.model.partitioning] or None,
            storage_options=self.ctx.delta_rs_storage_options(),
        )
        self.ctx.query(
            f"CREATE TABLE {self.ch_table} ENGINE = DeltaLake({self.ctx.s3_source_sql(self.ctx.bucket, self.prefix)})"
        )

    def execute(self, op, payload):
        if op["kind"] != "append":
            raise NotImplementedError(op["kind"])
        self.ctx.insert_arrow(
            self.ch_table, rows_to_arrow(self.model.columns, payload["rows"])
        )

    def read(self):
        from deltalake import DeltaTable

        names = [n for n, _ in self.model.columns]
        result = {}
        try:
            result["deltars"] = (
                DeltaTable(
                    self.uri, storage_options=self.ctx.delta_rs_storage_options()
                )
                .to_pyarrow_table()
                .select(names)
            )
        except Exception as e:
            raise ForeignReadError(
                f"delta-rs cannot read the ClickHouse-written table: {type(e).__name__}: {e}"
            ) from None
        if self.ctx.spark is not None:
            from .spark_common import to_arrow

            try:
                result["spark"] = to_arrow(
                    self.ctx.spark.read.format("delta").load(
                        f"s3a://{self.ctx.bucket}/{self.prefix}"
                    ),
                    self.model.columns,
                )
            except Exception as e:
                raise ForeignReadError(
                    f"Spark cannot read the ClickHouse-written table: {type(e).__name__}: {str(e)[:2000]}"
                ) from None
        return result

    def sources(self):
        return [Source("DeltaLake engine", self.ch_table, "delta")]

    def cleanup(self):
        try:
            self.ctx.query(f"DROP TABLE IF EXISTS {self.ch_table} SYNC")
        finally:
            self.ctx.remove_prefix(self.prefix + "/")
