"""Iceberg tables written by Spark (`iceberg-spark-runtime`) into MinIO through a Hadoop catalog."""

from ..ltypes import PRIMITIVE_KINDS
from ..model import ID_COLUMN
from .base import Capabilities, Source, Writer
from .spark_common import (
    dataframe,
    ddl,
    now_literal,
    q,
    set_clause,
    temp_view,
    to_arrow,
    where,
)

CATALOG = "fwf_ice"
NAMESPACE = "db"
WAREHOUSE_PREFIX = "fwf/spark_iceberg"


def spark_catalog_config(ctx_bucket):
    """Spark session settings for the catalog used by this writer."""
    return {
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.type": "hadoop",
        f"spark.sql.catalog.{CATALOG}.warehouse": f"s3a://{ctx_bucket}/{WAREHOUSE_PREFIX}",
    }


def spark_transform(p):
    c = q(p.column)
    return {
        "identity": c,
        "bucket": f"bucket({p.param}, {c})",
        "truncate": f"truncate({p.param}, {c})",
        "year": f"years({c})",
        "month": f"months({c})",
        "day": f"days({c})",
        "hour": f"hours({c})",
    }[p.transform]


def _options(rng, gates):
    format_version = rng.choice([1, 2, 2, 3, 3])
    # Row-level deletes (position deletes, deletion vectors) need format version 2.
    mode = lambda: (
        rng.choice(["copy-on-write", "merge-on-read", "merge-on-read"])
        if format_version > 1
        else "copy-on-write"
    )
    return {
        "format_version": format_version,
        "delete_mode": mode(),
        "update_mode": mode(),
        "merge_mode": mode(),
        "files_per_write": rng.choice([1, 1, 2, 3]),
        "metadata_previous_versions_max": rng.choice([0, 3]),
    }


class SparkIcebergWriter(Writer):
    name = "spark_iceberg"
    caps = Capabilities(
        primitive_kinds=tuple(k for k in PRIMITIVE_KINDS if k != "uuid"),
        weird_names=True,
        partition_transforms=(
            "identity",
            "bucket",
            "truncate",
            "year",
            "month",
            "day",
            "hour",
        ),
        op_weights={
            "append": 10,
            "overwrite": 2,
            "delete": 6,
            "update": 4,
            "merge": 3,
            "add_column": 3,
            "drop_column": 2,
            "rename_column": 2,
            "move_column": 1,
            "promote_column": 2,
            "add_partition_field": 2,
            "drop_partition_field": 1,
            "compact": 2,
            "rewrite_position_deletes": 1,
            "expire_snapshots": 1,
            "set_write_mode": 1,
        },
        options=_options,
    )

    @property
    def spark(self):
        return self.ctx.spark

    @property
    def ident(self):
        return f"{CATALOG}.{NAMESPACE}.{self.table_name}"

    @property
    def prefix(self):
        return f"{WAREHOUSE_PREFIX}/{NAMESPACE}/{self.table_name}"

    def create(self):
        o = self.model.options
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        partitioned = ""
        if self.model.partitioning:
            partitioned = (
                "PARTITIONED BY ("
                + ", ".join(spark_transform(p) for p in self.model.partitioning)
                + ")"
            )
        props = {
            "format-version": str(o["format_version"]),
            "write.delete.mode": o["delete_mode"],
            "write.update.mode": o["update_mode"],
            "write.merge.mode": o["merge_mode"],
            "write.metadata.previous-versions-max": str(
                max(1, o["metadata_previous_versions_max"])
            ),
            "write.metadata.delete-after-commit.enabled": (
                "true" if o["metadata_previous_versions_max"] else "false"
            ),
        }
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        self.spark.sql(
            f"CREATE TABLE {self.ident} ({ddl(self.model.columns)}) USING iceberg {partitioned} TBLPROPERTIES ({props_sql})"
        )

    def execute(self, op, payload):
        kind = op["kind"]
        cols = self.model.columns
        sql = self.spark.sql
        if kind == "append":
            dataframe(
                self.spark, cols, payload["rows"], self.model.options["files_per_write"]
            ).writeTo(self.ident).append()
        elif kind == "overwrite":
            df = dataframe(
                self.spark, cols, payload["rows"], self.model.options["files_per_write"]
            )
            if op["dynamic"]:
                df.writeTo(self.ident).overwritePartitions()
            else:
                from pyspark.sql.functions import lit

                df.writeTo(self.ident).overwrite(lit(True))
        elif kind == "delete":
            sql(f"DELETE FROM {self.ident} WHERE {where(op['predicate'])}")
        elif kind == "update":
            sql(
                f"UPDATE {self.ident} SET {set_clause(op['column'], payload['type'], payload['value'])} WHERE {where(op['predicate'])}"
            )
        elif kind == "merge":
            view = temp_view(self.spark, cols, payload["rows"])
            sql(
                f"MERGE INTO {self.ident} t USING {view} s ON t.{q(ID_COLUMN)} = s.{q(ID_COLUMN)} "
                f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
            )
        elif kind == "add_column":
            from ..ltypes import LType

            sql(
                f"ALTER TABLE {self.ident} ADD COLUMN {q(op['name'])} {LType.from_json(op['type']).spark_ddl()}"
            )
        elif kind == "drop_column":
            sql(f"ALTER TABLE {self.ident} DROP COLUMN {q(op['name'])}")
        elif kind == "rename_column":
            sql(
                f"ALTER TABLE {self.ident} RENAME COLUMN {q(op['old'])} TO {q(op['new'])}"
            )
        elif kind == "move_column":
            position = "FIRST" if op["after"] is None else f"AFTER {q(op['after'])}"
            sql(f"ALTER TABLE {self.ident} ALTER COLUMN {q(op['name'])} {position}")
        elif kind == "promote_column":
            from ..ltypes import LType

            sql(
                f"ALTER TABLE {self.ident} ALTER COLUMN {q(op['name'])} TYPE {LType.from_json(op['to']).spark_ddl()}"
            )
        elif kind == "add_partition_field":
            from ..model import PartitionField

            sql(
                f"ALTER TABLE {self.ident} ADD PARTITION FIELD {spark_transform(PartitionField.from_json(op))}"
            )
        elif kind == "drop_partition_field":
            from ..model import PartitionField

            sql(
                f"ALTER TABLE {self.ident} DROP PARTITION FIELD {spark_transform(PartitionField.from_json(op))}"
            )
        elif kind == "compact":
            sql(
                f"CALL {CATALOG}.system.rewrite_data_files(table => '{NAMESPACE}.{self.table_name}', "
                f"options => map('min-input-files', '1', 'rewrite-all', 'true', 'delete-file-threshold', '1'))"
            )
        elif kind == "rewrite_position_deletes":
            sql(
                f"CALL {CATALOG}.system.rewrite_position_delete_files(table => '{NAMESPACE}.{self.table_name}', "
                f"options => map('rewrite-all', 'true'))"
            )
        elif kind == "expire_snapshots":
            sql(
                f"CALL {CATALOG}.system.expire_snapshots(table => '{NAMESPACE}.{self.table_name}', "
                f"older_than => {now_literal()}, retain_last => {op['retain_last']})"
            )
        elif kind == "set_write_mode":
            m = op["mode"]
            sql(
                f"ALTER TABLE {self.ident} SET TBLPROPERTIES ('write.delete.mode' = '{m}', 'write.update.mode' = '{m}', 'write.merge.mode' = '{m}')"
            )
        else:
            raise NotImplementedError(kind)

    def read(self):
        return to_arrow(self.spark.table(self.ident), self.model.columns)

    def sources(self):
        return [
            Source(
                "icebergS3",
                f"icebergS3({self.ctx.s3_source_sql(self.ctx.bucket, self.prefix)})",
                "iceberg",
                time_travel=True,
            ),
        ]

    def snapshot_ref(self):
        rows = self.spark.sql(
            f"SELECT snapshot_id FROM {self.ident}.refs WHERE name = 'main'"
        ).collect()
        return int(rows[0][0]) if rows else None

    def live_refs(self, refs):
        alive = {
            int(r[0])
            for r in self.spark.sql(
                f"SELECT snapshot_id FROM {self.ident}.snapshots"
            ).collect()
        }
        return [r for r in refs if r in alive]

    def time_travel_settings(self, ref):
        return {"iceberg_snapshot_id": ref}

    def cleanup(self):
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.ident}")
        finally:
            self.ctx.remove_prefix(self.prefix + "/")
