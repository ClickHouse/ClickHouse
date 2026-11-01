"""Delta Lake tables written by Spark (`delta-spark`) into MinIO through `s3a`."""

from ..ltypes import PRIMITIVE_KINDS, LType
from ..model import ID_COLUMN
from .base import Capabilities, Source, Writer
from .spark_common import dataframe, ddl, q, set_clause, temp_view, to_arrow, where

PREFIX = "fwf/spark_delta"


def _options(rng, gates):
    return {
        "deletion_vectors": rng.random() < 0.6,
        "column_mapping": rng.choice(["none", "name", "name", "id"]),
        "checkpoint_interval": rng.choice([2, 3, 10]),
        "type_widening": rng.random() < 0.5,
        "files_per_write": rng.choice([1, 1, 2, 3]),
    }


class SparkDeltaWriter(Writer):
    name = "spark_delta"
    required_table_functions = ("deltaLake",)
    caps = Capabilities(
        primitive_kinds=tuple(k for k in PRIMITIVE_KINDS if k != "uuid"),
        partition_transforms=("identity",),
        partition_kinds=("boolean", "int", "long", "string", "date"),
        initial_partition_fields=(0, 0, 1, 1, 2),
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
            "compact": 2,
            "vacuum": 1,
        },
        options=_options,
    )

    @property
    def spark(self):
        return self.ctx.spark

    @property
    def prefix(self):
        return f"{PREFIX}/{self.table_name}"

    @property
    def path(self):
        return f"s3a://{self.ctx.bucket}/{self.prefix}"

    @property
    def ident(self):
        return f"delta.`{self.path}`"

    def create(self):
        o = self.model.options
        partitioned = ""
        if self.model.partitioning:
            partitioned = (
                "PARTITIONED BY ("
                + ", ".join(q(p.column) for p in self.model.partitioning)
                + ")"
            )
        props = {
            "delta.enableDeletionVectors": str(o["deletion_vectors"]).lower(),
            "delta.checkpointInterval": str(o["checkpoint_interval"]),
        }
        if o["column_mapping"] != "none":
            props["delta.columnMapping.mode"] = o["column_mapping"]
        if o["type_widening"]:
            props["delta.enableTypeWidening"] = "true"
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        self.spark.sql(
            f"CREATE TABLE {self.ident} ({ddl(self.model.columns)}) USING delta {partitioned} TBLPROPERTIES ({props_sql})"
        )

    def execute(self, op, payload):
        kind = op["kind"]
        cols = self.model.columns
        sql = self.spark.sql
        files = self.model.options["files_per_write"]
        if kind == "append":
            dataframe(self.spark, cols, payload["rows"], files).write.format(
                "delta"
            ).mode("append").save(self.path)
        elif kind == "overwrite":
            writer = (
                dataframe(self.spark, cols, payload["rows"], files)
                .write.format("delta")
                .mode("overwrite")
            )
            if op["dynamic"]:
                writer = writer.option("partitionOverwriteMode", "dynamic")
            writer.save(self.path)
        elif kind == "delete":
            sql(f"DELETE FROM {self.ident} WHERE {where(op['predicate'])}")
        elif kind == "update":
            sql(
                f"UPDATE {self.ident} SET {set_clause(op['column'], payload['type'], payload['value'])} WHERE {where(op['predicate'])}"
            )
        elif kind == "merge":
            with temp_view(self.spark, cols, payload["rows"]) as view:
                sql(
                    f"MERGE INTO {self.ident} t USING {view} s ON t.{q(ID_COLUMN)} = s.{q(ID_COLUMN)} "
                    f"WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
                )
        elif kind == "add_column":
            sql(
                f"ALTER TABLE {self.ident} ADD COLUMNS ({q(op['name'])} {LType.from_json(op['type']).spark_ddl()})"
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
            sql(
                f"ALTER TABLE {self.ident} ALTER COLUMN {q(op['name'])} TYPE {LType.from_json(op['to']).spark_ddl()}"
            )
        elif kind == "compact":
            sql(f"OPTIMIZE {self.ident}")
        elif kind == "vacuum":
            sql(f"VACUUM {self.ident} RETAIN 0 HOURS")
            self.vacuumed_at_version = self.snapshot_ref()
        else:
            raise NotImplementedError(kind)

    def read(self):
        return to_arrow(
            self.spark.read.format("delta").load(self.path), self.model.columns
        )

    def sources(self):
        return [
            Source(
                "deltaLake",
                f"deltaLake({self.ctx.s3_source_sql(self.ctx.bucket, self.prefix)})",
                "delta",
                time_travel=True,
            )
        ]

    def snapshot_ref(self):
        return int(
            self.spark.sql(f"DESCRIBE HISTORY {self.ident} LIMIT 1").collect()[0][
                "version"
            ]
        )

    def live_refs(self, refs):
        floor = getattr(self, "vacuumed_at_version", None)
        return [r for r in refs if floor is None or r >= floor]

    def time_travel_settings(self, ref):
        return {"delta_lake_snapshot_version": ref}

    def trivial_count_reliable(self):
        return not (
            self.model.options["deletion_vectors"]
            and self.ctx.gates.active("delta_trivial_count_with_deletion_vectors")
        )

    def cleanup(self):
        self.ctx.remove_prefix(self.prefix + "/")
