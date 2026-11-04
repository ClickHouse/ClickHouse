"""Apache Paimon tables written by Spark (`paimon-spark`).

Paimon's own file IO has no `s3a` support in the runner image, so like the other Paimon suites the
table is written to the local warehouse of `helpers.paimon_utils` and uploaded to MinIO in commit
order after every operation (`upload_paimon_table`).
"""

from helpers import paimon_utils

from ..ltypes import LType
from ..model import ID_COLUMN
from .base import Capabilities, Source, Writer
from .spark_common import ddl, q, set_clause, temp_view, to_arrow, where

CATALOG = paimon_utils.PAIMON_CATALOG


def spark_catalog_config():
    return {
        f"spark.sql.catalog.{CATALOG}": "org.apache.paimon.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.warehouse": f"file:{paimon_utils.PAIMON_WAREHOUSE}",
    }


def _options(rng, gates):
    primary_key = rng.random() < 0.6
    return {
        "primary_key": primary_key,
        "bucket": rng.choice([1, 2, 4]) if primary_key else -1,
        "deletion_vectors": primary_key and rng.random() < 0.4,
        "merge_engine": (
            rng.choice(["deduplicate", "deduplicate", "partial-update"])
            if primary_key
            else None
        ),
    }


class SparkPaimonWriter(Writer):
    name = "spark_paimon"
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
            "timestamp_ntz",
            "string",
            "binary",
        ),
        partition_transforms=("identity",),
        partition_kinds=("int", "long", "string", "date"),
        partition_on_id=False,
        initial_partition_fields=(0, 0, 1),
        rename_partition_sources=False,
        op_weights={
            "append": 10,
            "overwrite": 2,
            "delete": 4,
            "update": 3,
            "merge": 3,
            "add_column": 3,
            "drop_column": 2,
            "rename_column": 2,
            "move_column": 1,
            "promote_column": 2,
            "compact": 2,
            "expire_snapshots": 1,
        },
        options=_options,
    )

    @staticmethod
    def adjust_caps(caps, gates):
        import dataclasses

        if gates.active("paimon_date_before_epoch"):
            caps = dataclasses.replace(
                caps,
                primitive_kinds=tuple(k for k in caps.primitive_kinds if k != "date"),
                partition_kinds=tuple(k for k in caps.partition_kinds if k != "date"),
            )
        if gates.active("paimon_blank_string_partition"):
            caps = dataclasses.replace(
                caps,
                partition_kinds=tuple(k for k in caps.partition_kinds if k != "string"),
            )
        if gates.active("paimon_row_type"):
            caps = dataclasses.replace(
                caps, nested_kinds=tuple(k for k in caps.nested_kinds if k != "struct")
            )
        if gates.active("paimon_columns_by_name"):
            caps = dataclasses.replace(
                caps,
                op_weights={
                    k: v
                    for k, v in caps.op_weights.items()
                    if k not in ("rename_column", "drop_column")
                },
            )
        return caps

    @property
    def spark(self):
        return self.ctx.spark

    @property
    def ident(self):
        return paimon_utils.spark_table(self.table_name)

    SCHEMA_OPS = (
        "add_column",
        "drop_column",
        "rename_column",
        "move_column",
        "promote_column",
    )

    def _snapshot_count(self):
        return self.spark.sql(
            f"SELECT count(*) FROM {CATALOG}.default.`{self.table_name}$snapshots`"
        ).collect()[0][0]

    def checkable(self):
        return not getattr(self, "schema_pending", False)

    def _upload(self):
        paimon_utils.upload_paimon_table(self.ctx.cluster, "s3", self.table_name)

    def create(self):
        o = self.model.options
        props = {"file.format": "parquet"}
        not_null = ()
        if o["primary_key"]:
            keys = [ID_COLUMN] + [p.column for p in self.model.partitioning]
            props["primary-key"] = ",".join(keys)
            props["bucket"] = str(o["bucket"])
            props["merge-engine"] = o["merge_engine"]
            not_null = tuple(keys)
        if o["deletion_vectors"]:
            props["deletion-vectors.enabled"] = "true"
        partitioned = ""
        if self.model.partitioning:
            partitioned = (
                "PARTITIONED BY ("
                + ", ".join(q(p.column) for p in self.model.partitioning)
                + ")"
            )
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.default")
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in props.items())
        self.spark.sql(
            f"CREATE TABLE {self.ident} ({ddl(self.model.columns, not_null)}) {partitioned} TBLPROPERTIES ({props_sql})"
        )

    def execute(self, op, payload):
        snapshots_before = self._snapshot_count()
        self._execute(op, payload)
        if op["kind"] in self.SCHEMA_OPS and self.ctx.gates.active(
            "paimon_schema_change_without_snapshot"
        ):
            self.schema_pending = True
        elif self._snapshot_count() > snapshots_before:
            self.schema_pending = False

    def _execute(self, op, payload):
        kind = op["kind"]
        cols = self.model.columns
        sql = self.spark.sql
        names = ", ".join(q(n) for n, _ in cols)
        if kind in ("append", "overwrite"):
            verb = "INTO" if kind == "append" else "OVERWRITE"
            with temp_view(self.spark, cols, payload["rows"]) as view:
                sql(f"INSERT {verb} {self.ident} ({names}) SELECT {names} FROM {view}")
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
            sql(f"CALL {CATALOG}.sys.compact(table => 'default.{self.table_name}')")
        elif kind == "expire_snapshots":
            sql(
                f"CALL {CATALOG}.sys.expire_snapshots(table => 'default.{self.table_name}', retain_max => {op['retain_last']}, retain_min => 1)"
            )
        else:
            raise NotImplementedError(kind)
        self._upload()

    def read(self):
        return to_arrow(self.spark.table(self.ident), self.model.columns)

    def sources(self):
        expression = paimon_utils.get_creation_expression(
            "s3", self.table_name, self.ctx.cluster, table_function=True
        )
        return [Source("paimonS3", expression, "paimon")]

    def cleanup(self):
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.ident}")
        finally:
            # The uploader names objects by their absolute local path; MinIO drops the leading slash.
            self.ctx.remove_prefix(
                "var/lib/clickhouse/user_files"
                + paimon_utils.paimon_table_dir(self.table_name)
            )
