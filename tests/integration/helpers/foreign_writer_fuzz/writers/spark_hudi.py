"""Apache Hudi tables written by Spark (`hudi-spark-bundle`) into MinIO through `s3a`."""

from ..model import ID_COLUMN
from .base import Capabilities, Source, Writer
from .spark_common import dataframe, q, to_arrow, where

PREFIX = "fwf/hudi"


def _options(rng, gates):
    table_type = "COPY_ON_WRITE"
    if not gates.active("hudi_merge_on_read") and rng.random() < 0.5:
        table_type = "MERGE_ON_READ"
    return {"table_type": table_type, "files_per_write": rng.choice([1, 2])}


class SparkHudiWriter(Writer):
    name = "spark_hudi"
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
            "binary",
        ),
        nested_kinds=("list", "struct"),
        max_nesting_depth=1,
        partition_transforms=("identity",),
        partition_kinds=("int", "long", "string"),
        partition_on_id=False,
        initial_partition_fields=(0, 0, 1),
        update_kinds=("boolean", "int", "long", "string", "date", "decimal"),
        op_weights={"append": 10, "overwrite": 2, "delete": 6, "update": 4, "merge": 4},
        options=_options,
    )

    @staticmethod
    def adjust_caps(caps, gates):
        import dataclasses

        if not gates.active("hudi_replacecommit"):
            return caps
        return dataclasses.replace(
            caps,
            op_weights={k: v for k, v in caps.op_weights.items() if k != "overwrite"},
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

    def _hudi_options(self, operation):
        partition = self.model.partitioning[0].column if self.model.partitioning else ""
        options = {
            "hoodie.table.name": self.table_name,
            "hoodie.datasource.write.table.name": self.table_name,
            "hoodie.datasource.write.table.type": self.model.options["table_type"],
            "hoodie.datasource.write.operation": operation,
            "hoodie.datasource.write.recordkey.field": ID_COLUMN,
            "hoodie.datasource.write.precombine.field": ID_COLUMN,
            "hoodie.datasource.write.partitionpath.field": partition,
            "hoodie.datasource.write.keygenerator.class": (
                "org.apache.hudi.keygen.SimpleKeyGenerator"
                if partition
                else "org.apache.hudi.keygen.NonpartitionedKeyGenerator"
            ),
            "hoodie.datasource.write.hive_style_partitioning": "true",
            "hoodie.metadata.enable": "false",
            "hoodie.compact.inline": "false",
            "hoodie.parquet.compression.codec": "snappy",
        }
        return options

    def _save(self, df, operation, mode="append"):
        df.write.format("hudi").options(**self._hudi_options(operation)).mode(
            mode
        ).save(self.path)

    def _current(self):
        return self.spark.read.format("hudi").load(self.path)

    def create(self):
        # Hudi creates the table on the first write.
        self.created = False

    def execute(self, op, payload):
        kind = op["kind"]
        cols = self.model.columns
        files = self.model.options["files_per_write"]
        if kind == "append":
            self._save(
                dataframe(self.spark, cols, payload["rows"], files),
                "insert",
                "append" if self.created else "overwrite",
            )
            self.created = True
        elif kind == "overwrite":
            self._save(
                dataframe(self.spark, cols, payload["rows"], files),
                "insert_overwrite_table" if self.created else "insert",
                "append" if self.created else "overwrite",
            )
            self.created = True
        elif kind == "merge":
            self._save(
                dataframe(self.spark, cols, payload["rows"], files),
                "upsert",
                "append" if self.created else "overwrite",
            )
            self.created = True
        elif kind == "delete":
            doomed = (
                self._current()
                .where(where(op["predicate"]))
                .select(*[q(n) for n, _ in cols])
            )
            if doomed.count():
                self._save(doomed, "delete")
        elif kind == "update":
            from pyspark.sql.functions import expr

            from ..predicates import spark_literal

            matched = (
                self._current()
                .where(where(op["predicate"]))
                .select(*[q(n) for n, _ in cols])
            )
            if matched.count():
                updated = matched.withColumn(
                    op["column"], expr(spark_literal(payload["type"], payload["value"]))
                )
                self._save(updated, "upsert")
        else:
            raise NotImplementedError(kind)

    def readable(self):
        return self.created

    def read(self):
        return to_arrow(self._current(), self.model.columns)

    def sources(self):
        return [
            Source(
                "hudi",
                f"hudi({self.ctx.s3_source_sql(self.ctx.bucket, self.prefix)})",
                "hudi",
            )
        ]

    def cleanup(self):
        self.ctx.remove_prefix(self.prefix + "/")
