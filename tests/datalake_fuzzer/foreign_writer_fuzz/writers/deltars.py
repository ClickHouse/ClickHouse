"""Delta Lake tables written by delta-rs (the `deltalake` Python package) directly into MinIO."""

from ..ltypes import LType
from ..model import ID_COLUMN, arrow_schema, rows_to_arrow
from ..predicates import pred_from_json, render
from .base import Capabilities, OpRejected, Source, Writer

PREFIX = "fwf/deltars"


def _options(rng, gates):
    return {
        "checkpoint_interval": rng.choice([2, 5, 100]),
        "commits_per_write": rng.choice([1, 1, 2, 3]),
    }


def _sql_value(t, v):
    if v is None:
        return "NULL"
    if t.kind == "boolean":
        return "true" if v else "false"
    if t.kind in ("int", "long"):
        return str(v)
    if t.kind == "string":
        return "'" + v.replace("'", "''") + "'"
    raise OpRejected(f"no DataFusion literal for {t.kind}")


class DeltaRsWriter(Writer):
    name = "deltars"
    required_table_functions = ("deltaLake",)
    caps = Capabilities(
        # No `decimal`: delta-rs 0.25 writes decimal statistics through a float and cannot parse them
        # back when a value uses all digits of the precision ("parse decimal overflow"), after which every
        # operation on the table fails. No `timestamp_ntz`: delta-rs creates such tables without the
        # `timestampNtz` table feature, which delta-kernel-rs rightly rejects. Spark Delta covers both.
        primitive_kinds=(
            "boolean",
            "int",
            "long",
            "float",
            "double",
            "date",
            "timestamp",
            "string",
            "binary",
        ),
        partition_transforms=("identity",),
        partition_kinds=("boolean", "int", "long", "string", "date"),
        predicate_kinds=("boolean", "int", "long", "string"),
        update_kinds=("boolean", "int", "long", "string"),
        promotion_targets=(),
        op_weights={
            "append": 10,
            "overwrite": 2,
            "delete": 6,
            "update": 3,
            "merge": 3,
            "add_column": 3,
            "compact": 2,
            "vacuum": 1,
            "checkpoint": 1,
        },
        options=_options,
    )

    @property
    def prefix(self):
        return f"{PREFIX}/{self.table_name}"

    @property
    def uri(self):
        return f"s3://{self.ctx.bucket}/{self.prefix}"

    def _table(self):
        from deltalake import DeltaTable

        return DeltaTable(self.uri, storage_options=self.ctx.delta_rs_storage_options())

    def create(self):
        from deltalake import DeltaTable

        DeltaTable.create(
            self.uri,
            schema=arrow_schema(self.model.columns),
            partition_by=[p.column for p in self.model.partitioning] or None,
            configuration={
                "delta.checkpointInterval": str(
                    self.model.options["checkpoint_interval"]
                )
            },
            storage_options=self.ctx.delta_rs_storage_options(),
        )

    def _write(self, rows, mode):
        from deltalake import write_deltalake

        # Several commits per operation give tables with many small files and many versions.
        chunks = self.model.options["commits_per_write"] if mode == "append" else 1
        size = max(1, -(-len(rows) // chunks))
        for start in range(0, max(1, len(rows)), size):
            write_deltalake(
                self.uri,
                rows_to_arrow(self.model.columns, rows[start : start + size]),
                mode=mode,
                partition_by=[p.column for p in self.model.partitioning] or None,
                storage_options=self.ctx.delta_rs_storage_options(),
            )

    def execute(self, op, payload):
        kind = op["kind"]
        if kind == "append":
            self._write(payload["rows"], "append")
        elif kind == "overwrite":
            self._write(payload["rows"], "overwrite")
        elif kind == "delete":
            self._table().delete(render(pred_from_json(op["predicate"]), "datafusion"))
        elif kind == "update":
            column = '"' + op["column"].replace('"', '""') + '"'
            self._table().update(
                updates={column: _sql_value(payload["type"], payload["value"])},
                predicate=render(pred_from_json(op["predicate"]), "datafusion"),
            )
        elif kind == "merge":
            source = rows_to_arrow(self.model.columns, payload["rows"])
            (
                self._table()
                .merge(
                    source,
                    predicate=f'target."{ID_COLUMN}" = source."{ID_COLUMN}"',
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        elif kind == "add_column":
            from deltalake import Schema

            t = LType.from_json(op["type"])
            field = Schema.from_pyarrow(arrow_schema([[op["name"], t]])).fields
            self._table().alter.add_columns(field)
        elif kind == "compact":
            self._table().optimize.compact()
        elif kind == "vacuum":
            table = self._table()
            table.vacuum(
                retention_hours=0, dry_run=False, enforce_retention_duration=False
            )
            self.vacuumed_at_version = self._table().version()
        elif kind == "checkpoint":
            self._table().create_checkpoint()
        else:
            raise NotImplementedError(kind)

    def read(self):
        table = self._table().to_pyarrow_table()
        return table.select([n for n, _ in self.model.columns])

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
        return self._table().version()

    def live_refs(self, refs):
        floor = getattr(self, "vacuumed_at_version", None)
        return [r for r in refs if floor is None or r >= floor]

    def time_travel_settings(self, ref):
        return {"delta_lake_snapshot_version": ref}

    def cleanup(self):
        self.ctx.remove_prefix(self.prefix + "/")
