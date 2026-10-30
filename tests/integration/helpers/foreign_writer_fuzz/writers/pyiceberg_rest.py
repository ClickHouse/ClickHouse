"""Iceberg tables written by PyIceberg through the Iceberg REST catalog (`tabulario/iceberg-rest`).

ClickHouse reads them three ways: the `icebergS3` table function on the table location, and a
`DataLakeCatalog` database attached to the same REST catalog.
"""

import datetime
import itertools
import re

from ..ltypes import LType
from ..model import ID_COLUMN, PartitionField, rows_to_arrow
from ..predicates import pred_from_json, to_pyiceberg
from .base import Capabilities, OpRejected, Source, Writer

NAMESPACE = "fwf"
REST_BUCKET = "warehouse-rest"


def iceberg_type(t, ids):
    from pyiceberg import types as it

    k = t.kind
    simple = {
        "boolean": it.BooleanType,
        "int": it.IntegerType,
        "long": it.LongType,
        "float": it.FloatType,
        "double": it.DoubleType,
        "date": it.DateType,
        "timestamp": it.TimestamptzType,
        "timestamp_ntz": it.TimestampType,
        "string": it.StringType,
        "binary": it.BinaryType,
        "uuid": it.UUIDType,
    }
    if k in simple:
        return simple[k]()
    if k == "decimal":
        return it.DecimalType(t.precision, t.scale)
    if k == "list":
        return it.ListType(
            next(ids), iceberg_type(t.element, ids), element_required=False
        )
    if k == "map":
        return it.MapType(
            next(ids),
            iceberg_type(t.key, ids),
            next(ids),
            iceberg_type(t.value, ids),
            value_required=False,
        )
    if k == "struct":
        return it.StructType(
            *[
                it.NestedField(next(ids), n, iceberg_type(ft, ids), required=False)
                for n, ft in t.fields
            ]
        )
    raise ValueError(k)


def iceberg_schema(columns):
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    ids = itertools.count(1)
    fields = []
    for n, t in columns:
        field_id = next(ids)
        fields.append(NestedField(field_id, n, iceberg_type(t, ids), required=False))
    return Schema(*fields)


def iceberg_transform(p):
    from pyiceberg import transforms as tr

    return {
        "identity": lambda: tr.IdentityTransform(),
        "bucket": lambda: tr.BucketTransform(p.param),
        "truncate": lambda: tr.TruncateTransform(p.param),
        "year": lambda: tr.YearTransform(),
        "month": lambda: tr.MonthTransform(),
        "day": lambda: tr.DayTransform(),
        "hour": lambda: tr.HourTransform(),
    }[p.transform]()


def partition_field_name(p):
    return "p_" + re.sub(
        r"[^A-Za-z0-9]", "_", f"{p.column}_{p.transform}{p.param or ''}"
    )


def _options(rng, gates):
    return {"format_version": rng.choice([1, 2, 2])}


class PyIcebergRestWriter(Writer):
    name = "pyiceberg_rest"
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
            "uuid",
        ),
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
        partition_kinds=(
            "boolean",
            "int",
            "long",
            "string",
            "date",
            "decimal",
            "timestamp",
            "timestamp_ntz",
        ),
        predicate_kinds=("boolean", "int", "long", "string", "date"),
        update_kinds=("boolean", "int", "long", "string", "date", "decimal"),
        op_weights={
            "append": 10,
            "overwrite": 2,
            "delete": 5,
            "update": 3,
            "merge": 3,
            "add_column": 3,
            "drop_column": 2,
            "rename_column": 2,
            "move_column": 1,
            "promote_column": 2,
            "add_partition_field": 2,
            "drop_partition_field": 1,
            "expire_snapshots": 1,
        },
        options=_options,
    )

    @property
    def catalog(self):
        return self.ctx.rest_catalog

    @property
    def identifier(self):
        return f"{NAMESPACE}.{self.table_name}"

    @property
    def location(self):
        return f"s3://{REST_BUCKET}/{NAMESPACE}/{self.table_name}"

    def _table(self):
        return self.catalog.load_table(self.identifier)

    def create(self):
        try:
            self.catalog.create_namespace(NAMESPACE)
        except Exception as e:
            if (
                "already exists" not in str(e).lower()
                and "AlreadyExists" not in type(e).__name__
            ):
                raise
        table = self.catalog.create_table(
            self.identifier,
            schema=iceberg_schema(self.model.columns),
            location=self.location,
            properties={"format-version": str(self.model.options["format_version"])},
        )
        if self.model.partitioning:
            with table.update_spec() as spec:
                for p in self.model.partitioning:
                    spec.add_field(
                        p.column, iceberg_transform(p), partition_field_name(p)
                    )

    def execute(self, op, payload):
        kind = op["kind"]
        table = self._table()
        arrow = lambda rows: rows_to_arrow(self.model.columns, rows).cast(
            table.schema().as_arrow()
        )
        if kind == "append":
            table.append(arrow(payload["rows"]))
        elif kind == "overwrite":
            if op["dynamic"]:
                table.dynamic_partition_overwrite(arrow(payload["rows"]))
            else:
                table.overwrite(arrow(payload["rows"]))
        elif kind == "delete":
            table.delete(to_pyiceberg(pred_from_json(op["predicate"])))
        elif kind == "update":
            matched = table.scan(
                row_filter=to_pyiceberg(pred_from_json(op["predicate"]))
            ).to_arrow()
            if matched.num_rows == 0:
                return
            import pyarrow as pa

            t = payload["type"]
            index = matched.schema.get_field_index(op["column"])
            column = pa.array(
                [t.to_arrow_value(payload["value"])] * matched.num_rows,
                type=matched.schema.field(index).type,
            )
            table.upsert(
                matched.set_column(index, matched.schema.field(index), column),
                join_cols=[ID_COLUMN],
            )
        elif kind == "merge":
            if not payload["rows"]:
                return
            table.upsert(arrow(payload["rows"]), join_cols=[ID_COLUMN])
        elif kind == "add_column":
            with table.update_schema() as update:
                update.add_column(
                    op["name"],
                    iceberg_type(LType.from_json(op["type"]), itertools.count(10_000)),
                    required=False,
                )
        elif kind == "drop_column":
            with table.update_schema() as update:
                update.delete_column(op["name"])
        elif kind == "rename_column":
            with table.update_schema() as update:
                update.rename_column(op["old"], op["new"])
        elif kind == "move_column":
            with table.update_schema() as update:
                if op["after"] is None:
                    update.move_first(op["name"])
                else:
                    update.move_after(op["name"], op["after"])
        elif kind == "promote_column":
            with table.update_schema() as update:
                update.update_column(
                    op["name"],
                    field_type=iceberg_type(
                        LType.from_json(op["to"]), itertools.count(10_000)
                    ),
                )
        elif kind == "add_partition_field":
            p = PartitionField.from_json(op)
            with table.update_spec() as spec:
                spec.add_field(p.column, iceberg_transform(p), partition_field_name(p))
        elif kind == "drop_partition_field":
            p = PartitionField.from_json(op)
            names = [f.name for f in table.spec().fields]
            # The field name follows the source column through renames only by id; look it up by transform.
            source_id = table.schema().find_field(p.column).field_id
            candidates = [
                f.name
                for f in table.spec().fields
                if f.source_id == source_id
                and str(f.transform) == str(iceberg_transform(p))
            ]
            if not candidates:
                raise OpRejected(f"no partition field for {p.describe()} in {names}")
            with table.update_spec() as spec:
                spec.remove_field(candidates[0])
        elif kind == "expire_snapshots":
            table.maintenance.expire_snapshots().older_than(
                datetime.datetime.now(datetime.timezone.utc)
            ).commit()
        else:
            raise NotImplementedError(kind)

    def read(self):
        import pyarrow as pa

        from ..model import arrow_schema

        scan = self._table().scan()
        try:
            return scan.to_arrow().select([n for n, _ in self.model.columns])
        except pa.ArrowNotImplementedError:
            # PyIceberg cannot build an empty result with a UUID column (`Schema.empty_table` of an
            # extension type); the table is empty then.
            if any(True for _ in scan.plan_files()):
                raise
            return arrow_schema(self.model.columns, storage=True).empty_table()

    def sources(self):
        url = f"{self.ctx.minio_endpoint_from_clickhouse}/{REST_BUCKET}/{NAMESPACE}/{self.table_name}/"
        result = [
            Source(
                "icebergS3",
                f"icebergS3('{url}', {self.ctx.s3_credentials_sql()})",
                "iceberg",
                time_travel=True,
            )
        ]
        if self.ctx.rest_catalog_database:
            result.append(
                Source(
                    "DataLakeCatalog",
                    f"{self.ctx.rest_catalog_database}.`{self.identifier}`",
                    "iceberg",
                    time_travel=True,
                )
            )
        return result

    def snapshot_ref(self):
        snapshot = self._table().current_snapshot()
        return snapshot.snapshot_id if snapshot else None

    def live_refs(self, refs):
        alive = {s.snapshot_id for s in self._table().snapshots()}
        return [r for r in refs if r in alive]

    def time_travel_settings(self, ref):
        return {"iceberg_snapshot_id": ref}

    def cleanup(self):
        try:
            self.catalog.drop_table(self.identifier)
        finally:
            self.ctx.remove_prefix(
                f"{NAMESPACE}/{self.table_name}/", bucket=REST_BUCKET
            )
