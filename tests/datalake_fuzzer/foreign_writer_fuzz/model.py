"""The table model: what the harness knows about a table independently of any engine.

The model tracks the logical schema (column names and types, in order), the partitioning and the
next free row id. It does not track row contents: the expected rows of every step come from the
writer's own read of its snapshot (the differential oracle), so the harness never has to emulate
the semantics of `MERGE`, partition overwrite, deletion vectors or schema evolution itself.
"""

import copy
import random
from dataclasses import dataclass, field
from typing import List, Optional

from .ltypes import LType

ID_COLUMN = "id"


@dataclass
class PartitionField:
    transform: str  # identity, bucket, truncate, year, month, day, hour
    column: str
    param: int = 0

    def to_json(self):
        return {"transform": self.transform, "column": self.column, "param": self.param}

    @staticmethod
    def from_json(j):
        return PartitionField(j["transform"], j["column"], j.get("param", 0))

    def describe(self):
        if self.transform in ("bucket", "truncate"):
            return f"{self.transform}[{self.param}]({self.column})"
        return f"{self.transform}({self.column})"


@dataclass
class TableModel:
    columns: List[List] = field(default_factory=list)  # [name, LType]
    partitioning: List[PartitionField] = field(default_factory=list)
    options: dict = field(default_factory=dict)
    next_id: int = 0
    # Names of dropped columns, which an ADD may reuse.
    dropped: List[str] = field(default_factory=list)

    def copy(self):
        return copy.deepcopy(self)

    # ----------------------------------------------------------------- columns
    def names(self):
        return [n for n, _ in self.columns]

    def column_type(self, name) -> Optional[LType]:
        for n, t in self.columns:
            if n == name:
                return t
        return None

    def has(self, name):
        return self.column_type(name) is not None

    def index(self, name):
        return self.names().index(name)

    def non_id_columns(self):
        return [(n, t) for n, t in self.columns if n != ID_COLUMN]

    def partition_source_columns(self):
        return {p.column for p in self.partitioning}

    # ----------------------------------------------------------------- rows
    def generate_rows(self, n, seed, ids=None, null_probability=0.15):
        """`n` rows for the current schema from a dedicated RNG, so a replayed history reproduces
        them exactly. Fresh ids are allocated unless `ids` is given."""
        rng = random.Random(seed)
        if ids is None:
            ids = list(range(self.next_id, self.next_id + n))
            self.next_id += n
        rows = []
        for row_id in ids:
            row = {}
            for name, t in self.columns:
                row[name] = (
                    row_id
                    if name == ID_COLUMN
                    else t.random_value(rng, null_probability)
                )
            rows.append(row)
        return rows

    # ----------------------------------------------------------------- serialization
    def to_json(self):
        return {
            "columns": [[n, t.to_json()] for n, t in self.columns],
            "partitioning": [p.to_json() for p in self.partitioning],
            "options": self.options,
        }

    @staticmethod
    def from_json(j):
        return TableModel(
            columns=[[n, LType.from_json(t)] for n, t in j["columns"]],
            partitioning=[PartitionField.from_json(p) for p in j["partitioning"]],
            options=dict(j["options"]),
        )

    def describe(self):
        cols = ", ".join(f"{n} {t.describe()}" for n, t in self.columns)
        parts = ", ".join(p.describe() for p in self.partitioning) or "unpartitioned"
        return f"({cols}) partitioned by [{parts}] options {self.options}"


def arrow_schema(columns, storage=False):
    import pyarrow as pa

    return pa.schema(
        [pa.field(n, t.arrow(storage), nullable=(n != ID_COLUMN)) for n, t in columns]
    )


def rows_to_arrow(columns, rows):
    import pyarrow as pa

    data = {n: [t.to_arrow_value(r[n]) for r in rows] for n, t in columns}
    return pa.Table.from_pydict(data, schema=arrow_schema(columns, storage=True)).cast(
        arrow_schema(columns)
    )
