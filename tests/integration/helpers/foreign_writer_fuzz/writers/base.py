"""Writer adapter interface.

A writer adapter owns one table in one table format. It executes the operations of a history with
its own engine (Spark, PyIceberg, delta-rs, or ClickHouse itself), reads the table back with its
own reader (the reference for the oracle), and names the ClickHouse sources that must return the
same data: table functions, table engines, `DataLakeCatalog` databases.

When ClickHouse is the writer the roles flip: `execute` runs ClickHouse SQL and `read` uses a
foreign reader, so the same oracle checks that other tools can read what ClickHouse wrote.
"""

import random
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

from ..ltypes import PREDICATE_KINDS


class OpRejected(Exception):
    """The writer refused the operation (unsupported by this tool or this table configuration).
    The table is unchanged and the history continues without the operation."""


class ForeignReadError(Exception):
    """The foreign reader could not read the table. A failure when ClickHouse wrote the table."""


@dataclass
class Capabilities:
    primitive_kinds: Tuple[str, ...]
    nested_kinds: Tuple[str, ...] = ("list", "map", "struct")
    max_nesting_depth: int = 2
    initial_columns: int = 6
    max_columns: int = 14
    max_decimal_precision: int = 38
    add_nested: bool = True
    weird_names: bool = False
    predicate_kinds: Tuple[str, ...] = PREDICATE_KINDS
    update_kinds: Tuple[str, ...] = PREDICATE_KINDS
    promotion_targets: Tuple[str, ...] = ("long", "double", "decimal")
    partition_transforms: Tuple[str, ...] = ()
    partition_kinds: Tuple[str, ...] = (
        "boolean",
        "int",
        "long",
        "string",
        "date",
        "decimal",
        "timestamp",
        "timestamp_ntz",
        "binary",
        "uuid",
    )
    partition_on_id: bool = True
    initial_partition_fields: Tuple[int, ...] = (0, 0, 1, 1, 2)
    rename_partition_sources: bool = True
    op_weights: Dict[str, int] = field(default_factory=dict)
    # (rng, gates) -> dict of table options stored in the model, e.g. {"format_version": 2}.
    options: Callable = lambda rng, gates: {}

    def random_options(self, rng: random.Random, gates):
        return self.options(rng, gates)


@dataclass
class Source:
    """A way for ClickHouse to read the table."""

    label: str
    expression: str  # table function call or table name, usable after FROM
    family: str  # iceberg, delta, hudi, paimon: selects the randomized read settings
    time_travel: bool = False  # supports the writer's time-travel settings


class Writer:
    name = "base"
    caps: Capabilities = None
    # True when ClickHouse writes the table and `read` is a foreign reader.
    clickhouse_writes = False

    @staticmethod
    def adjust_caps(caps, gates):
        """Capabilities that depend on the known-issue gates."""
        return caps

    def __init__(self, ctx, table_name, model):
        self.ctx = ctx
        self.table_name = table_name
        self.model = model

    # Lifecycle -----------------------------------------------------------
    def create(self):
        raise NotImplementedError

    def execute(self, op, payload):
        """Execute one operation; `self.model` is the model before the operation."""
        raise NotImplementedError

    def cleanup(self):
        pass

    # Reading -------------------------------------------------------------
    def readable(self):
        """False while the table does not exist in storage yet (formats created by the first write)."""
        return True

    def checkable(self):
        """False when the oracle must skip this step (see `known_issues`)."""
        return True

    def read(self):
        """The table as the writer's own reader sees it: a pyarrow Table (or a dict of named
        pyarrow Tables when there are several foreign readers)."""
        raise NotImplementedError

    def sources(self) -> List[Source]:
        raise NotImplementedError

    # Time travel ---------------------------------------------------------
    def snapshot_ref(self):
        """An opaque reference to the current snapshot, or None without time travel."""
        return None

    def live_refs(self, refs):
        """The subset of `refs` that can still be read (snapshot expiry, VACUUM)."""
        return list(refs)

    def time_travel_settings(self, ref) -> Optional[dict]:
        return None

    # Extra oracle knobs --------------------------------------------------
    def trivial_count_reliable(self):
        return True

    def describe(self):
        return f"{self.name} table {self.table_name}"
