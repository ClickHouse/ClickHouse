"""Logical type model shared by every writer adapter, the generator and the oracle.

A column type is one of the immutable `LType` values below. Each type knows how to

* produce a random Python value (`random_value`),
* render itself as a Spark SQL DDL type, a pyarrow type and a ClickHouse type,
* reduce a value read back by any engine to one canonical, hashable form (`canon`).

`canon` is the heart of the differential oracle: Spark (`DataFrame.toArrow`), PyIceberg,
delta-rs and ClickHouse (`FORMAT Arrow`) all hand back pyarrow data, but with different
physical encodings of the same logical value (a `Date` may come back as a `date` or as an
integer number of days, a `String` as `str` or `bytes`, a timestamp as a naive or an aware
`datetime`). After `canon` two engines agree exactly when they returned the same logical value.
"""

import datetime
import decimal
import math
import struct
import uuid
from dataclasses import dataclass
from typing import Optional, Tuple

# Wide enough for `Decimal(38, s)`; the default context (28 digits) would round such values.
DECIMAL_CONTEXT = decimal.Context(prec=100)

EPOCH_DATE = datetime.date(1970, 1, 1)
EPOCH_UTC = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)

# Inside the `Date32` and `DateTime64` ranges of ClickHouse and representable by every writer.
MIN_DATE = datetime.date(1901, 1, 1)
MAX_DATE = datetime.date(2261, 12, 31)

PRIMITIVE_KINDS = (
    "boolean",
    "int",
    "long",
    "float",
    "double",
    "decimal",
    "date",
    "timestamp",  # adjusted to UTC: Iceberg `timestamptz`, Delta/Spark `TIMESTAMP`
    "timestamp_ntz",  # wall clock: Iceberg `timestamp`, Delta/Spark `TIMESTAMP_NTZ`
    "string",
    "binary",
    "uuid",
)

# Kinds a predicate may reference: totally ordered, no NaN, with a literal syntax in every dialect.
PREDICATE_KINDS = (
    "boolean",
    "int",
    "long",
    "decimal",
    "date",
    "timestamp",
    "timestamp_ntz",
    "string",
)


@dataclass(frozen=True)
class LType:
    kind: str
    precision: int = 0  # decimal
    scale: int = 0  # decimal
    element: Optional["LType"] = None  # list
    key: Optional["LType"] = None  # map
    value: Optional["LType"] = None  # map
    fields: Tuple[Tuple[str, "LType"], ...] = ()  # struct

    # ----------------------------------------------------------------- shape
    @property
    def is_primitive(self):
        return self.kind in PRIMITIVE_KINDS

    @property
    def is_nested(self):
        return not self.is_primitive

    def to_json(self):
        if self.kind == "decimal":
            return {"kind": "decimal", "precision": self.precision, "scale": self.scale}
        if self.kind == "list":
            return {"kind": "list", "element": self.element.to_json()}
        if self.kind == "map":
            return {
                "kind": "map",
                "key": self.key.to_json(),
                "value": self.value.to_json(),
            }
        if self.kind == "struct":
            return {
                "kind": "struct",
                "fields": [[n, t.to_json()] for n, t in self.fields],
            }
        return {"kind": self.kind}

    @staticmethod
    def from_json(j):
        kind = j["kind"]
        if kind == "decimal":
            return decimal_t(j["precision"], j["scale"])
        if kind == "list":
            return list_t(LType.from_json(j["element"]))
        if kind == "map":
            return map_t(LType.from_json(j["key"]), LType.from_json(j["value"]))
        if kind == "struct":
            return struct_t([(n, LType.from_json(t)) for n, t in j["fields"]])
        return LType(kind)

    def describe(self):
        if self.kind == "decimal":
            return f"decimal({self.precision},{self.scale})"
        if self.kind == "list":
            return f"list<{self.element.describe()}>"
        if self.kind == "map":
            return f"map<{self.key.describe()},{self.value.describe()}>"
        if self.kind == "struct":
            return (
                "struct<"
                + ",".join(f"{n}:{t.describe()}" for n, t in self.fields)
                + ">"
            )
        return self.kind

    def contains_kind(self, kind):
        if self.kind == kind:
            return True
        return any(child.contains_kind(kind) for child in self.children())

    def children(self):
        if self.kind == "list":
            return [self.element]
        if self.kind == "map":
            return [self.key, self.value]
        if self.kind == "struct":
            return [t for _, t in self.fields]
        return []

    # ----------------------------------------------------------------- DDL
    def spark_ddl(self):
        k = self.kind
        simple = {
            "boolean": "BOOLEAN",
            "int": "INT",
            "long": "BIGINT",
            "float": "FLOAT",
            "double": "DOUBLE",
            "date": "DATE",
            "timestamp": "TIMESTAMP",
            "timestamp_ntz": "TIMESTAMP_NTZ",
            "string": "STRING",
            "binary": "BINARY",
        }
        if k in simple:
            return simple[k]
        if k == "decimal":
            return f"DECIMAL({self.precision}, {self.scale})"
        if k == "list":
            return f"ARRAY<{self.element.spark_ddl()}>"
        if k == "map":
            return f"MAP<{self.key.spark_ddl()}, {self.value.spark_ddl()}>"
        if k == "struct":
            return (
                "STRUCT<"
                + ", ".join(f"`{n}`: {t.spark_ddl()}" for n, t in self.fields)
                + ">"
            )
        raise ValueError(f"Spark has no type for {self.describe()}")

    def arrow(self, storage=False):
        """The pyarrow type. With `storage`, extension types (UUID) are replaced by their storage type,
        which `pyarrow.array` can build from Python values."""
        import pyarrow as pa

        k = self.kind
        if k == "boolean":
            return pa.bool_()
        if k == "int":
            return pa.int32()
        if k == "long":
            return pa.int64()
        if k == "float":
            return pa.float32()
        if k == "double":
            return pa.float64()
        if k == "decimal":
            return pa.decimal128(self.precision, self.scale)
        if k == "date":
            return pa.date32()
        if k == "timestamp":
            return pa.timestamp("us", tz="UTC")
        if k == "timestamp_ntz":
            return pa.timestamp("us")
        if k == "string":
            return pa.string()
        if k == "binary":
            return pa.binary()
        if k == "uuid":
            return pa.binary(16) if storage else pa.uuid()
        if k == "list":
            return pa.list_(self.element.arrow(storage))
        if k == "map":
            return pa.map_(self.key.arrow(storage), self.value.arrow(storage))
        if k == "struct":
            return pa.struct(
                [pa.field(n, t.arrow(storage), nullable=True) for n, t in self.fields]
            )
        raise ValueError(k)

    def clickhouse(self, nullable=True):
        """ClickHouse type for a column of this logical type (used when ClickHouse is the writer)."""
        k = self.kind
        simple = {
            "boolean": "Bool",
            "int": "Int32",
            "long": "Int64",
            "float": "Float32",
            "double": "Float64",
            "date": "Date32",
            "timestamp": "DateTime64(6, 'UTC')",
            "timestamp_ntz": "DateTime64(6)",
            "string": "String",
            "binary": "String",
            "uuid": "UUID",
        }
        if k in simple:
            base = simple[k]
        elif k == "decimal":
            base = f"Decimal({self.precision}, {self.scale})"
        elif k == "list":
            return f"Array({self.element.clickhouse()})"
        elif k == "map":
            return (
                f"Map({self.key.clickhouse(nullable=False)}, {self.value.clickhouse()})"
            )
        elif k == "struct":
            return (
                "Tuple("
                + ", ".join(f"`{n}` {t.clickhouse()}" for n, t in self.fields)
                + ")"
            )
        else:
            raise ValueError(k)
        return f"Nullable({base})" if nullable else base

    # ----------------------------------------------------------------- values
    def random_value(self, rng, null_probability=0.1):
        """A random Python value. Nested values (list/map/struct) are never NULL themselves, only their
        elements: ClickHouse cannot express a NULL `Array`/`Map` (it reads it as empty), so a NULL
        there would be a guaranteed, uninteresting mismatch."""
        if self.is_primitive and rng.random() < null_probability:
            return None
        k = self.kind
        if k == "boolean":
            return rng.random() < 0.5
        if k == "int":
            return _random_int(rng, 31)
        if k == "long":
            return _random_int(rng, 63)
        if k == "float":
            return struct.unpack("f", struct.pack("f", _random_float(rng)))[0]
        if k == "double":
            return _random_float(rng)
        if k == "decimal":
            return _random_decimal(rng, self.precision, self.scale)
        if k == "date":
            return _random_date(rng)
        if k == "timestamp":
            return _random_datetime(rng).replace(tzinfo=datetime.timezone.utc)
        if k == "timestamp_ntz":
            return _random_datetime(rng)
        if k == "string":
            return _random_string(rng)
        if k == "binary":
            return _random_bytes(rng)
        if k == "uuid":
            return uuid.UUID(int=rng.getrandbits(128))
        if k == "list":
            return [
                self.element.random_value(rng, null_probability)
                for _ in range(rng.choice([0, 1, 1, 2, 3, 5]))
            ]
        if k == "map":
            keys = []
            for _ in range(rng.choice([0, 1, 2, 3])):
                key = self.key.random_value(rng, null_probability=0)
                if key not in keys:
                    keys.append(key)
            return [
                (key, self.value.random_value(rng, null_probability)) for key in keys
            ]
        if k == "struct":
            return {n: t.random_value(rng, null_probability) for n, t in self.fields}
        raise ValueError(k)

    def canon(self, v):
        """Canonical, hashable, comparable form of a value read back by any engine."""
        if v is None:
            # ClickHouse has no NULL `Array`, `Map` or `Tuple`: a NULL list or map reads as empty and a NULL
            # struct as a tuple of NULLs. Rows written before an `ADD COLUMN` of a nested type are NULL.
            if self.kind in ("list", "map"):
                return ()
            if self.kind == "struct":
                return tuple(t.canon(None) for _, t in self.fields)
            return None
        k = self.kind
        if k == "boolean":
            return bool(v)
        if k in ("int", "long"):
            return int(v)
        if k in ("float", "double"):
            f = float(v)
            if math.isnan(f):
                return "NaN"
            if k == "float":
                f = struct.unpack("f", struct.pack("f", f))[0]
            return 0.0 if f == 0 else f
        if k == "decimal":
            d = v if isinstance(v, decimal.Decimal) else decimal.Decimal(str(v))
            return d.quantize(
                decimal.Decimal(1).scaleb(-self.scale, context=DECIMAL_CONTEXT),
                context=DECIMAL_CONTEXT,
            )
        if k == "date":
            if isinstance(v, datetime.datetime):
                return v.date().isoformat()
            if isinstance(v, datetime.date):
                return v.isoformat()
            return (EPOCH_DATE + datetime.timedelta(days=int(v))).isoformat()
        if k in ("timestamp", "timestamp_ntz"):
            if isinstance(v, datetime.datetime):
                if v.tzinfo is None:
                    v = v.replace(tzinfo=datetime.timezone.utc)
                delta = v - EPOCH_UTC
                return (
                    delta.days * 86400 + delta.seconds
                ) * 1_000_000 + delta.microseconds
            return int(v)
        if k in ("string", "binary"):
            if isinstance(v, str):
                return v.encode("utf-8")
            return bytes(v)
        if k == "uuid":
            if isinstance(v, uuid.UUID):
                return str(v)
            if isinstance(v, (bytes, bytearray)) and len(v) == 16:
                return str(uuid.UUID(bytes=bytes(v)))
            if isinstance(v, (bytes, bytearray)):
                v = v.decode()
            return str(uuid.UUID(str(v)))
        if k == "list":
            return tuple(self.element.canon(e) for e in v)
        if k == "map":
            items = v.items() if isinstance(v, dict) else v
            return tuple(
                sorted(
                    ((self.key.canon(a), self.value.canon(b)) for a, b in items),
                    key=repr,
                )
            )
        if k == "struct":
            if hasattr(v, "asDict"):
                v = v.asDict()
            if isinstance(v, dict):
                # Match fields by name: a reader that swaps the values of two fields must not compare equal.
                if sorted(v) != sorted(n for n, _ in self.fields):
                    return ("<struct fields mismatch>", tuple(sorted(map(repr, v))))
                return tuple(t.canon(v[n]) for n, t in self.fields)
            values = list(v)
            if len(values) != len(self.fields):
                return ("<struct arity mismatch>", tuple(map(repr, values)))
            return tuple(t.canon(x) for (_, t), x in zip(self.fields, values))
        raise ValueError(k)

    def to_spark_value(self, v):
        """A generated value in the form `SparkSession.createDataFrame` accepts for this type."""
        if v is None:
            return None
        k = self.kind
        if k == "binary":
            return bytearray(v)
        if k == "uuid":
            return str(v)
        if k == "list":
            return [self.element.to_spark_value(e) for e in v]
        if k == "map":
            return {
                self.key.to_spark_value(a): self.value.to_spark_value(b) for a, b in v
            }
        if k == "struct":
            return tuple(t.to_spark_value(v[n]) for n, t in self.fields)
        return v

    def to_arrow_value(self, v):
        if v is None:
            return None
        k = self.kind
        if k == "uuid":
            return v.bytes
        if k == "list":
            return [self.element.to_arrow_value(e) for e in v]
        if k == "map":
            return [
                (self.key.to_arrow_value(a), self.value.to_arrow_value(b)) for a, b in v
            ]
        if k == "struct":
            return {n: t.to_arrow_value(v[n]) for n, t in self.fields}
        return v

    # ----------------------------------------------------------------- evolution
    def promotions(self):
        """Types this type may be promoted to by schema evolution (Iceberg spec; Delta type widening)."""
        if self.kind == "int":
            return [LType("long")]
        if self.kind == "float":
            return [LType("double")]
        if self.kind == "decimal" and self.precision < 38:
            return [
                decimal_t(min(38, self.precision + extra), self.scale)
                for extra in (1, 9)
            ]
        return []


def decimal_t(precision, scale):
    return LType("decimal", precision=precision, scale=scale)


def list_t(element):
    return LType("list", element=element)


def map_t(key, value):
    return LType("map", key=key, value=value)


def struct_t(fields):
    return LType("struct", fields=tuple((n, t) for n, t in fields))


# --------------------------------------------------------------------- random primitives
_INT_EDGES = [0, 1, -1, 2, 7, 10, 42, 100, -100, 1000]
_STRINGS = [
    "",
    "a",
    "b",
    "abc",
    "ABC",
    "foo",
    "bar",
    "baz",
    "z" * 40,
    "with space",
    "ünï©ødé",
    "日本",
    "a'quote",
    "back\\slash",
    "null",
    "NULL",
]
_FLOATS = [
    0.0,
    -0.0,
    1.0,
    -1.0,
    0.5,
    1.5,
    3.25,
    -2.75,
    1e10,
    -1e-10,
    123.456,
    float("inf"),
    float("-inf"),
    float("nan"),
]


def _random_int(rng, bits):
    r = rng.random()
    if r < 0.5:
        return rng.randint(-20, 120)
    if r < 0.8:
        return rng.choice(_INT_EDGES)
    if r < 0.95:
        return rng.randint(-(2**bits), 2**bits - 1)
    return rng.choice([-(2**bits), 2**bits - 1])


def _random_float(rng):
    if rng.random() < 0.4:
        return rng.choice(_FLOATS)
    return round(rng.uniform(-1000, 1000), rng.randint(0, 4))


def _random_decimal(rng, precision, scale):
    int_digits = precision - scale
    r = rng.random()
    if r < 0.5:
        unscaled = rng.randint(-1000, 1000)
    elif r < 0.9:
        unscaled = rng.randint(-(10**precision) + 1, 10**precision - 1)
    else:
        unscaled = rng.choice([10**precision - 1, -(10**precision) + 1, 0])
    limit = 10**precision - 1
    unscaled = max(-limit, min(limit, unscaled))
    if int_digits <= 0:
        unscaled = max(-(10**scale) + 1, min(10**scale - 1, unscaled))
    return decimal.Decimal(unscaled).scaleb(-scale, context=DECIMAL_CONTEXT)


def _random_date(rng):
    r = rng.random()
    if r < 0.6:
        return datetime.date(2024, 1, 1) + datetime.timedelta(
            days=rng.randint(-40, 400)
        )
    if r < 0.75:
        return rng.choice(
            [
                datetime.date(1970, 1, 1),
                datetime.date(1969, 12, 31),
                datetime.date(2000, 2, 29),
                MIN_DATE,
                MAX_DATE,
            ]
        )
    return MIN_DATE + datetime.timedelta(
        days=rng.randint(0, (MAX_DATE - MIN_DATE).days)
    )


def _random_datetime(rng):
    date = _random_date(rng)
    r = rng.random()
    if r < 0.3:
        time = datetime.time(0, 0, 0)
    elif r < 0.6:
        time = datetime.time(rng.randint(0, 23), rng.randint(0, 59), rng.randint(0, 59))
    else:
        time = datetime.time(
            rng.randint(0, 23),
            rng.randint(0, 59),
            rng.randint(0, 59),
            rng.randint(0, 999999),
        )
    return datetime.datetime.combine(date, time)


def _random_string(rng):
    if rng.random() < 0.7:
        return rng.choice(_STRINGS)
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 12)))


def _random_bytes(rng):
    if rng.random() < 0.3:
        return rng.choice([b"", b"\x00", b"\xff\xfe", b"abc"])
    return bytes(rng.getrandbits(8) for _ in range(rng.randint(0, 10)))


# --------------------------------------------------------------------- random types
def random_primitive(rng, kinds, max_decimal_precision=38):
    kind = rng.choice(kinds)
    if kind == "decimal":
        precision = min(
            max_decimal_precision,
            rng.choice([1, 5, 9, 10, 18, 19, 28, 38, rng.randint(1, 38)]),
        )
        scale = rng.choice([0, 0, 1, 2, min(precision, 6), precision])
        return decimal_t(precision, min(scale, precision))
    return LType(kind)


def random_type(rng, caps, depth=0):
    """A random column type allowed by the writer capabilities `caps` (see `writers.Capabilities`)."""
    nested = [k for k in ("list", "map", "struct") if k in caps.nested_kinds]
    if depth < caps.max_nesting_depth and nested and rng.random() < 0.25:
        kind = rng.choice(nested)
        if kind == "list":
            return list_t(random_type(rng, caps, depth + 1))
        if kind == "map":
            key_kinds = [
                k for k in ("int", "long", "string") if k in caps.primitive_kinds
            ]
            return map_t(
                random_primitive(rng, key_kinds), random_type(rng, caps, depth + 1)
            )
        names = []
        for i in range(rng.randint(1, 3)):
            names.append(f"f{i}")
        return struct_t([(n, random_type(rng, caps, depth + 1)) for n in names])
    return random_primitive(rng, list(caps.primitive_kinds), caps.max_decimal_precision)
