"""Random row predicates with one evaluation model and several SQL dialects.

A predicate is a small tree of `Cmp`, `Between`, `In`, `IsNull`, `Not`, `And`, `Or` over
top-level primitive columns. It is

* evaluated in Python with SQL three-valued logic over the canonical rows the writer read back
  (`evaluate`), which is the oracle for ClickHouse filtered reads: partition pruning, min-max
  pruning, Parquet filter push-down and the Delta engine predicate all have to agree with it;
* rendered for ClickHouse, Spark SQL and DataFusion (delta-rs), and converted into a PyIceberg
  expression, so the same predicate can drive a writer-side `DELETE`/`UPDATE`.
"""

import datetime
import decimal
import uuid

from .ltypes import DECIMAL_CONTEXT, PREDICATE_KINDS, LType

CMP_OPS = ("<", "<=", ">", ">=", "=", "!=")


# --------------------------------------------------------------------- value (de)serialization
def value_to_json(t, v):
    if v is None:
        return None
    k = t.kind
    if k in ("boolean", "int", "long", "string"):
        return v
    if k in ("float", "double"):
        return repr(v)
    if k == "decimal":
        return str(v)
    if k == "date":
        return v.isoformat()
    if k in ("timestamp", "timestamp_ntz"):
        return v.replace(tzinfo=None).isoformat(sep=" ")
    if k == "binary":
        return v.hex()
    if k == "uuid":
        return str(v)
    raise ValueError(f"Cannot serialize a literal of {t.describe()}")


def value_from_json(t, j):
    if j is None:
        return None
    k = t.kind
    if k in ("boolean", "int", "long", "string"):
        return j
    if k in ("float", "double"):
        return float(j)
    if k == "decimal":
        return decimal.Decimal(j)
    if k == "date":
        return datetime.date.fromisoformat(j)
    if k == "timestamp":
        return datetime.datetime.fromisoformat(j).replace(tzinfo=datetime.timezone.utc)
    if k == "timestamp_ntz":
        return datetime.datetime.fromisoformat(j)
    if k == "binary":
        return bytes.fromhex(j)
    if k == "uuid":
        return uuid.UUID(j)
    raise ValueError(k)


# --------------------------------------------------------------------- literal rendering
def _escape(s, backslash=True):
    if backslash:
        s = s.replace("\\", "\\\\")
    return s.replace("'", "\\'" if backslash else "''")


def clickhouse_literal(t, v):
    k = t.kind
    if v is None:
        return "NULL"
    if k == "boolean":
        return "true" if v else "false"
    if k in ("int", "long"):
        return str(v)
    if k == "decimal":
        return f"toDecimal256('{v}', {t.scale})"
    if k == "date":
        return f"toDate32('{v.isoformat()}')"
    if k in ("timestamp", "timestamp_ntz"):
        return f"toDateTime64('{v.replace(tzinfo=None).isoformat(sep=' ')}', 6, 'UTC')"
    if k == "string":
        return "'" + _escape(v) + "'"
    raise ValueError(k)


def spark_literal(t, v):
    k = t.kind
    if v is None:
        return f"CAST(NULL AS {t.spark_ddl()})"
    if k == "boolean":
        return "true" if v else "false"
    if k == "int":
        return str(v)
    if k == "long":
        return f"{v}L"
    if k == "float":
        return _spark_float(v, "FLOAT")
    if k == "double":
        return _spark_float(v, "DOUBLE")
    if k == "decimal":
        return f"CAST('{v}' AS DECIMAL({t.precision}, {t.scale}))"
    if k == "date":
        return f"DATE'{v.isoformat()}'"
    if k == "timestamp":
        return f"TIMESTAMP'{v.replace(tzinfo=None).isoformat(sep=' ')}+00:00'"
    if k == "timestamp_ntz":
        return f"TIMESTAMP_NTZ'{v.isoformat(sep=' ')}'"
    if k == "string":
        return "'" + _escape(v) + "'"
    if k == "binary":
        return f"X'{v.hex()}'"
    raise ValueError(k)


def _spark_float(v, ddl):
    if v != v:
        return f"CAST('NaN' AS {ddl})"
    if v in (float("inf"), float("-inf")):
        return f"CAST('{'-' if v < 0 else ''}Infinity' AS {ddl})"
    return f"CAST({v!r} AS {ddl})"


def datafusion_literal(t, v):
    k = t.kind
    if k == "boolean":
        return "true" if v else "false"
    if k in ("int", "long"):
        return str(v)
    if k == "string":
        return "'" + _escape(v, backslash=False) + "'"
    if k == "date":
        return f"'{v.isoformat()}'::date"
    raise ValueError(f"No DataFusion literal for {k}")


# --------------------------------------------------------------------- predicate tree
class Pred:
    def to_json(self):
        raise NotImplementedError

    def columns(self):
        raise NotImplementedError


class Cmp(Pred):
    def __init__(self, column, op, t, value):
        self.column, self.op, self.t, self.value = column, op, t, value

    def to_json(self):
        return {
            "p": "cmp",
            "c": self.column,
            "op": self.op,
            "t": self.t.to_json(),
            "v": value_to_json(self.t, self.value),
        }

    def columns(self):
        return {self.column}


class Between(Pred):
    def __init__(self, column, t, lo, hi):
        self.column, self.t, self.lo, self.hi = column, t, lo, hi

    def to_json(self):
        return {
            "p": "between",
            "c": self.column,
            "t": self.t.to_json(),
            "lo": value_to_json(self.t, self.lo),
            "hi": value_to_json(self.t, self.hi),
        }

    def columns(self):
        return {self.column}


class In(Pred):
    def __init__(self, column, t, values):
        self.column, self.t, self.values = column, t, values

    def to_json(self):
        return {
            "p": "in",
            "c": self.column,
            "t": self.t.to_json(),
            "vs": [value_to_json(self.t, v) for v in self.values],
        }

    def columns(self):
        return {self.column}


class IsNull(Pred):
    def __init__(self, column, negated):
        self.column, self.negated = column, negated

    def to_json(self):
        return {"p": "isnull", "c": self.column, "neg": self.negated}

    def columns(self):
        return {self.column}


class Not(Pred):
    def __init__(self, a):
        self.a = a

    def to_json(self):
        return {"p": "not", "a": self.a.to_json()}

    def columns(self):
        return self.a.columns()


class And(Pred):
    def __init__(self, a, b):
        self.a, self.b = a, b

    def to_json(self):
        return {"p": "and", "a": self.a.to_json(), "b": self.b.to_json()}

    def columns(self):
        return self.a.columns() | self.b.columns()


class Or(Pred):
    def __init__(self, a, b):
        self.a, self.b = a, b

    def to_json(self):
        return {"p": "or", "a": self.a.to_json(), "b": self.b.to_json()}

    def columns(self):
        return self.a.columns() | self.b.columns()


def pred_from_json(j):
    p = j["p"]
    if p == "cmp":
        t = LType.from_json(j["t"])
        return Cmp(j["c"], j["op"], t, value_from_json(t, j["v"]))
    if p == "between":
        t = LType.from_json(j["t"])
        return Between(
            j["c"], t, value_from_json(t, j["lo"]), value_from_json(t, j["hi"])
        )
    if p == "in":
        t = LType.from_json(j["t"])
        return In(j["c"], t, [value_from_json(t, v) for v in j["vs"]])
    if p == "isnull":
        return IsNull(j["c"], j["neg"])
    if p == "not":
        return Not(pred_from_json(j["a"]))
    if p == "and":
        return And(pred_from_json(j["a"]), pred_from_json(j["b"]))
    if p == "or":
        return Or(pred_from_json(j["a"]), pred_from_json(j["b"]))
    raise ValueError(p)


def random_predicate(rng, columns, kinds=PREDICATE_KINDS, depth=0):
    """`columns` is a list of `(name, LType)`; only primitive columns of `kinds` are referenced.
    Returns None when there is no such column."""
    eligible = [(n, t) for n, t in columns if t.kind in kinds]
    if not eligible:
        return None
    if depth < 2 and rng.random() < 0.35:
        shape = rng.choice(["not", "and", "or"])
        a = random_predicate(rng, columns, kinds, depth + 1)
        if shape == "not":
            return Not(a)
        b = random_predicate(rng, columns, kinds, depth + 1)
        return And(a, b) if shape == "and" else Or(a, b)
    name, t = rng.choice(eligible)
    r = rng.random()
    if r < 0.12:
        return IsNull(name, negated=rng.random() < 0.5)
    if r < 0.25 and t.kind != "boolean":
        lo, hi = sorted(
            [t.random_value(rng, 0), t.random_value(rng, 0)], key=_order_key(t)
        )
        return Between(name, t, lo, hi)
    if r < 0.4:
        values = []
        for _ in range(rng.randint(1, 4)):
            v = t.random_value(rng, 0)
            if v not in values:
                values.append(v)
        return In(name, t, values)
    op = rng.choice(CMP_OPS if t.kind != "boolean" else ("=", "!="))
    return Cmp(name, op, t, t.random_value(rng, 0))


def _order_key(t):
    return lambda v: t.canon(v)


# --------------------------------------------------------------------- evaluation (3VL)
def evaluate(pred, row):
    """`row` maps a column name to its canonical value. Returns True, False or None (unknown)."""
    if isinstance(pred, Cmp):
        v = row[pred.column]
        if v is None:
            return None
        lit = pred.t.canon(pred.value)
        return _compare(v, pred.op, lit)
    if isinstance(pred, Between):
        v = row[pred.column]
        if v is None:
            return None
        return pred.t.canon(pred.lo) <= v <= pred.t.canon(pred.hi)
    if isinstance(pred, In):
        v = row[pred.column]
        if v is None:
            return None
        return v in {pred.t.canon(x) for x in pred.values}
    if isinstance(pred, IsNull):
        is_null = row[pred.column] is None
        return (not is_null) if pred.negated else is_null
    if isinstance(pred, Not):
        a = evaluate(pred.a, row)
        return None if a is None else not a
    if isinstance(pred, And):
        a, b = evaluate(pred.a, row), evaluate(pred.b, row)
        if a is False or b is False:
            return False
        if a is None or b is None:
            return None
        return True
    if isinstance(pred, Or):
        a, b = evaluate(pred.a, row), evaluate(pred.b, row)
        if a is True or b is True:
            return True
        if a is None or b is None:
            return None
        return False
    raise TypeError(pred)


def _compare(a, op, b):
    if op == "<":
        return a < b
    if op == "<=":
        return a <= b
    if op == ">":
        return a > b
    if op == ">=":
        return a >= b
    if op == "=":
        return a == b
    return a != b


# --------------------------------------------------------------------- rendering
def render(pred, dialect):
    """SQL text of `pred` for `dialect` in ("clickhouse", "spark", "datafusion")."""
    quote = {"clickhouse": _bq, "spark": _bq, "datafusion": _dq}[dialect]
    literal = {
        "clickhouse": clickhouse_literal,
        "spark": spark_literal,
        "datafusion": datafusion_literal,
    }[dialect]
    return _render(pred, quote, literal)


def _bq(name):
    return "`" + name.replace("`", "``") + "`"


def _dq(name):
    return '"' + name.replace('"', '""') + '"'


def _render(pred, quote, literal):
    if isinstance(pred, Cmp):
        return f"{quote(pred.column)} {pred.op} {literal(pred.t, pred.value)}"
    if isinstance(pred, Between):
        return f"{quote(pred.column)} BETWEEN {literal(pred.t, pred.lo)} AND {literal(pred.t, pred.hi)}"
    if isinstance(pred, In):
        return (
            f"{quote(pred.column)} IN ("
            + ", ".join(literal(pred.t, v) for v in pred.values)
            + ")"
        )
    if isinstance(pred, IsNull):
        return f"{quote(pred.column)} IS {'NOT ' if pred.negated else ''}NULL"
    if isinstance(pred, Not):
        return f"NOT ({_render(pred.a, quote, literal)})"
    if isinstance(pred, And):
        return f"({_render(pred.a, quote, literal)}) AND ({_render(pred.b, quote, literal)})"
    if isinstance(pred, Or):
        return f"({_render(pred.a, quote, literal)}) OR ({_render(pred.b, quote, literal)})"
    raise TypeError(pred)


def to_pyiceberg(pred):
    """A PyIceberg `BooleanExpression` equivalent to `pred`."""
    from pyiceberg import expressions as e

    def lit(t, v):
        if t.kind == "decimal":
            return decimal.Decimal(v)
        if t.kind == "date":
            return v.isoformat()
        if t.kind == "timestamp":
            return v.isoformat()
        if t.kind == "timestamp_ntz":
            return v.isoformat()
        return v

    if isinstance(pred, Cmp):
        cls = {
            "<": e.LessThan,
            "<=": e.LessThanOrEqual,
            ">": e.GreaterThan,
            ">=": e.GreaterThanOrEqual,
            "=": e.EqualTo,
            "!=": e.NotEqualTo,
        }[pred.op]
        return cls(pred.column, lit(pred.t, pred.value))
    if isinstance(pred, Between):
        return e.And(
            e.GreaterThanOrEqual(pred.column, lit(pred.t, pred.lo)),
            e.LessThanOrEqual(pred.column, lit(pred.t, pred.hi)),
        )
    if isinstance(pred, In):
        return e.In(pred.column, {lit(pred.t, v) for v in pred.values})
    if isinstance(pred, IsNull):
        return e.NotNull(pred.column) if pred.negated else e.IsNull(pred.column)
    if isinstance(pred, Not):
        return e.Not(to_pyiceberg(pred.a))
    if isinstance(pred, And):
        return e.And(to_pyiceberg(pred.a), to_pyiceberg(pred.b))
    if isinstance(pred, Or):
        return e.Or(to_pyiceberg(pred.a), to_pyiceberg(pred.b))
    raise TypeError(pred)


def predicate_kinds_for(dialect):
    if dialect == "datafusion":
        return ("boolean", "int", "long", "string")
    if dialect == "pyiceberg":
        return ("boolean", "int", "long", "string", "date")
    return PREDICATE_KINDS


__all__ = [
    "Pred",
    "Cmp",
    "Between",
    "In",
    "IsNull",
    "Not",
    "And",
    "Or",
    "random_predicate",
    "evaluate",
    "render",
    "to_pyiceberg",
    "pred_from_json",
    "value_to_json",
    "value_from_json",
    "predicate_kinds_for",
    "DECIMAL_CONTEXT",
]
