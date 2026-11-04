"""Operation registry and the seeded history generator.

A history is `{seed, writer, table, ops}`: the initial table model (schema, partitioning, writer
options) and a list of operations, each a plain JSON dict `{"kind": ..., <params>}`. It is a pure
function of the seed and the writer capabilities, it serializes to JSON, and it can be replayed
and shrunk: every operation is re-validated against the model at execution time and skipped when
an earlier operation it depends on was removed (for example a `rename_column` of a column whose
`add_column` was shrunk away).

To add an operation: write an `OpSpec`, `register` it, then implement it in the writer adapters
that list it in their `Capabilities.op_weights`.
"""

import json
import random
from dataclasses import dataclass
from typing import Callable, Dict, Optional

from .ltypes import LType, random_primitive, random_type
from .model import ID_COLUMN, PartitionField, TableModel
from .predicates import pred_from_json, random_predicate


@dataclass
class OpSpec:
    kind: str
    # (rng, model, ctx) -> params dict or None when the operation does not apply to the model.
    generate: Callable
    # (model, params) -> bool: the operation still makes sense for the model (replay, shrinking).
    validate: Callable = lambda model, params: True
    # (model, params) -> payload (rows to write); may allocate row ids. Runs before the writer.
    prepare: Callable = lambda model, params: {}
    # (model, params) -> None: apply the schema/partitioning effect. Runs after the writer succeeded.
    apply_schema: Callable = lambda model, params: None


REGISTRY: Dict[str, OpSpec] = {}


def register(spec: OpSpec):
    REGISTRY[spec.kind] = spec
    return spec


@dataclass
class GenContext:
    caps: "object"  # writers.Capabilities
    gates: "object"  # known_issues.Gates


# --------------------------------------------------------------------- helpers
def _types_equal(model, name, type_json):
    t = model.column_type(name)
    return t is not None and t.to_json() == type_json


def _predicate_valid(model, pj):
    pred = pred_from_json(pj)
    return all(model.has(c) for c in pred.columns()) and _pred_types_match(model, pj)


def _pred_types_match(model, pj):
    if "t" in pj and not _types_equal(model, pj["c"], pj["t"]):
        return False
    if pj["p"] == "isnull":
        return model.has(pj["c"])
    return all(_pred_types_match(model, pj[k]) for k in ("a", "b") if k in pj)


def _predicate(rng, model, ctx):
    kinds = [
        k
        for k in ctx.caps.predicate_kinds
        if k in ctx.caps.primitive_kinds or k == "long"
    ]
    pred = random_predicate(rng, model.columns, kinds)
    return pred.to_json() if pred else None


def _new_name(rng, model, ctx):
    if model.dropped and rng.random() < 0.3:
        name = rng.choice(model.dropped)
        if not model.has(name):
            return name
    for _ in range(100):
        base = f"c{rng.randrange(1000)}"
        if ctx.caps.weird_names and rng.random() < 0.3:
            base = rng.choice(
                [
                    f"Col Mixed {base}",
                    f"ünï_{base}",
                    f"{base} x",
                    f"{base}-dash",
                    f"UPPER_{base}",
                ]
            )
        if not model.has(base) and all(
            n.lower() != base.lower() for n in model.names()
        ):
            return base
    raise RuntimeError("Could not invent a fresh column name")


# --------------------------------------------------------------------- data operations
def _gen_rows(rng, model, ctx):
    return {"n": rng.choice([1, 2, 3, 5, 10, 20, 50]), "seed": rng.getrandbits(32)}


register(
    OpSpec(
        "append",
        _gen_rows,
        prepare=lambda m, p: {"rows": m.generate_rows(p["n"], p["seed"])},
    )
)


def _gen_overwrite(rng, model, ctx):
    params = _gen_rows(rng, model, ctx)
    params["dynamic"] = bool(model.partitioning) and rng.random() < 0.5
    return params


register(
    OpSpec(
        "overwrite",
        _gen_overwrite,
        prepare=lambda m, p: {"rows": m.generate_rows(p["n"], p["seed"])},
    )
)


def _gen_delete(rng, model, ctx):
    pj = _predicate(rng, model, ctx)
    return {"predicate": pj} if pj else None


register(
    OpSpec(
        "delete", _gen_delete, validate=lambda m, p: _predicate_valid(m, p["predicate"])
    )
)


def _gen_update(rng, model, ctx):
    pj = _predicate(rng, model, ctx)
    targets = [
        (n, t)
        for n, t in model.non_id_columns()
        if t.is_primitive and t.kind in ctx.caps.update_kinds
    ]
    if not pj or not targets:
        return None
    name, t = rng.choice(targets)
    return {
        "predicate": pj,
        "column": name,
        "type": t.to_json(),
        "value_seed": rng.getrandbits(32),
    }


def _prepare_update(model, params):
    t = LType.from_json(params["type"])
    return {
        "value": t.random_value(
            random.Random(params["value_seed"]), null_probability=0.2
        ),
        "type": t,
    }


register(
    OpSpec(
        "update",
        _gen_update,
        validate=lambda m, p: _predicate_valid(m, p["predicate"])
        and _types_equal(m, p["column"], p["type"]),
        prepare=_prepare_update,
    )
)


def _gen_merge(rng, model, ctx):
    return {
        "n_matched": rng.choice([0, 1, 3, 10]),
        "n_new": rng.choice([0, 1, 3, 10]),
        "seed": rng.getrandbits(32),
    }


def _prepare_merge(model, params):
    rng = random.Random(params["seed"])
    matched = (
        rng.sample(range(model.next_id), min(params["n_matched"], model.next_id))
        if model.next_id
        else []
    )
    fresh = list(range(model.next_id, model.next_id + params["n_new"]))
    model.next_id += params["n_new"]
    return {
        "rows": model.generate_rows(
            len(matched) + len(fresh), params["seed"], ids=matched + fresh
        )
    }


register(OpSpec("merge", _gen_merge, prepare=_prepare_merge))


# --------------------------------------------------------------------- schema evolution
def _gen_add_column(rng, model, ctx):
    if len(model.columns) >= ctx.caps.max_columns:
        return None
    t = (
        random_type(rng, ctx.caps)
        if ctx.caps.add_nested
        else random_primitive(
            rng, list(ctx.caps.primitive_kinds), ctx.caps.max_decimal_precision
        )
    )
    return {"name": _new_name(rng, model, ctx), "type": t.to_json()}


def _apply_add_column(model, params):
    model.columns.append([params["name"], LType.from_json(params["type"])])
    if params["name"] in model.dropped:
        model.dropped.remove(params["name"])


register(
    OpSpec(
        "add_column",
        _gen_add_column,
        validate=lambda m, p: not m.has(p["name"]),
        apply_schema=_apply_add_column,
    )
)


def _droppable(model):
    sources = model.partition_source_columns()
    return [n for n, _ in model.non_id_columns() if n not in sources]


def _gen_drop_column(rng, model, ctx):
    candidates = _droppable(model)
    return {"name": rng.choice(candidates)} if candidates else None


def _apply_drop_column(model, params):
    model.columns = [[n, t] for n, t in model.columns if n != params["name"]]
    model.dropped.append(params["name"])


register(
    OpSpec(
        "drop_column",
        _gen_drop_column,
        validate=lambda m, p: p["name"] in _droppable(m),
        apply_schema=_apply_drop_column,
    )
)


def _gen_rename_column(rng, model, ctx):
    candidates = [
        n
        for n, _ in model.non_id_columns()
        if ctx.caps.rename_partition_sources
        or n not in model.partition_source_columns()
    ]
    if not candidates:
        return None
    return {"old": rng.choice(candidates), "new": _new_name(rng, model, ctx)}


def _apply_rename_column(model, params):
    for c in model.columns:
        if c[0] == params["old"]:
            c[0] = params["new"]
    for p in model.partitioning:
        if p.column == params["old"]:
            p.column = params["new"]


register(
    OpSpec(
        "rename_column",
        _gen_rename_column,
        validate=lambda m, p: m.has(p["old"])
        and p["old"] != ID_COLUMN
        and not m.has(p["new"]),
        apply_schema=_apply_rename_column,
    )
)


def _gen_move_column(rng, model, ctx):
    candidates = [n for n, _ in model.non_id_columns()]
    if len(model.columns) < 3 or not candidates:
        return None
    name = rng.choice(candidates)
    others = [n for n in model.names() if n != name]
    after = None if rng.random() < 0.3 else rng.choice(others)
    return {"name": name, "after": after}


def _apply_move_column(model, params):
    col = next(c for c in model.columns if c[0] == params["name"])
    model.columns.remove(col)
    if params["after"] is None:
        model.columns.insert(0, col)
    else:
        model.columns.insert(model.index(params["after"]) + 1, col)


register(
    OpSpec(
        "move_column",
        _gen_move_column,
        validate=lambda m, p: m.has(p["name"])
        and (p["after"] is None or m.has(p["after"])),
        apply_schema=_apply_move_column,
    )
)


def _gen_promote_column(rng, model, ctx):
    candidates = [
        (n, t, to)
        for n, t in model.non_id_columns()
        for to in t.promotions()
        if to.kind in ctx.caps.promotion_targets
    ]
    if not candidates:
        return None
    name, t, to = rng.choice(candidates)
    return {"name": name, "from": t.to_json(), "to": to.to_json()}


def _apply_promote_column(model, params):
    for c in model.columns:
        if c[0] == params["name"]:
            c[1] = LType.from_json(params["to"])


register(
    OpSpec(
        "promote_column",
        _gen_promote_column,
        validate=lambda m, p: _types_equal(m, p["name"], p["from"]),
        apply_schema=_apply_promote_column,
    )
)


# --------------------------------------------------------------------- partitioning
TRANSFORM_KINDS = {
    "identity": (
        "boolean",
        "int",
        "long",
        "string",
        "date",
        "decimal",
        "timestamp",
        "timestamp_ntz",
    ),
    "bucket": (
        "int",
        "long",
        "decimal",
        "date",
        "timestamp",
        "timestamp_ntz",
        "string",
        "binary",
        "uuid",
    ),
    "truncate": ("int", "long", "decimal", "string", "binary"),
    "year": ("date", "timestamp", "timestamp_ntz"),
    "month": ("date", "timestamp", "timestamp_ntz"),
    "day": ("date", "timestamp", "timestamp_ntz"),
    "hour": ("timestamp", "timestamp_ntz"),
}


def random_partition_field(rng, model, caps, existing):
    options = []
    for transform in caps.partition_transforms:
        for name, t in (
            model.non_id_columns() if not caps.partition_on_id else model.columns
        ):
            if t.kind in TRANSFORM_KINDS[transform] and t.kind in caps.partition_kinds:
                # One partition field per source column: several are "redundant" for PyIceberg.
                if not any(p.column == name for p in existing):
                    options.append((transform, name))
    if not options:
        return None
    transform, name = rng.choice(options)
    param = (
        rng.choice([1, 2, 3, 4, 16])
        if transform == "bucket"
        else rng.choice([1, 2, 4, 10]) if transform == "truncate" else 0
    )
    return PartitionField(transform, name, param)


def _gen_add_partition_field(rng, model, ctx):
    if len(model.partitioning) >= 3:
        return None
    field = random_partition_field(rng, model, ctx.caps, model.partitioning)
    return field.to_json() if field else None


def _apply_add_partition_field(model, params):
    model.partitioning.append(PartitionField.from_json(params))


register(
    OpSpec(
        "add_partition_field",
        _gen_add_partition_field,
        validate=lambda m, p: m.has(p["column"])
        and PartitionField.from_json(p) not in m.partitioning,
        apply_schema=_apply_add_partition_field,
    )
)


def _gen_drop_partition_field(rng, model, ctx):
    return rng.choice(model.partitioning).to_json() if model.partitioning else None


def _apply_drop_partition_field(model, params):
    model.partitioning.remove(PartitionField.from_json(params))


register(
    OpSpec(
        "drop_partition_field",
        _gen_drop_partition_field,
        validate=lambda m, p: PartitionField.from_json(p) in m.partitioning,
        apply_schema=_apply_drop_partition_field,
    )
)


# --------------------------------------------------------------------- maintenance
register(OpSpec("compact", lambda rng, m, ctx: {}))
register(OpSpec("rewrite_position_deletes", lambda rng, m, ctx: {}))
register(
    OpSpec(
        "expire_snapshots", lambda rng, m, ctx: {"retain_last": rng.choice([1, 2, 3])}
    )
)
register(
    OpSpec(
        "set_write_mode",
        lambda rng, m, ctx: {"mode": rng.choice(["copy-on-write", "merge-on-read"])},
    )
)
register(OpSpec("checkpoint", lambda rng, m, ctx: {}))
register(OpSpec("vacuum", lambda rng, m, ctx: {}))


# --------------------------------------------------------------------- history
def generate_table(rng, caps, gates):
    model = TableModel()
    model.columns.append([ID_COLUMN, LType("long")])
    for i in range(rng.randint(1, caps.initial_columns)):
        t = random_type(rng, caps)
        name = f"c{i}"
        if caps.weird_names and rng.random() < 0.15:
            name = rng.choice([f"Mixed Case {i}", f"ünï{i}", f"sp ace{i}"])
        model.columns.append([name, t])
    for _ in range(rng.choice(caps.initial_partition_fields)):
        field = random_partition_field(rng, model, caps, model.partitioning)
        if field:
            model.partitioning.append(field)
    model.options = caps.random_options(rng, gates)
    return model


def generate_history(seed, writer_name, caps, gates, num_ops):
    """A random history for one writer: initial table plus `num_ops` operations."""
    rng = random.Random(f"{writer_name}:{seed}")
    model = generate_table(rng, caps, gates)
    initial = model.copy()
    ctx = GenContext(caps=caps, gates=gates)
    kinds = sorted(caps.op_weights)
    weights = [caps.op_weights[k] for k in kinds]
    ops = []
    # The first operation always writes data, otherwise most checks would compare empty tables.
    first = {"kind": "append", **_gen_rows(rng, model, ctx)}
    REGISTRY["append"].prepare(model, first)
    ops.append(first)
    attempts = 0
    while len(ops) < num_ops and attempts < num_ops * 20:
        attempts += 1
        kind = rng.choices(kinds, weights)[0]
        spec = REGISTRY[kind]
        params = spec.generate(rng, model, ctx)
        if params is None:
            continue
        op = {"kind": kind, **params}
        spec.prepare(model, op)
        spec.apply_schema(model, op)
        ops.append(op)
    return {"seed": seed, "writer": writer_name, "table": initial.to_json(), "ops": ops}


def dumps_history(history):
    return json.dumps(history, sort_keys=True, ensure_ascii=False)


def describe_op(op):
    params = {k: v for k, v in op.items() if k != "kind"}
    if "predicate" in params:
        from .predicates import render

        params["predicate"] = render(pred_from_json(params["predicate"]), "clickhouse")
    return f"{op['kind']}({', '.join(f'{k}={v}' for k, v in params.items())})"


def validate_op(model, op) -> Optional[str]:
    """None when the operation applies to the model, otherwise the reason it is skipped."""
    spec = REGISTRY.get(op["kind"])
    if spec is None:
        return f"unknown operation {op['kind']}"
    try:
        return (
            None if spec.validate(model, op) else "does not apply to the current schema"
        )
    except (KeyError, ValueError) as e:
        return f"invalid parameters: {e}"
