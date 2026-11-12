#!/usr/bin/env python3
"""Generates random schemas and data with fastavro (an independent encoder) and
the columns that the Rust executor is expected to produce for them.

For every case i it writes, into <out dir>/random/:
  case_i.avsc   the schema (JSON text)
  case_i.bin    N rows, concatenated Avro binary datums (no container file)
  case_i.txt    first line: N; then one line per sink, `index:values`, in the
                format of tests/common/mod.rs `Buffers::dump`
  case_i.canon  Parsing Canonical Form as computed by fastavro
  case_i.fp     CRC-64-AVRO fingerprint of the canonical form, as fastavro computes it

The expected columns follow the "universal plan" of the Rust tests: every leaf
is decoded into its own sink, sinks are numbered in pre-order (a map has its
offsets sink, then its keys sink, then the sinks of its values), unions have a
tag sink, and the contents of a record that refers to itself are skipped.

Usage: gen_random.py <out dir> [cases] [seed]
"""
import io
import json
import os
import random
import struct
import sys

import fastavro
from fastavro.schema import fingerprint, parse_schema, to_parsing_canonical_form

PRIMS = ["null", "boolean", "int", "long", "float", "double", "bytes", "string"]


class Gen:
    def __init__(self, rng):
        self.rng = rng
        self.defs = {}  # fullname -> schema dict
        self.counter = 0

    def fresh(self, prefix):
        self.counter += 1
        return f"{prefix}{self.counter}"

    def ref_to(self, fullname, enclosing_ns):
        ns = fullname.rpartition(".")[0]
        simple = fullname.rpartition(".")[2]
        return simple if ns == enclosing_ns else fullname

    def gen(self, depth, ns, in_union=False, used=None, forbid_refs=()):
        r = self.rng
        opts = ["prim"] * 8
        if depth > 0:
            opts += ["record"] * 4 + ["enum", "fixed", "array", "array", "map", "map"]
            if not in_union:
                opts += ["union"] * 5
        if self.defs:
            opts += ["ref"] * 3
        kind = r.choice(opts)
        if used is None:
            used = set()
        if kind == "prim":
            p = r.choice(PRIMS)
            if p in used:
                return None
            used.add(p)
            return self.maybe_logical(p)
        if kind == "ref":
            fullname = r.choice(sorted(self.defs))
            if fullname in used:
                return None
            used.add(fullname)
            return self.ref_to(fullname, ns)
        if kind in ("array", "map"):
            if kind in used:
                return None
            used.add(kind)
            item = self.gen_required(depth - 1, ns)
            return {"type": kind, "items" if kind == "array" else "values": item}
        if kind == "union":
            branches = []
            seen = set()
            for _ in range(r.randint(1, 4)):
                b = self.gen(depth - 1, ns, in_union=True, used=seen)
                if b is not None:
                    branches.append(b)
            if r.random() < 0.7 and "null" not in seen:
                branches.insert(r.randint(0, len(branches)), "null")
            if not branches:
                branches = ["null"]
            return branches
        name = self.fresh("T")
        if kind == "enum":
            full = (ns + "." if ns else "") + name
            syms = [f"S{i}" for i in range(r.randint(1, 5))]
            d = {"type": "enum", "name": name, "symbols": syms}
            if ns:
                d["namespace"] = ns
            self.defs[full] = d
            used.add(full)
            return d
        if kind == "fixed":
            full = (ns + "." if ns else "") + name
            d = {"type": "fixed", "name": name, "size": r.choice([1, 2, 3, 4, 8, 12, 16])}
            if d["size"] == 12 and r.random() < 0.5:
                d["logicalType"] = "duration"
            if ns:
                d["namespace"] = ns
            self.defs[full] = d
            used.add(full)
            return d
        # record
        my_ns = ns
        extra = {}
        if r.random() < 0.3:
            my_ns = r.choice(["a", "a.b", "x.y.z", ""])
            extra["namespace"] = my_ns
        full = (my_ns + "." if my_ns else "") + name
        d = {"type": "record", "name": name, "fields": [], **extra}
        self.defs[full] = d  # recursion: the record is nameable inside its own body
        used.add(full)
        for i in range(r.randint(0, 4)):
            f = {"name": f"f{i}", "type": self.gen_required(depth - 1, my_ns)}
            if r.random() < 0.2:
                f["doc"] = "x"
            if r.random() < 0.2:
                f["field-id"] = 100 + i
            d["fields"].append(f)
        return d

    def gen_required(self, depth, ns):
        while True:
            t = self.gen(depth, ns)
            if t is not None:
                return t

    def maybe_logical(self, p):
        r = self.rng
        if p == "int" and r.random() < 0.3:
            return {"type": "int", "logicalType": r.choice(["date", "time-millis"])}
        if p == "long" and r.random() < 0.3:
            return {"type": "long", "logicalType": r.choice(["timestamp-millis", "timestamp-micros", "time-micros", "local-timestamp-millis"])}
        if p == "bytes" and r.random() < 0.3:
            return {"type": "bytes", "logicalType": "decimal", "precision": r.randint(1, 30), "scale": 0}
        if p == "string" and r.random() < 0.3:
            return {"type": "string", "logicalType": "uuid"}
        return p


def fullname_of(g, schema, ns):
    """Returns (fullname or None, plain type name)."""
    if isinstance(schema, str):
        if schema in PRIMS:
            return None
        return fullname_for_ref(g, schema, ns)
    if isinstance(schema, dict) and schema["type"] in ("record", "enum", "fixed"):
        n = schema["namespace"] if "namespace" in schema else ns
        return (n + "." if n else "") + schema["name"]
    return None


def fullname_for_ref(g, name, ns):
    if "." in name:
        return name
    cand = (ns + "." if ns else "") + name
    return cand if cand in g.defs else name


def resolve(g, schema, ns):
    """Follows a reference; returns (schema dict or str, enclosing ns for its body)."""
    if isinstance(schema, str) and schema not in PRIMS:
        full = fullname_for_ref(g, schema, ns)
        d = g.defs[full]
        return d, full.rpartition(".")[0]
    return schema, ns


def type_of(schema):
    if isinstance(schema, list):
        return "union"
    if isinstance(schema, dict):
        return schema["type"]
    return schema


def branch_name(g, b, ns):
    full = fullname_of(g, b, ns)
    return full if full is not None else type_of(b)


class Planner:
    """Mirror of `universal_plan` in tests/common/mod.rs."""

    def __init__(self, g):
        self.g = g
        self.nsinks = 0

    def sink(self):
        self.nsinks += 1
        return self.nsinks - 1

    def plan(self, schema, ns, path):
        g = self.g
        schema, ns = resolve(g, schema, ns)
        t = type_of(schema)
        if t == "record":
            full = (ns + "." if ns else "") + schema["name"] if "namespace" not in schema else (schema["namespace"] + "." if schema["namespace"] else "") + schema["name"]
            if full in path:
                return {"k": "skip"}
            rec_ns = full.rpartition(".")[0]
            node = {"k": "record", "children": []}
            path = path | {full}
            for f in schema["fields"]:
                node["children"].append((f["name"], self.plan(f["type"], rec_ns, path)))
            return node
        if t == "null":
            return {"k": "skip"}
        if t in ("boolean", "int", "long", "float", "double", "bytes", "string"):
            return {"k": t, "sink": self.sink()}
        if t == "fixed":
            return {"k": "fixed", "sink": self.sink()}
        if t == "enum":
            return {"k": "enum", "sink": self.sink(), "symbols": schema["symbols"]}
        if t == "array":
            node = {"k": "array", "sink": self.sink()}
            node["child"] = self.plan(schema["items"], ns, path)
            return node
        if t == "map":
            node = {"k": "map", "sink": self.sink(), "keys": self.sink()}
            node["child"] = self.plan(schema["values"], ns, path)
            return node
        if t == "union":
            node = {"k": "union", "sink": self.sink(), "children": []}
            for b in schema:
                node["children"].append(self.plan(b, ns, path))
            return node
        raise AssertionError(t)


def f32bits(v):
    return "%08x" % struct.unpack("<I", struct.pack("<f", v))[0]


def f64bits(v):
    return "%016x" % struct.unpack("<Q", struct.pack("<d", v))[0]


class Flattener:
    def __init__(self, g, nsinks):
        self.g = g
        self.cols = [[] for _ in range(nsinks)]
        self.running = {}

    def hexed(self, b):
        return b.hex() or "-"

    def flat(self, plan, schema, ns, v):
        g = self.g
        if plan["k"] == "skip":
            return
        schema, ns = resolve(g, schema, ns)
        k = plan["k"]
        cols = self.cols
        if k == "boolean":
            cols[plan["sink"]].append("1" if v else "0")
        elif k in ("int", "long"):
            cols[plan["sink"]].append(str(v))
        elif k == "float":
            cols[plan["sink"]].append(f32bits(v))
        elif k == "double":
            cols[plan["sink"]].append(f64bits(v))
        elif k == "string":
            cols[plan["sink"]].append(self.hexed(v.encode()))
        elif k in ("bytes", "fixed"):
            cols[plan["sink"]].append(self.hexed(v))
        elif k == "enum":
            cols[plan["sink"]].append(str(plan["symbols"].index(v)))
        elif k == "record":
            rec_ns = (schema["namespace"] if "namespace" in schema else ns)
            for (fname, child), f in zip(plan["children"], schema["fields"]):
                self.flat(child, f["type"], rec_ns, v[fname])
        elif k == "array":
            s = plan["sink"]
            self.running[s] = self.running.get(s, 0) + len(v)
            cols[s].append(str(self.running[s]))
            for item in v:
                self.flat(plan["child"], schema["items"], ns, item)
        elif k == "map":
            s = plan["sink"]
            self.running[s] = self.running.get(s, 0) + len(v)
            cols[s].append(str(self.running[s]))
            for key, item in v.items():
                cols[plan["keys"]].append(self.hexed(key.encode()))
                self.flat(plan["child"], schema["values"], ns, item)
        elif k == "union":
            name, inner = v
            idx = next(i for i, b in enumerate(schema) if branch_name(g, b, ns) == name)
            cols[plan["sink"]].append(str(idx))
            self.flat(plan["children"][idx], schema[idx], ns, inner)
        else:
            raise AssertionError(k)


class Values:
    def __init__(self, g, rng):
        self.g = g
        self.rng = rng

    def value(self, schema, ns, budget):
        g, r = self.g, self.rng
        schema, ns = resolve(g, schema, ns)
        t = type_of(schema)
        if t == "null":
            return None
        if t == "boolean":
            return r.random() < 0.5
        if t == "int":
            return r.choice([0, 1, -1, 63, 64, -65, 2**31 - 1, -(2**31), r.randint(-1000, 1000), r.randint(-(2**31), 2**31 - 1)])
        if t == "long":
            return r.choice([0, 1, -1, 2**63 - 1, -(2**63), r.randint(-1000, 1000), r.randint(-(2**63), 2**63 - 1)])
        if t == "float":
            return struct.unpack("<f", struct.pack("<f", r.choice([0.0, -0.0, 1.5, -3.25, 1e30, 1e-30, r.uniform(-1e6, 1e6)])))[0]
        if t == "double":
            return r.choice([0.0, -0.0, 1.5, 1e300, -1e-300, r.uniform(-1e12, 1e12)])
        if t == "bytes":
            return bytes(r.randrange(256) for _ in range(r.choice([0, 0, 1, 2, 5, 40])))
        if t == "string":
            return "".join(r.choice("abcXYZ09 \u00e9\u4e2d\U0001F600") for _ in range(r.choice([0, 0, 1, 3, 12])))
        if t == "fixed":
            return bytes(r.randrange(256) for _ in range(schema["size"]))
        if t == "enum":
            return r.choice(schema["symbols"])
        if t == "record":
            rec_ns = schema["namespace"] if "namespace" in schema else ns
            return {f["name"]: self.value(f["type"], rec_ns, budget - 1) for f in schema["fields"]}
        if t == "array":
            n = 0 if budget <= 0 else r.choice([0, 1, 2, 3, 8])
            return [self.value(schema["items"], ns, budget - 1) for _ in range(n)]
        if t == "map":
            n = 0 if budget <= 0 else r.choice([0, 1, 2, 4])
            return {f"k{i}": self.value(schema["values"], ns, budget - 1) for i in range(n)}
        if t == "union":
            idxs = list(range(len(schema)))
            if budget <= 0:
                # Prefer a branch that ends the recursion.
                easy = [i for i in idxs if type_of(resolve(g, schema[i], ns)[0]) in ("null", "array", "map", "boolean", "int", "long", "float", "double", "string", "bytes", "enum", "fixed")]
                idxs = easy or idxs
            i = r.choice(idxs)
            return (branch_name(g, schema[i], ns), self.value(schema[i], ns, budget - 1))
        raise AssertionError(t)


def main():
    out = sys.argv[1]
    cases = int(sys.argv[2]) if len(sys.argv) > 2 else 400
    seed = int(sys.argv[3]) if len(sys.argv) > 3 else 20261101
    os.makedirs(os.path.join(out, "random"), exist_ok=True)
    made = 0
    i = 0
    while made < cases:
        rng = random.Random(seed * 100003 + i)
        i += 1
        g = Gen(rng)
        top = g.gen_required(rng.randint(2, 5), "")
        # The top level must be a record for the Python planner's namespace handling to match.
        if not (isinstance(top, dict) and top["type"] == "record") and rng.random() < 0.7:
            top = {"type": "record", "name": "Top", "fields": [{"name": "v", "type": top}]}
            g.defs["Top"] = top
        try:
            parsed = parse_schema(json.loads(json.dumps(top)))
        except Exception as e:
            sys.stderr.write(f"case {i}: fastavro rejected the generated schema: {e}\n")
            continue
        ns0 = ""
        planner = Planner(g)
        plan = planner.plan(top, ns0, frozenset())
        vals = Values(g, rng)
        rows = rng.choice([1, 1, 2, 5, 20])
        buf = io.BytesIO()
        flat = Flattener(g, planner.nsinks)
        try:
            for _ in range(rows):
                v = vals.value(top, ns0, 6)
                fastavro.schemaless_writer(buf, parsed, v)
                flat.flat(plan, top, ns0, v)
        except Exception as e:
            sys.stderr.write(f"case {i}: could not encode: {type(e).__name__}: {e}\n")
            continue
        base = os.path.join(out, "random", f"case_{made}")
        text = json.dumps(top)
        with open(base + ".avsc", "w") as f:
            f.write(text)
        with open(base + ".bin", "wb") as f:
            f.write(buf.getvalue())
        with open(base + ".txt", "w") as f:
            f.write(f"{rows}\n")
            for n, col in enumerate(flat.cols):
                f.write(f"{n}:{' '.join(col)}\n")
        canon = to_parsing_canonical_form(parsed)
        with open(base + ".canon", "w") as f:
            f.write(canon)
        with open(base + ".fp", "w") as f:
            f.write(str(fingerprint(canon, "CRC-64-AVRO")))
        made += 1
    print(f"{made} cases written")


if __name__ == "__main__":
    main()
