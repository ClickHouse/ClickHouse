"""Rewrite the MergeTree tables of a performance test into Iceberg tables.

`perf.py --table-engine iceberg_local|iceberg_s3` runs a test with every
MergeTree-family and `Memory` table created as an `IcebergLocal` or `IcebergS3`
table instead, so the same queries measure the Iceberg read and write path. See the "Iceberg
configuration" section of `tests/performance/README.md`.

A `CREATE TABLE` is first formatted by `clickhouse format --oneline`, so the
rewrite only has to understand one canonical syntax. The rewrite:
  * replaces the engine, with one table path per test and table;
  * keeps the leading plain columns of `ORDER BY` as the Iceberg sort order;
  * drops what Iceberg has no equivalent for: `PARTITION BY` (kept out on
    purpose: partitions multiply data files and hit `iceberg_insert_max_partitions`),
    `PRIMARY KEY`, `SAMPLE BY`, `TTL`, `SETTINGS`, skip indexes, projections,
    constraints, codecs and column TTLs;
  * widens column types that Iceberg cannot store to the nearest type it can.

The engine-specific behavior of `ReplacingMergeTree`, `AggregatingMergeTree`
and the like is lost: the table keeps every inserted row. A lightweight
`UPDATE t SET ... WHERE ...` of a rewritten table, which Iceberg tables do not
support, becomes the mutation `ALTER TABLE t UPDATE ... WHERE ...`, which they do.

A column that an Iceberg table cannot store even widened (`AggregateFunction`,
`JSON`, a MATERIALIZED column) raises `IcebergRewriteError`: such a test
belongs in `tests/performance/iceberg_skip_list.txt`.
"""

import re
import subprocess

ENGINES = {
    "iceberg_local": "IcebergLocal(concat(getServerSetting('user_files_path'), '/perf_iceberg/{path}/'))",
    "iceberg_s3": "IcebergS3(perf_s3, filename = 'perf_iceberg/{path}/')",
}

# Settings for every statement of a test in this configuration: the writes are
# experimental or off by default, and a dropped table must not leave its files
# for the next test.
SETTINGS = {
    "allow_insert_into_iceberg": "1",
    "allow_experimental_iceberg_compaction": "1",
    "iceberg_delete_data_on_drop": "1",
}

# The engines of the tables that are rewritten.
REWRITTEN_ENGINE = re.compile(r"(Replicated|Shared)?\w*MergeTree|Memory")

# Top-level clauses after `ENGINE = X(...)`, as `clickhouse format` prints them.
CLAUSES = ["PARTITION BY", "PRIMARY KEY", "ORDER BY", "SAMPLE BY", "TTL", "SETTINGS", "COMMENT", "EMPTY AS", "AS"]

WIDENED = {
    "UInt8": "Int32",
    "UInt16": "Int32",
    "Int8": "Int32",
    "Int16": "Int32",
    "Bool": "Int32",
    # Iceberg has no unsigned 64-bit integer, and `CREATE` rejects `UInt64`.
    "UInt64": "Int64",
    "IPv4": "UInt32",
    "IPv6": "String",
    "Int128": "Decimal(38, 0)",
    "UInt128": "Decimal(38, 0)",
    "Int256": "Decimal(38, 0)",
    "UInt256": "Decimal(38, 0)",
}

UNSUPPORTED_TYPES = re.compile(
    r"\b(AggregateFunction|SimpleAggregateFunction|JSON|Object|Dynamic|Variant|Nested"
    r"|Point|Ring|Polygon|MultiPolygon|LineString|MultiLineString|Interval|Nothing|QBit)\b"
)


class IcebergRewriteError(Exception):
    pass


def _scan(s, start=0):
    """Yield (index, char, depth) of `s`, skipping string literals and quoted identifiers."""
    depth = 0
    i = start
    while i < len(s):
        c = s[i]
        if c in "'`\"":
            j = i + 1
            while j < len(s) and s[j] != c:
                j += 2 if s[j] == "\\" else 1
            i = j + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        yield i, c, depth
        i += 1


def _split(s):
    """Split `s` on the commas outside of parentheses."""
    parts, last = [], 0
    for i, c, d in _scan(s):
        if c == "," and d == 0:
            parts.append(s[last:i].strip())
            last = i + 1
    parts.append(s[last:].strip())
    return [p for p in parts if p]


def _group_end(s, i):
    """The index after the parenthesized group that starts at `s[i] == '('`."""
    for j, c, d in _scan(s, i):
        if c == ")" and d == 0:
            return j + 1
    raise IcebergRewriteError(f"Unbalanced parentheses in: {s}")


def _find_keyword(s, keyword):
    """The position of a top-level keyword of `s`, or -1."""
    for i, _, d in _scan(s):
        if d != 0 or not s.startswith(keyword, i):
            continue
        before = s[i - 1] if i else " "
        after = s[i + len(keyword)] if i + len(keyword) < len(s) else " "
        if not (before.isalnum() or before == "_") and not (after.isalnum() or after == "_"):
            return i
    return -1


def widen_type(t):
    """The nearest type to `t` that an Iceberg table can store."""
    m = re.fullmatch(r"(\w+)(?:\((.*)\))?", t, re.S)
    if not m:
        raise IcebergRewriteError(f"Cannot parse type {t}")
    name, args = m.group(1), m.group(2)
    if name in WIDENED:
        return WIDENED[name]
    if name == "LowCardinality":
        return widen_type(args.strip())
    if name in ("Enum", "Enum8", "Enum16", "FixedString"):
        return "String"
    if name == "DateTime64":
        # Iceberg timestamps are microseconds.
        scale, _, zone = (args or "3").partition(",")
        return f"DateTime64(6{', ' + zone.strip() if zone else ''})"
    if name in ("Nullable", "Array"):
        return f"{name}({widen_type(args.strip())})"
    if name == "Map":
        key, value = _split(args)
        return f"Map({widen_type(key)}, {widen_type(value)})"
    if name == "Tuple":
        elements = []
        for element in _split(args):
            named = re.fullmatch(r"(`[^`]+`|[A-Za-z_]\w*)\s+(.+)", element, re.S)
            elements.append(f"{named.group(1)} {widen_type(named.group(2))}" if named else widen_type(element))
        return f"Tuple({', '.join(elements)})"
    return t


def _rewrite_column(column):
    """One column of the column list, or None for an index, projection or constraint."""
    if column.split(None, 1)[0].upper() in ("INDEX", "PROJECTION", "CONSTRAINT", "STATISTICS"):
        return None
    m = re.fullmatch(r"(`[^`]+`|[A-Za-z_][\w.]*)\s+(\w+)(.*)", column, re.S)
    if not m:
        raise IcebergRewriteError(f"Cannot parse column: {column}")
    name, rest = m.group(1), m.group(2) + m.group(3)
    type_end = len(m.group(2))
    if rest[type_end:type_end + 1] == "(":
        type_end = _group_end(rest, type_end)
    column_type, rest = rest[:type_end], rest[type_end:].strip()
    if UNSUPPORTED_TYPES.search(column_type):
        raise IcebergRewriteError(f"Column {name} of type {column_type} has no Iceberg equivalent")
    result = f"{name} {widen_type(column_type)}"
    nullability = re.match(r"(NOT NULL|NULL)\b\s*", rest)
    if nullability:
        result += f" {nullability.group(1)}"
        rest = rest[nullability.end():]
    default = re.match(r"(DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL)\b\s*", rest)
    if default:
        if default.group(1) != "DEFAULT":
            raise IcebergRewriteError(f"{default.group(1)} column {name} is not supported by Iceberg tables")
        # The expression ends where the column-level clauses that are dropped begin.
        tail = rest[default.end():]
        ends = [p for p in (_find_keyword(tail, k) for k in ("CODEC", "TTL", "COMMENT", "SETTINGS", "STATISTICS")) if p != -1]
        result += f" DEFAULT {tail[:min(ends)] if ends else tail}".rstrip()
    return result


def _rewrite_order_by(expression):
    """The leading plain columns of a MergeTree sorting key, as an Iceberg sort order."""
    if expression.startswith("(") and _group_end(expression, 0) == len(expression):
        parts = _split(expression[1:-1])
    elif expression.startswith("tuple("):
        parts = _split(expression[6:-1])
    else:
        parts = [expression]
    columns = []
    for part in parts:
        if not re.fullmatch(r"(`[^`]+`|[A-Za-z_]\w*)( ASC| DESC)?", part):
            break
        columns.append(part)
    if not columns:
        return None
    return f"({', '.join(columns)})" if len(columns) > 1 else columns[0]


def _unquoted(name):
    return name.replace("`", "")


def rewrite_create(query, table_engine, test_name, rewritten_tables=()):
    """Rewrite one `CREATE TABLE` in the canonical one-line format.

    `rewritten_tables` are the tables already rewritten in this test, as they are
    named in the queries. Returns the rewritten query, or None when the query
    creates no MergeTree table."""
    m = re.match(
        r"CREATE (OR REPLACE )?TABLE (IF NOT EXISTS )?((?:`[^`]+`|\w+)(?:\.(?:`[^`]+`|\w+))?)\s*",
        query,
    )
    if not m:
        return None
    name = m.group(3)
    rest = query[m.end():]
    path = f"{test_name}/{re.sub(r'[^A-Za-z0-9_.-]', '_', _unquoted(name))}"
    engine = ENGINES[table_engine].format(path=path)

    columns = None
    if rest.startswith("("):
        end = _group_end(rest, 0)
        columns = rest[1:end - 1]
        rest = rest[end:].strip()
    source = None
    as_table = re.match(r"AS (?!SELECT\b|WITH\b|\()((?:`[^`]+`|\w+)(?:\.(?:`[^`]+`|\w+))?)\s*", rest)
    if as_table:
        source = as_table.group(1)
        rest = rest[as_table.end():]

    engine_position = _find_keyword(rest, "ENGINE")
    if engine_position == -1:
        if source is None or _unquoted(source) not in rewritten_tables:
            return None
        # `CREATE TABLE b AS a` copies the engine of `a`, its Iceberg path included,
        # so when `a` is a rewritten table, `b` needs a path of its own. A copy of
        # any other table is created as written.
        return f"{query[:m.end()]}AS {source} ENGINE = {engine}"
    head = rest[:engine_position]
    engine_clause = re.match(r"ENGINE = `?(\w+)`?", rest[engine_position:])
    if not engine_clause:
        raise IcebergRewriteError(f"Cannot parse the engine of: {query}")
    if not REWRITTEN_ENGINE.fullmatch(engine_clause.group(1)):
        return None
    after = rest[engine_position + engine_clause.end():]
    if after.startswith("("):
        after = after[_group_end(after, 0):]
    after = after.strip()

    positions = sorted((p, k) for k in CLAUSES if (p := _find_keyword(after, k)) != -1)
    clauses = {}
    for i, (position, keyword) in enumerate(positions):
        if keyword == "AS" and any(k == "EMPTY AS" and p + len("EMPTY ") == position for p, k in positions):
            continue
        end = positions[i + 1][0] if i + 1 < len(positions) else len(after)
        clauses[keyword] = after[position + len(keyword):end].strip()

    result = query[:m.end()].rstrip()
    if columns is not None:
        result += " (" + ", ".join(c for c in map(_rewrite_column, _split(columns)) if c) + ")"
    if source:
        result += f" AS {source}"
    result += f" {head.strip()} ENGINE = {engine}".replace("  ", " ")
    if "ORDER BY" in clauses:
        order_by = _rewrite_order_by(clauses["ORDER BY"])
        if order_by:
            result += f" ORDER BY {order_by}"
    if "EMPTY AS" in clauses:
        result += f" EMPTY AS {clauses['EMPTY AS']}"
    elif "AS" in clauses:
        result += f" AS {clauses['AS']}"
    return result


def format_one_line(binary, query):
    """`query` in the canonical one-line format of `clickhouse format`."""
    result = subprocess.run(
        [binary, "format", "--oneline"],
        input=query,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise IcebergRewriteError(f"clickhouse format failed on: {query}\n{result.stderr}")
    return result.stdout.strip()


LIGHTWEIGHT_UPDATE = re.compile(r"UPDATE ((?:`[^`]+`|\w+)(?:\.(?:`[^`]+`|\w+))?) SET (.+)$", re.S)


def rewrite_update(query, binary, rewritten_tables):
    """A lightweight `UPDATE` of a rewritten table as an `ALTER TABLE ... UPDATE`
    mutation; any other query as it is."""
    if not re.match(r"\s*UPDATE\s", query, re.I):
        return query
    m = LIGHTWEIGHT_UPDATE.match(format_one_line(binary, query))
    if not m or _unquoted(m.group(1)) not in rewritten_tables:
        return query
    return f"ALTER TABLE {m.group(1)} UPDATE {m.group(2)}"


def rewrite_setup_queries(queries, table_engine, test_name, binary):
    """Rewrite the `CREATE TABLE` statements among the setup queries of a test,
    and the lightweight updates of the rewritten tables.

    `queries` are substituted setup queries; the others are returned as they are.
    Returns (queries, the set of rewritten tables)."""
    result = []
    rewritten_tables = set()
    for query in queries:
        if not re.match(r"\s*CREATE\s+(OR\s+REPLACE\s+)?TABLE\b", query, re.I):
            result.append(rewrite_update(query, binary, rewritten_tables))
            continue
        canonical = format_one_line(binary, query)
        new_query = rewrite_create(canonical, table_engine, test_name, rewritten_tables)
        if new_query is None:
            result.append(query)
        else:
            result.append(new_query)
            rewritten_tables.add(_unquoted(re.match(r"CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?(\S+)", canonical).group(1)))
    return result, rewritten_tables
