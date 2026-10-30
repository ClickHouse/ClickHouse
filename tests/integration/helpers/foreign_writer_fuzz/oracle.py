"""Differential oracle: ClickHouse reads compared with the writer's own read of the same snapshot.

Checks per step and ClickHouse source:

* `content`: the full row multiset (`SELECT <all columns>`),
* `count`: `count()` with and without the trivial-count optimization (which answers from metadata),
* `aggregates`: `count(c)` for every primitive column and `min`/`max` for orderable ones,
* `filter`: rows and `count()` under random predicates, which exercises partition pruning, min-max
  pruning, Parquet filter push-down and the Delta engine predicate,
* `time_travel`: the full content of an older snapshot that is still alive.

Every query runs with a random variant of the read settings that select those optimizations, so a
mismatch that only shows up with, say, `use_iceberg_partition_pruning = 1` is still found. When a
check fails, the failing query is re-run with all optimizations off and with default settings, and
both outcomes are attached to the report.
"""

import collections
import hashlib

from .context import ClickHouseQueryError
from .ltypes import LType
from .predicates import evaluate, random_predicate, render
from .writers.spark_common import q

READ_SETTINGS_COMMON = {
    "max_threads": [1, 4],
    "max_block_size": [7, 65409],
    "input_format_parquet_filter_push_down": [0, 1],
    "input_format_parquet_bloom_filter_push_down": [0, 1],
    "input_format_parquet_page_filter_push_down": [0, 1],
    "input_format_parquet_use_native_reader_v3": [0, 1],
    "use_query_condition_cache": [0, 1],
    "optimize_move_to_prewhere": [0, 1],
    "query_plan_optimize_prewhere": [0, 1],
}

READ_SETTINGS_FAMILY = {
    "iceberg": {
        "use_iceberg_partition_pruning": [0, 1],
        "use_iceberg_manifest_list_partition_pruning": [0, 1],
        "use_iceberg_metadata_files_cache": [0, 1],
        "use_iceberg_manifest_statistics": [0, 1],
    },
    "delta": {
        "delta_lake_enable_engine_predicate": [0, 1],
    },
    "hudi": {},
    "paimon": {},
}

# Every optimization off: the reference re-run of a failing query.
SAFE_SETTINGS = {
    "max_threads": 1,
    "input_format_parquet_filter_push_down": 0,
    "input_format_parquet_bloom_filter_push_down": 0,
    "input_format_parquet_page_filter_push_down": 0,
    "use_query_condition_cache": 0,
    "optimize_move_to_prewhere": 0,
    "query_plan_optimize_prewhere": 0,
    "optimize_trivial_count_query": 0,
    "use_iceberg_partition_pruning": 0,
    "use_iceberg_manifest_list_partition_pruning": 0,
    "use_iceberg_metadata_files_cache": 0,
    "use_iceberg_manifest_statistics": 0,
    "delta_lake_enable_engine_predicate": 0,
}

ORDERABLE_KINDS = (
    "int",
    "long",
    "decimal",
    "date",
    "timestamp",
    "timestamp_ntz",
    "string",
    "binary",
)


class FuzzFailure(Exception):
    def __init__(self, kind, message, details=None):
        super().__init__(message)
        self.kind = kind
        self.message = message
        self.details = details or {}

    def signature(self):
        return (self.kind, self.details.get("source"), self.details.get("error_code"))


class SchemaMismatch(Exception):
    pass


def canonical_rows(table, columns):
    """Rows of a pyarrow table as tuples of canonical values, in the order of `columns`."""
    if table is None:
        return []
    present = set(table.column_names)
    missing = [n for n, _ in columns if n not in present]
    if missing:
        raise SchemaMismatch(f"columns {missing} are missing; got {table.column_names}")
    data = [table.column(n).to_pylist() for n, _ in columns]
    types = [t for _, t in columns]
    return [tuple(t.canon(v) for t, v in zip(types, row)) for row in zip(*data)]


def sort_key(row):
    return tuple((0, "") if v is None else (1, repr(v)) for v in row)


def content_hash(rows):
    h = hashlib.sha256()
    for row in sorted(rows, key=sort_key):
        h.update(repr(row).encode())
    return h.hexdigest()[:16]


def diff_rows(expected, actual, limit=5):
    e, a = collections.Counter(expected), collections.Counter(actual)
    missing = list((e - a).elements())
    extra = list((a - e).elements())
    return {
        "expected_rows": len(expected),
        "actual_rows": len(actual),
        "missing_in_clickhouse": [
            repr(r) for r in sorted(missing, key=sort_key)[:limit]
        ],
        "missing_count": len(missing),
        "extra_in_clickhouse": [repr(r) for r in sorted(extra, key=sort_key)[:limit]],
        "extra_count": len(extra),
    }


def error_code(text):
    import re

    m = re.search(r"\(([A-Z][A-Z0-9_]{2,})\)", text)
    return m.group(1) if m else None


class Oracle:
    def __init__(self, ctx, num_predicates=4):
        self.ctx = ctx
        self.num_predicates = num_predicates
        self.queries = 0

    # ----------------------------------------------------------------- settings
    def settings_variant(self, rng, family):
        matrix = dict(READ_SETTINGS_COMMON)
        matrix.update(READ_SETTINGS_FAMILY.get(family, {}))
        result = {}
        for name, values in sorted(matrix.items()):
            if self.ctx.known_settings and name not in self.ctx.known_settings:
                continue
            if rng.random() < 0.5:
                result[name] = rng.choice(values)
        return result

    def _safe(self, family, extra=None):
        result = {
            k: v
            for k, v in SAFE_SETTINGS.items()
            if not self.ctx.known_settings or k in self.ctx.known_settings
        }
        if extra:
            result.update(extra)
        return result

    # ----------------------------------------------------------------- queries
    def _select(self, source, columns, where=None):
        cols = ", ".join(q(n) for n, _ in columns)
        sql = f"SELECT {cols} FROM {source.expression}"
        if where:
            sql += f" WHERE {where}"
        return sql

    def _run_rows(self, sql, columns, settings, source, check):
        self.queries += 1
        try:
            return canonical_rows(self.ctx.query_arrow(sql, settings), columns)
        except ClickHouseQueryError as e:
            text = str(e)
            raise FuzzFailure(
                "query_error",
                f"ClickHouse failed to read {source.label} during the {check} check: {text[:3000]}",
                {
                    "source": source.label,
                    "error_code": error_code(text),
                    "query": sql,
                    "settings": settings,
                },
            ) from None
        except SchemaMismatch as e:
            raise FuzzFailure(
                "schema",
                f"{source.label}: {e}",
                {"source": source.label, "query": sql, "settings": settings},
            ) from None

    def _run_scalar(self, sql, settings, source, check):
        self.queries += 1
        try:
            return self.ctx.query(sql, settings).strip()
        except ClickHouseQueryError as e:
            text = str(e)
            raise FuzzFailure(
                "query_error",
                f"ClickHouse failed to read {source.label} during the {check} check: {text[:3000]}",
                {
                    "source": source.label,
                    "error_code": error_code(text),
                    "query": sql,
                    "settings": settings,
                },
            ) from None

    def _triage(self, run, family, settings, extra=None):
        """Outcome of a failing query re-run with all optimizations off, and with default settings."""
        notes = {}
        for label, s in (
            ("all optimizations off", self._safe(family, extra)),
            ("default settings", dict(extra or {})),
        ):
            try:
                notes[label] = run(s)
            except FuzzFailure as e:
                notes[label] = f"error: {e.message[:500]}"
        return notes

    # ----------------------------------------------------------------- checks
    def check(
        self, writer, columns, expected_rows, rng, step, deep=True, extra_settings=None
    ):
        """Raise `FuzzFailure` on the first disagreement. `extra_settings` selects a snapshot."""
        extra = dict(extra_settings or {})
        for index, source in enumerate(writer.sources()):
            if extra and not source.time_travel:
                continue
            self._check_content(source, columns, expected_rows, rng, step, extra)
            if extra:
                continue
            self._check_count(writer, source, expected_rows, rng, step)
            if deep and index == 0:
                self._check_aggregates(source, columns, expected_rows, rng, step)
                self._check_filters(source, columns, expected_rows, rng, step)

    def _fail(self, kind, source, step, message, sql, settings, details, triage):
        info = {
            "source": source.label,
            "query": sql,
            "settings": settings,
            "triage": triage,
        }
        info.update(details)
        raise FuzzFailure(kind, f"{source.label}: {message} after {step}", info)

    def _check_content(self, source, columns, expected_rows, rng, step, extra):
        settings = self.settings_variant(rng, source.family)
        settings.update(extra)
        sql = self._select(source, columns)
        actual = self._run_rows(sql, columns, settings, source, "content")
        if collections.Counter(actual) != collections.Counter(expected_rows):
            kind = "time_travel" if extra else "content"
            run = lambda s: diff_rows(
                expected_rows, self._run_rows(sql, columns, s, source, "content")
            )
            self._fail(
                kind,
                source,
                step,
                "full content differs from the writer's read",
                sql,
                settings,
                diff_rows(expected_rows, actual),
                self._triage(run, source.family, settings, extra),
            )

    def _check_count(self, writer, source, expected_rows, rng, step):
        sql = f"SELECT count() FROM {source.expression}"
        modes = [0, 1] if writer.trivial_count_reliable() else [0]
        for trivial in modes:
            settings = self.settings_variant(rng, source.family)
            settings["optimize_trivial_count_query"] = trivial
            actual = self._run_scalar(sql, settings, source, "count")
            if actual != str(len(expected_rows)):
                run = lambda s: self._run_scalar(sql, s, source, "count")
                self._fail(
                    "count",
                    source,
                    step,
                    f"count() = {actual}, writer has {len(expected_rows)} rows",
                    sql,
                    settings,
                    {},
                    self._triage(run, source.family, settings),
                )

    def _check_aggregates(self, source, columns, expected_rows, rng, step):
        exprs, expected = [], []
        out_columns = []
        for i, (name, t) in enumerate(columns):
            if not t.is_primitive:
                continue
            values = [row[i] for row in expected_rows if row[i] is not None]
            exprs.append(f"count({q(name)})")
            expected.append(len(values))
            out_columns.append((f"count_{len(exprs)}", LType("long")))
            if t.kind in ORDERABLE_KINDS:
                exprs.append(f"min({q(name)})")
                expected.append(min(values) if values else None)
                out_columns.append((f"min_{len(exprs)}", t))
                exprs.append(f"max({q(name)})")
                expected.append(max(values) if values else None)
                out_columns.append((f"max_{len(exprs)}", t))
        if not exprs:
            return
        sql = f"SELECT {', '.join(f'{e} AS {n}' for e, (n, _) in zip(exprs, out_columns))} FROM {source.expression}"
        settings = self.settings_variant(rng, source.family)
        rows = self._run_rows(sql, out_columns, settings, source, "aggregates")
        actual = list(rows[0]) if rows else []
        # `min`/`max` over no values is NULL for a `Nullable` column but the default for the non-nullable `id`.
        ignore = {
            i
            for i, (e, x) in enumerate(zip(exprs, expected))
            if x is None and not e.startswith("count")
        }
        actual = [None if i in ignore else a for i, a in enumerate(actual)]
        if actual != expected:
            mismatches = [
                f"{e}: ClickHouse {a!r}, writer {x!r}"
                for e, a, x in zip(exprs, actual, expected)
                if a != x
            ]
            run = lambda s: self._run_rows(sql, out_columns, s, source, "aggregates")
            self._fail(
                "aggregates",
                source,
                step,
                "; ".join(mismatches[:5]),
                sql,
                settings,
                {},
                self._triage(run, source.family, settings),
            )

    def _check_filters(self, source, columns, expected_rows, rng, step):
        names = [n for n, _ in columns]
        for _ in range(self.num_predicates):
            pred = random_predicate(rng, columns)
            if pred is None:
                return
            where = render(pred, "clickhouse")
            expected = [
                row
                for row in expected_rows
                if evaluate(pred, dict(zip(names, row))) is True
            ]
            settings = self.settings_variant(rng, source.family)
            sql = self._select(source, columns, where)
            actual = self._run_rows(sql, columns, settings, source, "filter")
            if collections.Counter(actual) != collections.Counter(expected):
                run = lambda s: diff_rows(
                    expected, self._run_rows(sql, columns, s, source, "filter")
                )
                self._fail(
                    "filter",
                    source,
                    step,
                    f"WHERE {where} differs from the writer's rows filtered by the predicate",
                    sql,
                    settings,
                    diff_rows(expected, actual),
                    self._triage(run, source.family, settings),
                )
            count_sql = f"SELECT count() FROM {source.expression} WHERE {where}"
            settings = self.settings_variant(rng, source.family)
            actual_count = self._run_scalar(count_sql, settings, source, "filter")
            if actual_count != str(len(expected)):
                run = lambda s: self._run_scalar(count_sql, s, source, "filter")
                self._fail(
                    "filter",
                    source,
                    step,
                    f"count() WHERE {where} = {actual_count}, expected {len(expected)}",
                    count_sql,
                    settings,
                    {},
                    self._triage(run, source.family, settings),
                )
