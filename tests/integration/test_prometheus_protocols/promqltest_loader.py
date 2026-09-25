"""Parse upstream Prometheus ``promqltest`` files into eval scenarios.

The loader does not talk to ClickHouse. The integration test applies each
scenario with ``prometheusQuery`` / ``prometheusQueryRange``.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional


SNAPSHOT_DIR = Path(__file__).resolve().parent / "promqltest"
TESTDATA_DIR = SNAPSHOT_DIR / "testdata"
TABLE_NAME = "promqltest_scenario"

FLOAT_FRACTION = 0.00001
FLOAT_MARGIN = 0.0001

_DURATION_TOKEN = re.compile(r"([0-9]*\.?[0-9]+)(ms|s|m|h|d|w|y)")
_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "w": 604800.0,
    "y": 365 * 86400.0,
}
_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
_MATCHER_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|!~|!=|=)\s*"((?:[^"\\]|\\.)*)"')
_METRIC_NAME_RE = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
_NUMBER_RE = r"[+-]?(?:Inf|inf|NaN|nan|[0-9]*\.?[0-9]+(?:e[+-]?[0-9]+)?)"
_EXPAND_RE = re.compile(
    rf"^({_NUMBER_RE})([+-])({_NUMBER_RE})x([0-9]+)$",
    re.IGNORECASE,
)
_REPEAT_RE = re.compile(rf"^({_NUMBER_RE})x([0-9]+)$", re.IGNORECASE)
_DURATION_VALUE_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?)(?:ms|s|m|h|d|w|y)")
_COMMAND_START = re.compile(
    r"^(load(?:_with_nhcb)?|clear|eval_fail|eval)\b", re.IGNORECASE
)


class PromqltestParseError(ValueError):
    pass


@dataclass
class Sample:
    offset_index: int
    value: Optional[float]
    missing: bool = False
    stale: bool = False
    native_histogram: bool = False
    raw: str = ""


@dataclass
class SeriesSpec:
    metric: str
    labels: dict[str, str]
    samples: list[Sample]
    native_histogram: bool = False
    start_timestamp: bool = False

    def label_map(self) -> dict[str, str]:
        out = dict(self.labels)
        if self.metric and "__name__" not in out:
            out["__name__"] = self.metric
        return out


@dataclass
class EvalCase:
    eval_id: str
    file_name: str
    line: int
    kind: str
    expr: str
    time_s: float
    start_s: float = 0.0
    end_s: float = 0.0
    step_s: float = 0.0
    expect_fail: bool = False
    expect_fail_diagnostic: bool = False
    expect_ordered: bool = False
    expect_string: Optional[str] = None
    expect_range_vector: bool = False
    annotation_asserts: list[str] = field(default_factory=list)
    expected_series: list[SeriesSpec] = field(default_factory=list)
    expected_scalar: Optional[float] = None
    has_scalar: bool = False
    native_histogram: bool = False
    load_with_nhcb: bool = False
    native_hist_series: list[dict[str, str]] = field(default_factory=list)
    start_ts_metric_names: set[str] = field(default_factory=set)
    unparsed_expected: bool = False
    stale_markers: bool = False

    def exclusion_reason(self) -> Optional[str]:
        if self.load_with_nhcb:
            return "load_with_nhcb"
        if self.native_histogram:
            return "native_histogram_expected_or_loaded"
        if self.stale_markers:
            return "stale_marker"
        if self.expect_string is not None:
            return "string_literal"
        if self.expect_range_vector:
            return "range_vector_instant"
        if self.expect_fail_diagnostic:
            return "expect_fail_diagnostic"
        if self.annotation_asserts:
            return "annotation_assertion"
        query_names = set(_METRIC_NAME_RE.findall(self.expr))
        if any(
            selector_matches(matchers, labels)
            for matchers in parse_selectors(self.expr)
            for labels in self.native_hist_series
        ):
            return "query_uses_native_histogram_metric"
        if query_names & self.start_ts_metric_names or self.unparsed_expected:
            return "start_timestamp"
        return None

    def annotation_excluded(self) -> bool:
        return bool(self.annotation_asserts)


@dataclass
class LoadBlock:
    interval_s: float
    series: list[SeriesSpec]
    with_nhcb: bool = False
    line: int = 0


@dataclass
class Scenario:
    file_name: str
    line: int
    commands: list[LoadBlock | EvalCase]
    native_hist_series: list[dict[str, str]] = field(default_factory=list)
    start_ts_metric_names: set[str] = field(default_factory=set)
    load_with_nhcb: bool = False

    @property
    def loads(self) -> list[LoadBlock]:
        return [c for c in self.commands if isinstance(c, LoadBlock)]

    @property
    def evals(self) -> list[EvalCase]:
        return [c for c in self.commands if isinstance(c, EvalCase)]


def load_snapshot_meta(snapshot_dir: Path = SNAPSHOT_DIR) -> dict[str, Any]:
    path = snapshot_dir / "snapshot.json"
    return json.loads(path.read_text())


def snapshot_test_files(snapshot_dir: Path = SNAPSHOT_DIR) -> list[Path]:
    meta = load_snapshot_meta(snapshot_dir)
    testdata = snapshot_dir / "testdata"
    return [testdata / name for name in meta["included_files"]]


def parse_duration(text: str) -> float:
    s = text.strip()
    if not s:
        raise PromqltestParseError("empty duration")
    if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", s):
        return float(s)
    total = 0.0
    pos = 0
    for m in _DURATION_TOKEN.finditer(s):
        if m.start() != pos:
            raise PromqltestParseError(f"invalid duration {text!r}")
        total += float(m.group(1)) * _UNITS[m.group(2)]
        pos = m.end()
    if pos != len(s):
        raise PromqltestParseError(f"invalid duration {text!r}")
    return total


def _unescape_label(value: str) -> str:
    return value.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n")


def _skip_quoted_braces(s: str, start: int) -> int:
    if start >= len(s) or s[start] != "{":
        raise PromqltestParseError(f"expected '{{' in {s!r}")
    depth = 0
    in_str = False
    escape = False
    for j in range(start, len(s)):
        c = s[j]
        if in_str:
            if escape:
                escape = False
            elif c == "\\":
                escape = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return j + 1
    raise PromqltestParseError(f"unterminated labels in {s!r}")


def split_ident_and_values(line: str) -> tuple[str, str, bool]:
    s = line.strip()
    i = 0
    if i < len(s) and (s[i].isalpha() or s[i] in "_:"):
        while i < len(s) and (s[i].isalnum() or s[i] in "_:"):
            i += 1
    if i < len(s) and s[i] == "{":
        i = _skip_quoted_braces(s, i)
    start_ts = False
    if s.startswith("@st", i):
        start_ts = True
        i += 3
    if i < len(s) and not s[i].isspace():
        return s[:i], s[i:].strip(), start_ts
    return s[:i], s[i:].strip(), start_ts


def parse_metric_and_labels(ident: str) -> tuple[str, dict[str, str]]:
    ident = ident.strip()
    if ident.endswith("@st"):
        ident = ident[:-3]
    if not ident:
        return "", {}
    if ident.startswith("{"):
        return "", {k: _unescape_label(v) for k, v in _LABEL_RE.findall(ident)}
    brace = ident.find("{")
    if brace < 0:
        return ident, {}
    name = ident[:brace]
    labels = {k: _unescape_label(v) for k, v in _LABEL_RE.findall(ident[brace:])}
    return name, labels


def parse_selectors(expr: str) -> list[list[tuple[str, str, str]]]:
    """Split a query into vector selectors, each a list of (label, op, value) matchers."""
    selectors: list[list[tuple[str, str, str]]] = []
    i = 0
    while i < len(expr):
        char = expr[i]
        if char in "\"'`":
            i += 1
            while i < len(expr) and expr[i] != char:
                i += 2 if expr[i] == "\\" else 1
            i += 1
            continue
        matchers: list[tuple[str, str, str]] = []
        name = _METRIC_NAME_RE.match(expr, i)
        if name and (i == 0 or not (expr[i - 1].isalnum() or expr[i - 1] in "_:")):
            i = name.end()
            if i < len(expr) and expr[i] == "(":
                continue
            matchers.append(("__name__", "=", name.group(0)))
        elif char != "{":
            i += 1
            continue
        if i < len(expr) and expr[i] == "{":
            end = _skip_quoted_braces(expr, i)
            matchers.extend(
                (label, op, _unescape_label(value))
                for label, op, value in _MATCHER_RE.findall(expr[i:end])
            )
            i = end
        if matchers:
            selectors.append(matchers)
    return selectors


def selector_matches(matchers: list[tuple[str, str, str]], labels: dict[str, str]) -> bool:
    for label, op, value in matchers:
        if op in ("=~", "!~"):
            hit = re.fullmatch(value, labels.get(label, "")) is not None
        else:
            hit = labels.get(label, "") == value
        if hit != (op in ("=", "=~")):
            return False
    return True


def _parse_number(token: str) -> float:
    low = token.lower()
    if low in ("+inf", "inf"):
        return float("inf")
    if low == "-inf":
        return float("-inf")
    if low == "nan":
        return float("nan")
    return float(token)


def tokenize_samples(raw: str) -> list[str]:
    tokens: list[str] = []
    i = 0
    s = raw.strip()
    while i < len(s):
        if s[i].isspace():
            i += 1
            continue
        hist = s.find("{{", i)
        if hist >= i and not any(c.isspace() for c in s[i:hist]):
            j = hist
            while True:
                close = s.find("}}", j)
                if close < 0:
                    raise PromqltestParseError(f"unterminated histogram in {raw!r}")
                j = close + 2
                rest = s[j:]
                space = re.search(r"\s", rest)
                nxt = rest.find("{{")
                plus_hist = rest.startswith("+{{") or rest.startswith("-{{")
                if plus_hist or (nxt >= 0 and (space is None or nxt < space.start())):
                    j = j + (0 if plus_hist else nxt)
                    if plus_hist:
                        pass
                    continue
                x_m = re.match(r"x[0-9]+", rest)
                if x_m:
                    j += x_m.end()
                tokens.append(s[i:j])
                i = j
                break
            continue
        j = i
        while j < len(s) and not s[j].isspace():
            j += 1
        tokens.append(s[i:j])
        i = j
    return tokens


def expand_sample_token(token: str) -> list[Sample]:
    if token == "_":
        return [Sample(offset_index=0, value=None, missing=True, raw=token)]
    if token.lower() == "stale":
        return [Sample(offset_index=0, value=float("nan"), stale=True, raw=token)]
    if token.startswith("{{") or "{{" in token:
        m = re.search(r"x([0-9]+)$", token)
        count = int(m.group(1)) + 1 if m else 1
        return [
            Sample(
                offset_index=i,
                value=None,
                native_histogram=True,
                raw=token,
            )
            for i in range(count)
        ]
    m = _EXPAND_RE.match(token)
    if m:
        start = _parse_number(m.group(1))
        sign = 1.0 if m.group(2) == "+" else -1.0
        step = _parse_number(m.group(3)) * sign
        extra = int(m.group(4))
        out = []
        for i in range(extra + 1):
            out.append(Sample(offset_index=i, value=start + step * i, raw=token))
        return out
    m = _REPEAT_RE.match(token)
    if m:
        value = _parse_number(m.group(1))
        extra = int(m.group(2))
        return [Sample(offset_index=i, value=value, raw=token) for i in range(extra + 1)]
    if _DURATION_VALUE_RE.match(token):
        m = re.search(r"x([0-9]+)$", token)
        count = int(m.group(1)) + 1 if m else 1
        return [
            Sample(offset_index=i, value=None, raw=token) for i in range(count)
        ]
    return [Sample(offset_index=0, value=_parse_number(token), raw=token)]


def parse_series_line(line: str) -> SeriesSpec:
    ident, values, start_ts = split_ident_and_values(line)
    metric, labels = parse_metric_and_labels(ident)
    samples: list[Sample] = []
    native = False
    if not values and ident:
        raise PromqltestParseError(f"bad series line {line!r}")
    for token in tokenize_samples(values):
        part = expand_sample_token(token)
        for sample in part:
            sample.offset_index = len(samples)
            samples.append(sample)
            if sample.native_histogram:
                native = True
    return SeriesSpec(
        metric=metric,
        labels=labels,
        samples=samples,
        native_histogram=native,
        start_timestamp=start_ts,
    )


def parse_expected_scalar_line(line: str) -> Optional[float]:
    token = line.strip()
    if not token or token.startswith("#") or token.startswith("expect "):
        return None
    if "{" in token or token.endswith("x") or " " in token:
        return None
    try:
        return _parse_number(token)
    except ValueError:
        return None


def _strip_comment(line: str) -> str:
    s = line.rstrip("\n")
    if s.lstrip().startswith("#"):
        return ""
    return s


def _split_eval_header(rest: str) -> tuple[str, str, dict[str, float]]:
    parts = rest.split()
    if not parts:
        raise PromqltestParseError(f"empty eval header {rest!r}")
    kind = parts[0]
    if kind == "instant":
        if len(parts) < 4 or parts[1] != "at":
            raise PromqltestParseError(f"bad instant eval {rest!r}")
        time_s = parse_duration(parts[2])
        expr = rest.split(None, 3)[3]
        return "instant", expr, {"time_s": time_s}
    if kind == "range":
        # eval range from <a> to <b> step <c> <expr>
        try:
            from_i = parts.index("from")
            to_i = parts.index("to")
            step_i = parts.index("step")
        except ValueError as e:
            raise PromqltestParseError(f"bad range eval {rest!r}") from e
        start_s = parse_duration(parts[from_i + 1])
        end_s = parse_duration(parts[to_i + 1])
        step_s = parse_duration(parts[step_i + 1])
        expr = rest.split(None, 7)[7]
        return "range", expr, {"start_s": start_s, "end_s": end_s, "step_s": step_s, "time_s": end_s}
    raise PromqltestParseError(f"unknown eval kind in {rest!r}")


def parse_test_file(path: Path) -> list[Scenario]:
    lines = path.read_text().splitlines()
    file_name = path.name
    scenarios: list[Scenario] = []
    current = Scenario(file_name=file_name, line=1, commands=[])
    pending_eval: Optional[EvalCase] = None
    eval_index = 0

    def finish_eval() -> None:
        nonlocal pending_eval
        if pending_eval is None:
            return
        pending_eval.native_hist_series = list(current.native_hist_series)
        pending_eval.start_ts_metric_names = set(current.start_ts_metric_names)
        pending_eval.load_with_nhcb = current.load_with_nhcb
        if any(s.native_histogram for s in pending_eval.expected_series):
            pending_eval.native_histogram = True
        pending_eval.stale_markers = any(
            sample.stale
            for block in current.loads
            for series in block.series
            for sample in series.samples
        ) or any(
            sample.stale
            for series in pending_eval.expected_series
            for sample in series.samples
        )
        current.commands.append(pending_eval)
        pending_eval = None

    def finish_scenario() -> None:
        nonlocal current
        finish_eval()
        if current.commands:
            scenarios.append(current)
        current = Scenario(file_name=file_name, line=1, commands=[])

    i = 0
    while i < len(lines):
        raw = lines[i]
        line_no = i + 1
        stripped = _strip_comment(raw).strip()
        if not stripped:
            i += 1
            continue
        cmd = _COMMAND_START.match(stripped)
        if cmd:
            finish_eval()
            name = cmd.group(1).lower()
            if name in ("load", "load_with_nhcb"):
                tokens = stripped.split()
                interval = parse_duration(tokens[1]) if len(tokens) > 1 else 0.0
                with_nhcb = name == "load_with_nhcb" or "with_nhcb" in tokens
                block = LoadBlock(interval_s=interval, series=[], with_nhcb=with_nhcb, line=line_no)
                i += 1
                while i < len(lines):
                    nxt = _strip_comment(lines[i]).strip()
                    if not nxt:
                        i += 1
                        continue
                    if _COMMAND_START.match(nxt) or nxt.startswith("expect "):
                        break
                    try:
                        series = parse_series_line(nxt)
                    except (PromqltestParseError, ValueError):
                        series = SeriesSpec(
                            metric="",
                            labels={},
                            samples=[],
                            native_histogram="{{" in nxt,
                            start_timestamp="@st" in nxt or _DURATION_VALUE_RE.search(nxt) is not None,
                        )
                        name_m = _METRIC_NAME_RE.match(nxt)
                        if name_m:
                            series.metric = name_m.group(0)
                    block.series.append(series)
                    if series.native_histogram:
                        current.native_hist_series.append(series.label_map())
                    if series.start_timestamp:
                        current.start_ts_metric_names.add(series.metric)
                    i += 1
                current.commands.append(block)
                if with_nhcb:
                    current.load_with_nhcb = True
                continue
            if name == "clear":
                finish_scenario()
                current.line = line_no
                i += 1
                continue
            fail = name == "eval_fail"
            rest = stripped.split(None, 1)[1]
            kind, expr, times = _split_eval_header(rest)
            eval_index += 1
            pending_eval = EvalCase(
                eval_id=f"{file_name}:{line_no}",
                file_name=file_name,
                line=line_no,
                kind=kind,
                expr=expr,
                time_s=times.get("time_s", 0.0),
                start_s=times.get("start_s", 0.0),
                end_s=times.get("end_s", 0.0),
                step_s=times.get("step_s", 0.0),
                expect_fail=fail,
            )
            i += 1
            continue

        if stripped.startswith("expect "):
            if pending_eval is None:
                raise PromqltestParseError(f"{file_name}:{line_no}: expect without eval")
            body = stripped[len("expect ") :].strip()
            if body.startswith("fail"):
                pending_eval.expect_fail = True
                if body[len("fail") :].strip():
                    pending_eval.expect_fail_diagnostic = True
            elif body.startswith("ordered"):
                pending_eval.expect_ordered = True
            elif body.startswith("string "):
                pending_eval.expect_string = _parse_expect_string(body[len("string ") :])
            elif body.startswith("range vector"):
                pending_eval.expect_range_vector = True
            elif body.startswith(("warn", "no_warn", "info", "no_info")):
                pending_eval.annotation_asserts.append(body)
            else:
                raise PromqltestParseError(f"{file_name}:{line_no}: unknown expect {body!r}")
            i += 1
            continue

        if pending_eval is not None:
            scalar = parse_expected_scalar_line(stripped)
            if (
                scalar is not None
                and "{" not in stripped
                and not pending_eval.expected_series
                and " " not in stripped.strip()
            ):
                pending_eval.expected_scalar = scalar
                pending_eval.has_scalar = True
            else:
                try:
                    spec = parse_series_line(stripped)
                except (PromqltestParseError, ValueError):
                    if "{{" in stripped:
                        pending_eval.native_histogram = True
                    else:
                        pending_eval.unparsed_expected = True
                    i += 1
                    continue
                pending_eval.expected_series.append(spec)
                if spec.native_histogram:
                    pending_eval.native_histogram = True
            i += 1
            continue

        raise PromqltestParseError(f"{file_name}:{line_no}: unexpected line {raw!r}")

    finish_scenario()
    return scenarios


def _parse_expect_string(text: str) -> str:
    text = text.strip()
    if len(text) >= 2 and text[0] in "\"'`" and text[-1] == text[0]:
        return text[1:-1]
    return text


def parse_all_files(snapshot_dir: Path = SNAPSHOT_DIR) -> list[Scenario]:
    scenarios: list[Scenario] = []
    for path in snapshot_test_files(snapshot_dir):
        scenarios.extend(parse_test_file(path))
    return scenarios


def manifest_eval_ids(scenarios: list[Scenario]) -> list[str]:
    return [ev.eval_id for sc in scenarios for ev in sc.evals]


def assert_manifest_complete(scenarios: list[Scenario], snapshot_dir: Path = SNAPSHOT_DIR) -> None:
    ids = manifest_eval_ids(scenarios)
    if len(ids) != len(set(ids)):
        raise PromqltestParseError("duplicate eval ids in manifest")
    eval_header = re.compile(r"^\s*eval(?:_fail)?\b")
    expected = []
    for path in snapshot_test_files(snapshot_dir):
        for i, line in enumerate(path.read_text().splitlines(), 1):
            if eval_header.match(line) and not line.lstrip().startswith("#"):
                expected.append(f"{path.name}:{i}")
    missing = [eid for eid in expected if eid not in ids]
    extra = [eid for eid in ids if eid not in expected]
    if missing or extra:
        raise PromqltestParseError(
            f"manifest drift missing={missing[:10]} extra={extra[:10]} "
            f"counts expected={len(expected)} got={len(ids)}"
        )


def series_insert_values(interval_s: float, series: SeriesSpec) -> Optional[str]:
    if series.native_histogram or series.start_timestamp:
        return None
    points = []
    for sample in series.samples:
        if sample.missing or sample.native_histogram:
            continue
        ts = sample.offset_index * interval_s
        if sample.stale or (sample.value is not None and math.isnan(sample.value)):
            val = "nan"
        elif sample.value is None:
            continue
        elif math.isinf(sample.value):
            val = "inf" if sample.value > 0 else "-inf"
        else:
            val = repr(float(sample.value))
        points.append(f"(toDateTime64({ts}, 9), {val})")
    if not points:
        return None
    labels = dict(series.labels)
    labels.pop("__name__", None)
    metric = series.metric or labels.pop("__name__", "")
    tag_items = ", ".join(
        f"'{k}': '{v.replace(chr(39), chr(39)+chr(39))}'" for k, v in labels.items()
    )
    metric_sql = metric.replace("'", "''")
    return f"('{metric_sql}', {{{tag_items}}}, [{', '.join(points)}])"


def series_insert_sql(table: str, interval_s: float, series: SeriesSpec) -> Optional[str]:
    values = series_insert_values(interval_s, series)
    if values is None:
        return None
    return f"INSERT INTO {table} (metric_name, tags, samples) VALUES {values}"


def sql_literal(expr: str) -> str:
    return "'" + expr.replace("'", "''") + "'"


def eval_sql(table: str, case: EvalCase) -> str:
    expr = sql_literal(case.expr)
    if case.kind == "range":
        return (
            f"SELECT * FROM prometheusQueryRange({table}, {expr}, "
            f"{case.start_s}, {case.end_s}, {case.step_s})"
        )
    return f"SELECT * FROM prometheusQuery({table}, {expr}, {case.time_s})"


def values_approx_equal(a: float, b: float) -> bool:
    if math.isnan(a) and math.isnan(b):
        return True
    if math.isinf(a) and math.isinf(b):
        return (a > 0) == (b > 0)
    if math.isinf(a) or math.isinf(b) or math.isnan(a) or math.isnan(b):
        return False
    if a == b:
        return True
    return abs(a - b) <= FLOAT_FRACTION * max(abs(a), abs(b)) + FLOAT_MARGIN


def _label_key(labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((k, v) for k, v in labels.items() if k != "__name__" or True))


def normalize_labels(metric: str, labels: dict[str, str]) -> dict[str, str]:
    out = dict(labels)
    if metric and "__name__" not in out:
        out["__name__"] = metric
    return out


def parse_sql_labels(cell: str) -> dict[str, str]:
    pairs = re.findall(r"\('(?:\\'|[^'])*','(?:\\'|[^'])*'\)", cell.replace('"', "'"))
    if not pairs:
        pairs = re.findall(r"\('([^']*)','([^']*)'\)", cell)
        return {k: v for k, v in pairs}
    out = {}
    for item in re.findall(r"\('((?:\\'|[^'])*)','((?:\\'|[^'])*)'\)", cell):
        out[item[0]] = item[1]
    return out


def parse_sql_result(tsv: str) -> list[dict[str, Any]]:
    rows = []
    for line in tsv.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) == 2:
            if parts[0].lstrip().startswith("["):
                labels = parse_sql_labels(parts[0])
                for ts, val in re.findall(
                    rf"\('([^']+)',({_NUMBER_RE})\)", parts[1], re.IGNORECASE
                ):
                    rows.append(
                        {"metric": labels, "timestamp": ts, "value": _parse_number(val)}
                    )
            else:
                value = re.fullmatch(_NUMBER_RE, parts[1].strip(), re.IGNORECASE)
                if value:
                    rows.append(
                        {
                            "metric": {},
                            "timestamp": parts[0],
                            "value": _parse_number(value.group()),
                        }
                    )
            continue
        if len(parts) < 3:
            continue
        labels = parse_sql_labels(parts[0])
        ts = parts[1]
        val = _parse_number(parts[2])
        rows.append({"metric": labels, "timestamp": ts, "value": val})
    return rows


def _sql_ts_to_seconds(ts: str) -> Optional[float]:
    try:
        return float(ts)
    except ValueError:
        pass
    m = re.match(
        r"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?",
        ts,
    )
    if not m:
        return None
    from datetime import datetime, timezone

    micro = int((m.group(7) or "0").ljust(6, "0")[:6])
    dt = datetime(
        int(m.group(1)),
        int(m.group(2)),
        int(m.group(3)),
        int(m.group(4)),
        int(m.group(5)),
        int(m.group(6)),
        micro,
        tzinfo=timezone.utc,
    )
    return dt.timestamp()


def compare_eval(case: EvalCase, tsv: str, error: Optional[str]) -> tuple[str, str]:
    """Return (status, reason) status in passed/failed/unsupported."""
    if error:
        if case.expect_fail:
            return "passed", ""
        err_l = error.lower()
        if (
            "not implemented" in err_l
            or "not_implemented" in err_l
            or "not supported" in err_l
            or "code: 48" in err_l
            or "501" in err_l
        ):
            return "unsupported", error
        return "failed", f"ClickHouse error: {error}"
    if case.expect_fail:
        return "failed", "expected failure but ClickHouse succeeded"

    rows = parse_sql_result(tsv)
    if case.has_scalar:
        if len(rows) != 1:
            return "failed", f"scalar expected, got {len(rows)} rows"
        if rows[0]["metric"]:
            return "failed", f"scalar expected, got labels {rows[0]['metric']}"
        if not values_approx_equal(float(rows[0]["value"]), float(case.expected_scalar)):
            return "failed", f"scalar mismatch: {rows[0]['value']} vs {case.expected_scalar}"
        return "passed", ""

    actual_series: dict[tuple, list] = {}
    actual_order = []
    for row in rows:
        key = tuple(sorted(row["metric"].items()))
        if key not in actual_series:
            actual_series[key] = []
            actual_order.append(key)
        actual_series[key].append(row)

    expected_order = []
    expected_series: dict[tuple, SeriesSpec] = {}
    for spec in case.expected_series:
        labels = normalize_labels(spec.metric, spec.labels)
        key = tuple(sorted(labels.items()))
        expected_order.append(key)
        expected_series[key] = spec

    if case.kind == "instant":
        if case.expect_ordered:
            if actual_order != expected_order:
                return "failed", f"order mismatch: {actual_order} vs {expected_order}"
        else:
            if set(actual_order) != set(expected_order):
                return (
                    "failed",
                    f"series set mismatch: {len(actual_order)} vs {len(expected_order)}",
                )
        for key, spec in expected_series.items():
            got = actual_series.get(key, [])
            want_vals = [s for s in spec.samples if not s.missing]
            if len(got) != len(want_vals):
                return "failed", f"value count mismatch for {dict(key)}"
            if not want_vals:
                continue
            if not values_approx_equal(float(got[0]["value"]), float(want_vals[-1].value)):
                return "failed", f"value mismatch for {dict(key)}: {got[0]['value']} vs {want_vals[-1].value}"
        return "passed", ""

    if set(actual_order) != set(expected_order):
        return "failed", f"series set mismatch: {len(actual_order)} vs {len(expected_order)}"
    for key, spec in expected_series.items():
        actual_ts = []
        actual_values = []
        for row in actual_series.get(key, []):
            sec = _sql_ts_to_seconds(str(row["timestamp"]))
            if sec is None:
                return "failed", f"unparseable timestamp {row['timestamp']}"
            actual_ts.append(round(sec, 6))
            actual_values.append(row["value"])
        want_samples = [sample for sample in spec.samples if not sample.missing]
        want_ts = [
            round(case.start_s + sample.offset_index * case.step_s, 6)
            for sample in want_samples
        ]
        if actual_ts != want_ts:
            return (
                "failed",
                f"timestamp mismatch for {dict(key)}: {actual_ts} vs {want_ts}",
            )
        for ts, value, sample in zip(actual_ts, actual_values, want_samples):
            if not values_approx_equal(float(value), float(sample.value)):
                return "failed", f"value mismatch at {ts} for {dict(key)}"
    return "passed", ""


def empty_suite_record(upstream_sha: str, excluded_files: list[str]) -> dict[str, Any]:
    return {
        "passed": 0,
        "failed": 0,
        "unsupported": 0,
        "total": 0,
        "pct": 0.0,
        "breakdown": {},
        "upstream_sha": upstream_sha,
        "excluded_native_histogram": 0,
        "excluded_assertions": 0,
        "excluded_files": excluded_files,
    }


def update_suite_record(record: dict[str, Any], status: str, reason: str = "") -> None:
    if status == "passed":
        record["passed"] += 1
    elif status == "unsupported":
        record["unsupported"] += 1
        key = reason or "unsupported"
        record["breakdown"][key] = record["breakdown"].get(key, 0) + 1
    elif status == "excluded_native_histogram":
        record["excluded_native_histogram"] += 1
        return
    elif status == "excluded_assertion":
        record["excluded_assertions"] += 1
        return
    else:
        record["failed"] += 1
        key = reason or "failed"
        record["breakdown"][key] = record["breakdown"].get(key, 0) + 1
    total = record["passed"] + record["failed"] + record["unsupported"]
    record["total"] = total
    record["pct"] = round((record["passed"] / total * 100) if total else 0.0, 4)


def classify_eval(case: EvalCase) -> Optional[str]:
    reason = case.exclusion_reason()
    if reason in {
        "load_with_nhcb",
        "native_histogram_expected_or_loaded",
        "query_uses_native_histogram_metric",
    }:
        return "excluded_native_histogram"
    if reason in {
        "string_literal",
        "range_vector_instant",
        "expect_fail_diagnostic",
        "annotation_assertion",
        "start_timestamp",
        "stale_marker",
    }:
        return "excluded_assertion"
    return None
