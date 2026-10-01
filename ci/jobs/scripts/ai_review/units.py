"""
Review units and the review state carried from one push to the next.

A single review run misses real issues mostly through skipped regions and
early stopping, and a re-review drips out issues the previous run simply did
not reach. Both are structural, so the job makes the structure explicit:

  * The PR diff is split into review units: the hunks of one file under one
    function (the section heading GitHub puts after each `@@`), so new code the
    index does not know yet still gets a unit. Each unit has a content
    fingerprint over its added and removed lines, which survives line moves,
    rebases and merges of the base branch.
  * Units are ordered by risk (core directories first, then size), the same
    order on every push. The agent must return a verdict for every unit in
    scope (`coverage.json`); the job lists the ones it did not.
  * The state of a review (unit fingerprints, the fingerprints of the findings
    it posted, the PR's contract) travels in a hidden marker of the summary
    comment. On the next push, units whose fingerprint did not change are out
    of scope: the agent looks at them only where the changed code affects
    them, and a new finding there is not posted as a new inline comment
    unless it is a Blocker. The agent is never told that unchanged code is
    "clean": telling a model code is correct measurably suppresses detection.
"""

import base64
import hashlib
import json
import re
import zlib

_HUNK_RE = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@ ?(.*)$")
STATE_MARKER = "<!-- ai-review-state: {data} -->"
_STATE_RE = re.compile(r"<!-- ai-review-state: ([A-Za-z0-9+/=]+) -->")
STATE_VERSION = 1

# Directories where a defect costs most, first; anything else under src/ next;
# tests, docs and tooling last.
_RISK_PREFIXES = (
    "src/Storages/MergeTree/", "src/Storages/", "src/Interpreters/", "src/Processors/", "src/Analyzer/",
    "src/Coordination/", "src/Access/", "src/Formats/", "src/IO/", "src/Common/", "src/Columns/",
    "src/DataTypes/", "src/Functions/", "src/AggregateFunctions/", "src/Core/", "src/Server/", "src/Client/",
)


def _risk_rank(path):
    for i, prefix in enumerate(_RISK_PREFIXES):
        if path.startswith(prefix):
            return i
    if path.startswith(("src/", "base/", "programs/")):
        return len(_RISK_PREFIXES)
    if path.startswith("tests/"):
        return len(_RISK_PREFIXES) + 1
    return len(_RISK_PREFIXES) + 2


def _fingerprint(lines):
    return hashlib.sha1("\n".join(lines).encode("utf-8", "replace")).hexdigest()[:16]


def build(files):
    """Split the PR diff (GitHub `pulls/<n>/files` rows) into review units,
    ordered by risk. Each unit: id (U1, U2, ... in that order), key (path and
    heading, stable across pushes), path, heading, new and old line ranges,
    changed line count, fingerprint."""
    units = {}
    for f in files:
        path = f["filename"]
        hunks = []
        current = None
        for raw in (f.get("patch") or "").split("\n"):
            m = _HUNK_RE.match(raw)
            if m:
                old, old_len, new, new_len = int(m.group(1)), int(m.group(2) or 1), int(m.group(3)), int(m.group(4) or 1)
                current = {"heading": m.group(5).strip(), "new": (new, new + max(new_len, 1) - 1),
                           "old": (old, old + max(old_len, 1) - 1), "changed": []}
                hunks.append(current)
            elif current is not None and raw[:1] in ("+", "-"):
                current["changed"].append(raw)
        if not hunks:
            # No patch (binary or too large): one unit for the whole file.
            hunks = [{"heading": "", "new": (0, 0), "old": (0, 0), "changed": [f"{f.get('status')}:{f.get('sha')}"]}]
        for h in hunks:
            key = f"{path}::{h['heading']}" if h["heading"] else f"{path}::"
            u = units.setdefault(key, {"key": key, "path": path, "heading": h["heading"], "new": [], "old": [], "changed": []})
            u["new"].append(h["new"])
            u["old"].append(h["old"])
            u["changed"] += h["changed"]
    ordered = sorted(units.values(), key=lambda u: (_risk_rank(u["path"]), u["path"], min(r[0] for r in u["new"])))
    for i, u in enumerate(ordered, 1):
        u["id"] = f"U{i}"
        u["size"] = len(u["changed"])
        u["fp"] = _fingerprint(u.pop("changed"))
    return ordered


def unit_for_line(units, path, line, side):
    """The unit a comment on `path:line` (RIGHT or LEFT) belongs to, or None."""
    for u in units:
        if u["path"] != path:
            continue
        for lo, hi in (u["new"] if side == "RIGHT" else u["old"]):
            if lo <= line <= hi:
                return u
    return None


def scope(units, previous_state):
    """Mark each unit `new`, `changed` or `unchanged` against the previous
    review's state. Without a usable previous state everything is in scope."""
    previous = (previous_state or {}).get("units") or {}
    for u in units:
        if not previous:
            u["status"] = "new"
        elif u["key"] not in previous:
            u["status"] = "new"
        elif previous[u["key"]] != u["fp"]:
            u["status"] = "changed"
        else:
            u["status"] = "unchanged"
    return units


def in_scope(unit):
    return unit.get("status") != "unchanged"


def render(units, incremental):
    """`units.md` for the review context."""
    def ranges(u):
        rs = [f"{lo}-{hi}" if hi > lo else f"{lo}" for lo, hi in u["new"] if lo]
        return ", ".join(rs) if rs else "whole file"

    def row(u):
        heading = f" `{u['heading']}`" if u["heading"] else ""
        return f"- **{u['id']}** `{u['path']}`{heading}: new lines {ranges(u)}, {u['size']} changed lines"

    out = ["# Review units", "",
           "The PR diff split by file and function, riskiest first. Give every unit in scope a verdict "
           "in `coverage.json`.", ""]
    active = [u for u in units if in_scope(u)]
    rest = [u for u in units if not in_scope(u)]
    if incremental:
        out += ["## In scope: new or changed since the previous review", ""]
    out += [row(u) + (f" ({u['status']})" if incremental else "") for u in active] or ["(none)"]
    if incremental and rest:
        out += ["", "## Unchanged since the previous review", "",
                "Out of scope for this push, except where an in-scope change affects them (a changed caller, "
                "callee, type or invariant they depend on).", ""]
        out += [row(u) for u in rest]
    return "\n".join(out) + "\n"


def finding_fingerprint(path, unit_key, body):
    """Line-independent identity of a finding: its file, its unit and the
    words of its first sentence."""
    first = re.split(r"(?<=[.!?])\s", (body or "").strip(), maxsplit=1)[0]
    words = " ".join(sorted({w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_:]{2,}", first)}))
    return hashlib.sha1(f"{path}|{unit_key}|{words}".encode()).hexdigest()[:16]


def encode_state(units, findings, contract, activity=""):
    """`activity` is the time of the latest comment by a person when the
    review started, so the next run can tell whether anyone wrote since."""
    state = {"v": STATE_VERSION, "units": {u["key"]: u["fp"] for u in units}, "findings": findings,
             "contract": (contract or "")[:6000], "activity": activity or ""}
    data = base64.b64encode(zlib.compress(json.dumps(state, separators=(",", ":")).encode(), 9)).decode()
    return STATE_MARKER.format(data=data)


def decode_state(text):
    m = _STATE_RE.search(text or "")
    if not m:
        return None
    try:
        state = json.loads(zlib.decompress(base64.b64decode(m.group(1))).decode())
    except (ValueError, zlib.error):
        return None
    return state if isinstance(state, dict) and state.get("v") == STATE_VERSION else None
