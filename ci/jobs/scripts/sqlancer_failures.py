#!/usr/bin/env python3
"""Collect and deduplicate SQLancer findings at the end of a fuzzing run.

When an oracle throws, SQLancer's `StateLogger.logException` writes the stack
trace plus the whole CREATE/INSERT/.../SELECT sequence of that database to
`logs/<dbms>/databaseN.log` and the worker then continues with a fresh database
name, so each of those files is exactly one finding (`databaseN-cur.log` is the
live per-select transcript of an in-progress database, not a finding).

A 5h run repeats the same bug many times, so the raw list is unreadable. This
script copies every reproducer log into `<out-dir>` (one file per finding, ready
to be attached as an artifact), fingerprints each one and groups equal
fingerprints into distinct failures:

    fingerprint = (exception class + ClickHouse error code/name from the
                   `Caused by:` chain, reporting oracle, first sqlancer frame)

The failure *message* is deliberately NOT part of the fingerprint: for most
oracles it is the generated SQL, which is unique to every finding and would
defeat the whole point. What identifies a bug is where it was detected (oracle +
assertion site) and, if the server rejected a query, which error it raised. Each
family therefore lists every occurrence by database name and up to three distinct
message shapes (literals to `'S'`, numbers to `N`, generated identifiers such as
`c0`/`t3`/`database7` to `id`) so an over-merged family is still visible as such.
Stack frame line numbers are kept: two assertions inside the same oracle are two
different bugs.

Outputs:
  <out-dir>/<databaseN>.log     one reproducer per finding (gzipped above 10 MB)
  <out-dir>/analysis.txt        human-readable analysis, attached to the report
  <out-dir>/findings.json       the same data as JSON: one entry per distinct
                                failure with its occurrences, for tooling (the
                                Slack notifier diffs it against CIDB history)
  <out-dir>/subresults.json     report rows: one per distinct failure, with that
                                failure's reproducer logs attached, as a JSON
                                fragment (comma-separated objects, no brackets)
  stdout                        "<findings>\t<distinct failures>\t<summary>"

Report row names are the fingerprint WITHOUT the occurrence count, because
praktika turns every row into a CIDB row keyed by that name (`CIDB.json_data_generator`):
a stable name is what makes "when did this failure first appear" answerable
across nightlies. The count lives in the row info.

`--dry-run` only counts (no files written, no copies) - used by the job script to
poll for a finding flood while the fuzzer is still running.
"""

import argparse
import gzip
import re
import shutil
import json
from pathlib import Path

COMPRESS_ABOVE_BYTES = 10 * 1024 * 1024

EXCEPTION_RE = re.compile(r"^\s*([\w.$]*(?:Error|Exception))\s*:?\s*(.*)$")
CODE_RE = re.compile(r"\bCode:\s*(\d+)")
CH_ERROR_NAME_RE = re.compile(r"\(([A-Z][A-Z0-9_]{3,})\)")
CAUSED_BY_RE = re.compile(r"^--\s*Caused by:\s*(.*)$")
FRAME_RE = re.compile(r"^--\s*at\s+([\w.$]+)\.([\w$]+)\((?:\w+\.java:(\d+)|[^)]*)\)")
# Oracles live in a `.oracle.` package; the ClickHouse ones are all named
# `ClickHouse<Name>Oracle`, the shared ones keep their upstream names
# (`PivotedQuerySynthesisBase` = PQS, `CERTOracle`, `NoRECOracle`, ...).
# `CompositeTestOracle` is only the dispatcher that runs the others.
CLICKHOUSE_ORACLE_RE = re.compile(r"^ClickHouse([A-Za-z0-9_]+)Oracle$")
ORACLE_DISPATCHERS = {"CompositeTestOracle", "TestOracle"}


def normalize_message(message):
    """Collapse a failure message to its shape so equal bugs compare equal."""
    text = re.sub(r"'[^']*'", "'S'", message)
    text = re.sub(r'"[^"]*"', "'S'", text)
    # Generated identifiers: c0, t12, database7, v_zero1, ...
    text = re.sub(r"\b[A-Za-z_][A-Za-z_]*\d+\b", "id", text)
    text = re.sub(r"\b\d[\d.,e+-]*\b", "N", text)
    return re.sub(r"\s+", " ", text).strip()


def oracle_from_frame(qualified_class):
    """The oracle name if this stack frame belongs to one, else ''."""
    class_name = qualified_class.rsplit(".", 1)[-1].split("$", 1)[0]
    if class_name in ORACLE_DISPATCHERS:
        return ""
    match = CLICKHOUSE_ORACLE_RE.match(class_name)
    if match:
        return match.group(1)
    if ".oracle." in qualified_class:
        # Shared oracles keep their upstream class names; trim the boilerplate so
        # `NoRECOracle` reads as `NoREC`, like the `--oracle` option values.
        return class_name.removeprefix("ClickHouse").removesuffix("Oracle")
    return ""


def parse_reproducer(path):
    """Fingerprint one reproducer log."""
    exception = ""
    message = ""
    frame = ""
    oracle = ""
    code = ""
    error_name = ""
    head = ""

    with path.open(encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f):
            line = line.rstrip("\n").rstrip("\r")
            if lineno == 0:
                head = line.removeprefix("--")
                match = EXCEPTION_RE.match(head)
                if match:
                    exception, message = match.group(1), match.group(2)
                else:
                    message = head
            # The server's error code usually only appears down the `Caused by:`
            # chain (the top-level AssertionError message is just the query), and
            # it is the strongest signal for telling two failures apart.
            if not code:
                caused_by = CAUSED_BY_RE.match(line)
                if caused_by or lineno == 0:
                    text = caused_by.group(1) if caused_by else head
                    code_match = CODE_RE.search(text)
                    if code_match:
                        code = code_match.group(1)
                        name_match = CH_ERROR_NAME_RE.search(text)
                        if name_match:
                            error_name = name_match.group(1)
            frame_match = FRAME_RE.match(line)
            if frame_match:
                if not frame and frame_match.group(1).startswith("sqlancer."):
                    frame = f"{frame_match.group(1).rsplit('.', 1)[-1]}.{frame_match.group(2)}"
                    if frame_match.group(3):
                        frame += f":{frame_match.group(3)}"
                if not oracle:
                    oracle = oracle_from_frame(frame_match.group(1))
            # The statement dump follows the stack trace; everything needed for
            # the fingerprint is in the first lines.
            if lineno > 200:
                break

    normalized = normalize_message(message)
    kind = exception or "unknown failure"
    if code:
        kind += f" / Code {code}" + (f" ({error_name})" if error_name else "")
    return {
        "database": path.stem,
        "head": head[:400],
        "exception": exception,
        "code": code,
        "error_name": error_name,
        "oracle": oracle,
        "frame": frame,
        "message": normalized[:200],
        "kind": kind,
        "fingerprint": (kind, oracle, frame),
    }


def family_title(members):
    example = members[0]
    parts = [example["oracle"] or "unknown oracle", example["kind"]]
    if example["frame"]:
        parts.append(f"at {example['frame']}")
    return " / ".join(parts)


def fingerprint_key(members):
    """Stable identity of a distinct failure - the CIDB test name."""
    return family_title(members)


def family_label(members):
    """Short `oracle/error` label for the one-line summary."""
    example = members[0]
    oracle = example["oracle"] or "unknown"
    if example["code"]:
        return f"{oracle}/Code {example['code']} x{len(members)}"
    return f"{oracle}/{example['exception'].rsplit('.', 1)[-1] or 'failure'} x{len(members)}"


def message_shapes(members, limit=3):
    shapes = []
    for member in members:
        if member["message"] not in shapes:
            shapes.append(member["message"])
        if len(shapes) >= limit:
            break
    return shapes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs-dir", required=True, help="sqlancer logs/<dbms> directory")
    parser.add_argument("--out-dir", required=True, help="where to put per-finding logs and the analysis")
    parser.add_argument("--max-files", type=int, default=50, help="cap on attached reproducer logs")
    parser.add_argument("--max-per-family", type=int, default=10, help="cap on attached logs per distinct failure")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="only count findings and distinct failures; write nothing (used while the fuzzer runs)",
    )
    args = parser.parse_args()

    logs_dir = Path(args.logs_dir)
    out_dir = Path(args.out_dir)

    reproducers = []
    if logs_dir.is_dir():
        for path in sorted(logs_dir.glob("database*.log")):
            if path.name.endswith("-cur.log") or path.stat().st_size == 0:
                continue
            reproducers.append(path)

    findings = [parse_reproducer(path) for path in reproducers]

    families = {}
    for finding in findings:
        families.setdefault(finding["fingerprint"], []).append(finding)
    # Loudest first - that is the one worth triaging.
    ordered = sorted(families.values(), key=lambda members: (-len(members), family_title(members)))

    if findings:
        top = ", ".join(family_label(members) for members in ordered[:5])
        if len(ordered) > 5:
            top += ", ..."
        summary = f"{len(findings)} finding(s) in {len(ordered)} distinct failure(s): {top}"
    else:
        summary = "no findings"

    if args.dry_run:
        print(f"{len(findings)}\t{len(ordered)}\t{summary}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    for finding in findings:
        finding["file"] = ""

    def attach(member):
        source = logs_dir / f"{member['database']}.log"
        target = out_dir / f"{member['database']}.log"
        shutil.copyfile(source, target)
        if target.stat().st_size > COMPRESS_ABOVE_BYTES:
            with target.open("rb") as raw, gzip.open(f"{target}.gz", "wb") as packed:
                shutil.copyfileobj(raw, packed)
            target.unlink()
            target = Path(f"{target}.gz")
        member["file"] = str(target)

    # Give every distinct failure one reproducer before spending the rest of the
    # budget on the loud ones. Straight loudest-first spending would let a single
    # 200-occurrence family eat the whole cap and leave the rare failures - the
    # interesting ones - as rows with no log to open.
    attached = 0
    for members in ordered:
        if attached >= args.max_files:
            break
        attach(members[0])
        attached += 1
    for members in ordered:
        for member in members[1 : args.max_per_family]:
            if attached >= args.max_files:
                break
            attach(member)
            attached += 1

    analysis = out_dir / "analysis.txt"
    with analysis.open("w", encoding="utf-8") as f:
        print("SQLancer failure analysis", file=f)
        print("=========================", file=f)
        print(f"findings: {len(findings)}    distinct failures: {len(ordered)}", file=f)
        print("", file=f)
        for members in ordered:
            example = members[0]
            shapes = message_shapes(members)
            print(f"x{len(members)}  {family_title(members)}", file=f)
            for i, shape in enumerate(shapes):
                print(f"    {'message:  ' if i == 0 else 'also:     '} {shape}", file=f)
            distinct = len({m["message"] for m in members})
            if distinct > len(shapes):
                print(f"    (+{distinct - len(shapes)} more message shape(s) - this family may cover more than one bug)", file=f)
            print(f"    example:   {example['database']}.log", file=f)
            print(f"    databases: {' '.join(m['database'] for m in members)}", file=f)
            print("", file=f)
        print("Per-finding index", file=f)
        print("-----------------", file=f)
        for finding in findings:
            print(f"{finding['database']}\t{finding['kind']}\t{finding['oracle']}\t{finding['head']}", file=f)

    # Machine-readable twin of analysis.txt: consumed by the Slack notifier and
    # available as an artifact for any cross-run tooling.
    findings_json = {
        "findings": len(findings),
        "distinct_failures": len(ordered),
        "summary": summary,
        "failures": [
            {
                "fingerprint": fingerprint_key(members),
                "count": len(members),
                "oracle": members[0]["oracle"],
                "kind": members[0]["kind"],
                "code": members[0]["code"],
                "error_name": members[0]["error_name"],
                "frame": members[0]["frame"],
                "message_shapes": message_shapes(members),
                "example_head": members[0]["head"],
                "databases": [m["database"] for m in members],
                "logs": [m["file"] for m in members if m["file"]],
            }
            for members in ordered
        ],
    }
    (out_dir / "findings.json").write_text(json.dumps(findings_json, indent=1), encoding="utf-8")

    # One flat row per distinct failure, with its reproducer logs attached.
    # Deliberately NOT nested per finding: the workflow report keeps only failed
    # *leaves* of a job result (`Result._flat_failed_leaves`), so a parent row
    # would be replaced by its children and the deduplication - the whole point
    # here - would disappear from the report.
    rows = []
    for members in ordered:
        files = [m["file"] for m in members if m["file"]]
        info = f"x{len(members)} | " + "; ".join(message_shapes(members, limit=2))
        info += f" | occurrences: {' '.join(m['database'] for m in members)}"
        if len(members) > len(files):
            info += f" ({len(files)} log(s) attached)"
        rows.append(
            {
                "name": fingerprint_key(members),
                "status": "FAIL",
                "files": files,
                "info": info,
                "results": [],
            }
        )

    if findings:
        rows.insert(
            0,
            {
                "name": "Failure analysis",
                "status": "FAIL",
                "files": [str(analysis), str(out_dir / "findings.json")],
                "info": summary,
                "results": [],
            },
        )

    fragment = out_dir / "subresults.json"
    fragment.write_text(",\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    print(f"{len(findings)}\t{len(ordered)}\t{summary}")


if __name__ == "__main__":
    main()
