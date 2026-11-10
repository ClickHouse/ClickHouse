#!/usr/bin/env python3
"""
Run :func:`test_promql_compliance` locally and write a JSON snapshot of scores.

CI compares PRs to the latest matching object on S3 under
``REFs/master/<sha>/promql_compliance/promql_compliance_result.json``; this script
is for local inspection or attaching a file to a discussion — not for a committed
baseline file.

Requirements

- Docker (the integration test starts containers).
- A fresh ``clickhouse`` binary, e.g. ``ninja -C build clickhouse`` on an up-to-date tree.

Example

.. code-block:: bash

   export CLICKHOUSE_TESTS_SERVER_BIN_PATH="$PWD/build/programs/clickhouse"
   ./tests/integration/test_prometheus_protocols/update_compliance_baseline.py \\
       --capture-log /tmp/promql_compliance_refresh.log

Or pass the binary explicitly::

   ./tests/integration/test_prometheus_protocols/update_compliance_baseline.py \\
       --binary "$PWD/build/programs/clickhouse" --dry-run

By default writes JSON to ``ci/tmp/promql_compliance_baseline_export.json`` (override
with ``--output``). ``--dry-run`` prints JSON to stdout only.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Repo root: file is under tests/integration/test_prometheus_protocols/; parents[3] is repo root.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_TESTS_INTEGRATION = _REPO_ROOT / "tests" / "integration"
_CI_TMP = _REPO_ROOT / "ci" / "tmp"
_DEFAULT_OUT = _CI_TMP / "promql_compliance_baseline_export.json"
_DEFAULT_NOTE = (
    "Exported by tests/integration/test_prometheus_protocols/update_compliance_baseline.py "
    "from a local test_promql_compliance run."
)


def _git_short_sha() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=str(_REPO_ROOT),
            text=True,
        ).strip()
        return out[:12]
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "local"


def _resolve_binary(path: str | None) -> Path:
    if path:
        p = Path(path).resolve()
        if not p.is_file():
            sys.exit(f"ERROR: binary not found: {p}")
        return p
    env = os.environ.get("CLICKHOUSE_TESTS_SERVER_BIN_PATH", "").strip()
    if env:
        p = Path(env).resolve()
        if p.is_file():
            return p
    cand = _REPO_ROOT / "build" / "programs" / "clickhouse"
    if cand.is_file():
        return cand.resolve()
    sys.exit(
        "ERROR: set --binary or CLICKHOUSE_TESTS_SERVER_BIN_PATH to a clickhouse executable, "
        f"or build {cand}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--binary",
        help="Path to clickhouse server binary (default: env or build/programs/clickhouse)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=_DEFAULT_OUT,
        metavar="PATH",
        help=f"Write JSON here (default: {_DEFAULT_OUT})",
    )
    parser.add_argument(
        "--capture-log",
        type=Path,
        metavar="PATH",
        help="Append pytest stdout/stderr to this file (tee)",
    )
    parser.add_argument(
        "--note",
        default=_DEFAULT_NOTE,
        help="Value for the note field in the JSON",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print JSON to stdout only; do not write --output",
    )
    args = parser.parse_args()

    binary = _resolve_binary(args.binary)
    _CI_TMP.mkdir(parents=True, exist_ok=True)
    result_path = _CI_TMP / "compliance_baseline_refresh_result.json"
    if result_path.exists():
        result_path.unlink()

    env = os.environ.copy()
    env["COMPLIANCE_RESULT_FILE"] = str(result_path)
    env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = str(binary)
    env["CLICKHOUSE_TESTS_BASE_CONFIG_DIR"] = str(_REPO_ROOT / "programs" / "server")
    env.setdefault("PYTEST_TIMEOUT", os.environ.get("PYTEST_TIMEOUT", "3600"))

    cmd = [
        sys.executable,
        "-m",
        "pytest",
        "test_prometheus_protocols/test_compliance.py::test_promql_compliance",
        "-vv",
        "--tb=short",
    ]
    print(f"Running (cwd={_TESTS_INTEGRATION}): {' '.join(cmd)}", flush=True)
    print(f"COMPLIANCE_RESULT_FILE={result_path}", flush=True)
    print(f"CLICKHOUSE_TESTS_SERVER_BIN_PATH={binary}", flush=True)

    log_fp = None
    try:
        if args.capture_log:
            args.capture_log.parent.mkdir(parents=True, exist_ok=True)
            append = (
                args.capture_log.exists() and args.capture_log.stat().st_size > 0
            )
            log_fp = open(args.capture_log, "a", encoding="utf-8")
            if append:
                log_fp.write("\n" + "=" * 72 + "\n")

        proc = subprocess.run(
            cmd,
            cwd=str(_TESTS_INTEGRATION),
            env=env,
            capture_output=True,
            text=True,
        )
        combined = proc.stdout + ("\n" + proc.stderr if proc.stderr else "")
        print(combined, end="" if combined.endswith("\n") else "\n", flush=True)
        if log_fp:
            log_fp.write(combined)
            log_fp.flush()
    finally:
        if log_fp:
            log_fp.close()

    if proc.returncode != 0:
        sys.exit(f"ERROR: pytest exited with code {proc.returncode}")

    if not result_path.is_file():
        sys.exit(f"ERROR: expected result file at {result_path}")

    record = json.loads(result_path.read_text())
    for key in ("passed", "failed", "unsupported", "total", "pct"):
        if key not in record:
            sys.exit(f"ERROR: result JSON missing key {key!r}")

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    baseline = {
        "passed": record["passed"],
        "failed": record["failed"],
        "unsupported": record["unsupported"],
        "total": record["total"],
        "pct": record["pct"],
        "commit": _git_short_sha(),
        "updated_utc": now,
        "note": args.note,
    }
    if "breakdown" in record:
        baseline["breakdown"] = record["breakdown"]

    text = json.dumps(baseline, indent=2) + "\n"
    if args.dry_run:
        print(text, end="")
        print("\n(dry-run: no file written)", flush=True)
        return

    out = args.output.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)
    print(f"Wrote {out}", flush=True)


if __name__ == "__main__":
    main()
