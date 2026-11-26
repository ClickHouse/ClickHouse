#!/usr/bin/env python3
"""Refresh the vendored Prometheus promqltest snapshot from a pinned commit.

CI reads only the files under ``promqltest/``. This script is for maintainers.
It does not run during tests.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import urllib.request
from pathlib import Path

DEFAULT_COMMIT = "3da2514eff5ce503de2e8f355cae8ca58a2a35ed"
REPO = "prometheus/prometheus"
UPSTREAM_DIR = "promql/promqltest/testdata"
INCLUDED = [
    "aggregators.test",
    "at_modifier.test",
    "collision.test",
    "functions.test",
    "limit.test",
    "literals.test",
    "name_label_dropping.test",
    "operators.test",
    "range_queries.test",
    "selectors.test",
    "staleness.test",
    "start_timestamps.test",
    "subquery.test",
    "trig_functions.test",
]
EXCLUDED = [
    "histograms.test",
    "native_histograms.test",
    "info.test",
    "type_and_unit.test",
    "fill-modifier.test",
    "extended_vectors.test",
    "duration_expression.test",
]


def _raw_url(commit: str, name: str) -> str:
    return f"https://raw.githubusercontent.com/{REPO}/{commit}/{UPSTREAM_DIR}/{name}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", default=DEFAULT_COMMIT)
    parser.add_argument(
        "--dest",
        type=Path,
        default=Path(__file__).resolve().parent / "promqltest",
    )
    args = parser.parse_args()
    testdata = args.dest / "testdata"
    testdata.mkdir(parents=True, exist_ok=True)
    files = []
    for name in INCLUDED:
        url = _raw_url(args.commit, name)
        with urllib.request.urlopen(url, timeout=60) as resp:
            data = resp.read()
        (testdata / name).write_bytes(data)
        files.append(
            {
                "name": name,
                "sha256": hashlib.sha256(data).hexdigest(),
                "bytes": len(data),
            }
        )
        print(f"wrote {name} ({len(data)} bytes)")
    snapshot = {
        "repository": f"https://github.com/{REPO}",
        "path": UPSTREAM_DIR,
        "commit": args.commit,
        "license": "Apache-2.0",
        "included_files": INCLUDED,
        "excluded_files": EXCLUDED,
        "files": files,
    }
    (args.dest / "snapshot.json").write_text(json.dumps(snapshot, indent=2) + "\n")
    print(f"wrote snapshot.json commit={args.commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
