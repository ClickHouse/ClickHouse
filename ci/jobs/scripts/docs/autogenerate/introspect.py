#!/usr/bin/env python3
"""
Structured introspection queries against a ClickHouse binary.

The catalog generators (table engines, formats, data types, ...) need typed
rows (capability flags, arrays) rather than pre-rendered markdown, so they
query via `clickhouse local` with JSONEachRow output.
"""

import json
import subprocess
import sys


def fetch_documentation(binary):
    """Fetch `system.documentation` -- the unified source of all embedded
    documentation -- once, keyed by entity type then name. `description` is
    the assembled Markdown (`catalog.split_assembled` peels the structured
    tail back off); `source` is the defining C++ file when known."""
    rows = fetch_rows(
        binary,
        "SELECT name, type, description, source"
        " FROM system.documentation ORDER BY type, name",
    )
    docs = {}
    for row in rows:
        docs.setdefault(row["type"], {})[row["name"]] = row
    return docs


def fetch_rows(binary, query):
    """Run `query` via `clickhouse local` and return a list of row dicts."""
    cmd = [binary, "local", "--output-format", "JSONEachRow", "--query", query]
    # `clickhouse local` treats inherited stdin as input data; detach it.
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=300, stdin=subprocess.DEVNULL
    )
    rows = []
    try:
        rows = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    except json.JSONDecodeError:
        rows = []
    # `clickhouse local` can exit non-zero because of races between system log
    # teardown and `Poco::Application` destruction, even after the query has
    # produced complete output. Tolerate that specific case only.
    if result.returncode != 0:
        if rows:
            print(
                f"Warning: query exited with code {result.returncode} but"
                f" produced {len(rows)} rows of output, treating as success.",
                file=sys.stderr,
            )
        else:
            print(f"Error running query: {query}", file=sys.stderr)
            print(f"stderr: {result.stderr}", file=sys.stderr)
            sys.exit(1)
    return rows
