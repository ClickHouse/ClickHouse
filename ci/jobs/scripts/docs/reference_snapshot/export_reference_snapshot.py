#!/usr/bin/env python3
"""Export immutable reference-documentation input from a ClickHouse binary.

The exporter intentionally talks only to the binary passed on the command line.
It does not read ClickHouse source files, so the resulting snapshot describes the
exact server that CI built.  The JSON files are canonicalized to make them
stable, content-addressable build inputs for the documentation renderer.
"""

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


SNAPSHOT_SCHEMA_VERSION = 1

# These are the source tables for the reference-docs snapshot. Add a catalog
# deliberately: it is an externally consumed snapshot contract. The exporter
# keeps each source table independent; renderers can join catalogs in memory
# without making the snapshot depend on a particular query plan.
CATALOG_QUERIES = {
    # The complete Markdown documentation for system tables and SQL statements,
    # together with the source metadata that identifies each documented entity.
    "documentation": "SELECT * FROM system.documentation FORMAT JSONEachRow",
    "statements": "SELECT * FROM system.statements FORMAT JSONEachRow",
    "functions": "SELECT * FROM system.functions FORMAT JSONEachRow",
    "table_functions": "SELECT * FROM system.table_functions FORMAT JSONEachRow",
    "table_engines": "SELECT * FROM system.table_engines FORMAT JSONEachRow",
    "database_engines": "SELECT * FROM system.database_engines FORMAT JSONEachRow",
    "dictionary_layouts": "SELECT * FROM system.dictionary_layouts FORMAT JSONEachRow",
    "dictionary_sources": "SELECT * FROM system.dictionary_sources FORMAT JSONEachRow",
    "data_type_families": "SELECT * FROM system.data_type_families FORMAT JSONEachRow",
    "formats": "SELECT * FROM system.formats FORMAT JSONEachRow",
    "settings": "SELECT * FROM system.settings FORMAT JSONEachRow",
    "server_settings": "SELECT * FROM system.server_settings FORMAT JSONEachRow",
    "merge_tree_settings": "SELECT * FROM system.merge_tree_settings FORMAT JSONEachRow",
    # Settings explorers use this release history to show when a setting's
    # default changed. It is captured separately because it relates one
    # setting to many versioned changes rather than describing its current
    # state.
    "settings_changes": "SELECT * FROM system.settings_changes FORMAT JSONEachRow",
}


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def run_query(binary: Path, config_file: Path, query: str) -> str:
    result = subprocess.run(
        [str(binary), "local", "--config-file", str(config_file), "--query", query],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or f"ClickHouse exited with {result.returncode}")
    return result.stdout


def read_json_each_row(binary: Path, config_file: Path, query: str) -> list[dict[str, Any]]:
    rows = [json.loads(line) for line in run_query(binary, config_file, query).splitlines() if line.strip()]
    # System-table scan order is not an API.  Sorting by canonical representation
    # makes byte-identical snapshots for identical binaries and catalog contents.
    return sorted(rows, key=lambda row: canonical_json(row))


def clickhouse_version(binary: Path, config_file: Path) -> str:
    return run_query(binary, config_file, "SELECT version() FORMAT TabSeparatedRaw").strip()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path, help="Built clickhouse binary")
    parser.add_argument(
        "--config-file",
        required=True,
        type=Path,
        help="ClickHouse config which materializes runtime system tables, including system logs",
    )
    parser.add_argument("--output", required=True, type=Path, help="Empty output directory")
    parser.add_argument("--source-revision", default=None, help="Optional immutable ClickHouse commit SHA")
    parser.add_argument(
        "--catalog",
        action="append",
        choices=sorted(CATALOG_QUERIES),
        help="Export only this catalog; repeat to select several",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    binary = args.binary.resolve()
    if not binary.is_file():
        raise SystemExit(f"binary does not exist: {binary}")
    config_file = args.config_file.resolve()
    if not config_file.is_file():
        raise SystemExit(f"config file does not exist: {config_file}")
    if args.output.exists():
        raise SystemExit(f"output already exists: {args.output}")

    selected = args.catalog or list(CATALOG_QUERIES)
    staging = args.output.with_name(f".{args.output.name}.tmp")
    if staging.exists():
        raise SystemExit(f"temporary output already exists: {staging}")
    staging.mkdir(parents=True)

    try:
        catalogs: list[dict[str, Any]] = []
        catalog_dir = staging / "catalogs"
        catalog_dir.mkdir()
        for name in selected:
            rows = read_json_each_row(binary, config_file, CATALOG_QUERIES[name])
            relative_path = f"catalogs/{name}.json"
            payload = canonical_json(rows)
            (staging / relative_path).write_bytes(payload)
            catalogs.append({
                "name": name,
                "path": relative_path,
                "rows": len(rows),
                "sha256": hashlib.sha256(payload).hexdigest(),
            })
            print(f"exported {name}: {len(rows)} rows", file=sys.stderr)

        manifest = {
            "schemaVersion": SNAPSHOT_SCHEMA_VERSION,
            "clickhouseVersion": clickhouse_version(binary, config_file),
            "sourceRevision": args.source_revision,
            "binarySha256": sha256_file(binary),
            "configSha256": hashlib.sha256(config_file.read_bytes()).hexdigest(),
            "catalogs": catalogs,
        }
        (staging / "manifest.json").write_bytes(canonical_json(manifest))
        staging.rename(args.output)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    print(f"reference snapshot: {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
