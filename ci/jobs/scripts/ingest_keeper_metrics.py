#!/usr/bin/env python3
"""Post-hook script: ingest keeper_metrics.jsonl into CIDB after a keeper stress job."""
import os
import sys
from pathlib import Path

_repo_dir = str(Path(__file__).resolve().parents[3])
_ci_dir = str(Path(__file__).resolve().parents[2])
for _p in (_repo_dir, _ci_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from praktika.settings import Settings
from praktika.utils import Utils
from ci.jobs.scripts.cidb_cluster import CIDBCluster


def main():
    metrics_file = os.environ.get(
        "KEEPER_METRICS_FILE",
        f"{Utils.absolute_path(Settings.TEMP_DIR)}/keeper_metrics.jsonl",
    )
    if not os.path.exists(metrics_file):
        print(f"NOTE: keeper metrics file not found at {metrics_file!r}, skipping ingestion")
        return

    ci_db = CIDBCluster()
    inserted, _ = ci_db.insert_keeper_metrics_from_file(
        file_path=metrics_file,
        chunk_size=1000,
    )
    print(f"NOTE: keeper metrics ingested: file={metrics_file} inserted={inserted}")


if __name__ == "__main__":
    main()
