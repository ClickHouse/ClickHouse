#!/usr/bin/env python3
"""Export the reference-documents input artifact from the CI-built binary.

This is intentionally independent from ``docs_autogen_nightly.py``. It does
not read or modify ``docs/`` and it does not publish anything to object
storage. Praktika uploads the resulting archive as the short-lived artifact of
this job, where a docs-rendering check can consume the exact same snapshot.
"""

import os
import stat
import subprocess
import sys
import tarfile
from pathlib import Path

from praktika.result import Result
from praktika.utils import Utils


REPO_ROOT = Path(Utils.cwd())
TEMP_DIR = REPO_ROOT / "ci" / "tmp"
BINARY = TEMP_DIR / "clickhouse"
CONFIG_FILE = REPO_ROOT / "utils" / "generate-system-tables-docs-config.xml"
EXPORTER = REPO_ROOT / "ci" / "jobs" / "scripts" / "docs" / "reference_snapshot" / "export_reference_snapshot.py"
SNAPSHOT_DIR = TEMP_DIR / "reference-snapshot"
SNAPSHOT_ARCHIVE = TEMP_DIR / "reference-snapshot.tar.zst"


def source_revision() -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        text=True,
    ).strip()


def _normalized_tarinfo(tarinfo: tarfile.TarInfo) -> tarfile.TarInfo:
    """Normalize archive metadata so equal snapshots have equal bytes."""
    if tarinfo.issym() or tarinfo.islnk():
        raise ValueError(f"Reference snapshot must not contain links: {tarinfo.name}")
    tarinfo.mtime = 0
    tarinfo.uid = 0
    tarinfo.gid = 0
    tarinfo.uname = ""
    tarinfo.gname = ""
    tarinfo.mode = 0o755 if tarinfo.isdir() else 0o644
    return tarinfo


def create_deterministic_archive(snapshot_dir: Path, archive_path: Path) -> None:
    """Write a reproducible ``.tar.zst`` archive for a snapshot directory."""
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with archive_path.open("wb") as output:
        compressor = subprocess.Popen(
            ["zstd", "-q", "-c"], stdin=subprocess.PIPE, stdout=output
        )
        assert compressor.stdin is not None
        try:
            with tarfile.open(
                mode="w|", fileobj=compressor.stdin, format=tarfile.USTAR_FORMAT
            ) as tar:
                tar.add(
                    snapshot_dir,
                    arcname=snapshot_dir.name,
                    recursive=False,
                    filter=_normalized_tarinfo,
                )
                for source in sorted(snapshot_dir.rglob("*")):
                    tar.add(
                        source,
                        arcname=str(
                            Path(snapshot_dir.name) / source.relative_to(snapshot_dir)
                        ),
                        recursive=False,
                        filter=_normalized_tarinfo,
                    )
        finally:
            compressor.stdin.close()
        if compressor.wait() != 0:
            raise subprocess.CalledProcessError(compressor.returncode, compressor.args)


def export_snapshot() -> bool:
    if not BINARY.is_file():
        raise FileNotFoundError(f"ClickHouse binary artifact is missing: {BINARY}")
    if not CONFIG_FILE.is_file():
        raise FileNotFoundError(f"Snapshot config is missing: {CONFIG_FILE}")
    if SNAPSHOT_DIR.exists() or SNAPSHOT_ARCHIVE.exists():
        raise FileExistsError(
            "Reference snapshot output already exists; CI jobs must start with a clean ci/tmp directory"
        )

    BINARY.chmod(BINARY.stat().st_mode | stat.S_IXUSR)
    # The binary artifact is self-extracting. Materialize it once before the
    # exporter starts querying it, so its manifest hashes the actual binary.
    subprocess.run([str(BINARY), "--version"], check=True)
    subprocess.run(
        [
            sys.executable,
            str(EXPORTER),
            "--binary",
            str(BINARY),
            "--config-file",
            str(CONFIG_FILE),
            "--output",
            str(SNAPSHOT_DIR),
            "--source-revision",
            source_revision(),
        ],
        check=True,
    )
    create_deterministic_archive(SNAPSHOT_DIR, SNAPSHOT_ARCHIVE)
    return SNAPSHOT_ARCHIVE.is_file()


if __name__ == "__main__":
    os.chdir(REPO_ROOT)
    Result.create_from(
        results=[
            Result.from_commands_run(
                name="Export reference snapshot",
                command=export_snapshot,
                with_info_on_failure=True,
            )
        ]
    ).complete_job()
