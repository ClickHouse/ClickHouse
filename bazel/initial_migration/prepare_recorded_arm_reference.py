#!/usr/bin/env python3
"""Stage the recorded native ARM64 CMake original for ClickHouseStrippedParity."""

import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sys
import zlib

from verify_stripped_hash import elf_machine

RECORD = Path(__file__).with_name("recorded_aarch64")
# Artifacts of the native CMake invocation 12df76fc-3a68-46da-8af4-1e01c1dd4b4b.
SUMMARY = "cmake-reference-summary.json"
ARTIFACT_SHA256 = {
    SUMMARY: "ede99d57e04ee4aa39b5b5033f3a76e980d7e4b6d169a1b25279c5c5b59e7133",
    "status.json": "9a372a90854f39f8deba1ae45751e5a7fd73bd044b23e2856029f551b3c73d58",
}


def prepare(source, repository):
    if not source.is_file():
        raise ValueError(f"CMAKE_REFERENCE_ELF is not a regular file: {source}")
    if any(repository.iterdir()):
        raise ValueError("Use an empty directory for the recorded CMake repository")
    for name, digest in ARTIFACT_SHA256.items():
        content = (RECORD / name).read_bytes()
        if hashlib.sha256(content).hexdigest() != digest:
            raise ValueError(f"{name} differs from the recorded ARM64 CMake artifact")
        (repository / name).write_bytes(content)
    expected = json.loads((repository / SUMMARY).read_text())
    original = repository / "clickhouse.cmake"
    digest = hashlib.sha256()
    with source.open("rb") as archive:
        compressed = archive.peek(2)[:2] == b"\x1f\x8b"
        if not compressed and source.stat().st_size != expected["bytes"]:
            raise ValueError("CMAKE_REFERENCE_ELF has the wrong size")
        if shutil.disk_usage(repository).free < expected["bytes"]:
            raise OSError(f"Staging the CMake reference needs {expected['bytes']} bytes in {repository}")
        with (gzip.GzipFile(fileobj=archive) if compressed else archive) as stream, original.open("xb") as out:
            size = 0
            while block := stream.read(1024 * 1024):
                size += len(block)
                if size > expected["bytes"]:
                    raise ValueError("CMAKE_REFERENCE_ELF exceeds the recorded size")
                digest.update(block)
                out.write(block)
    if size != expected["bytes"] or digest.hexdigest() != expected["cmake_sha256"]:
        raise ValueError("CMAKE_REFERENCE_ELF size or SHA-256 differs from the recorded ARM64 CMake ELF")
    if elf_machine(original) != expected["elf_machine"]:
        raise ValueError("CMAKE_REFERENCE_ELF is not the recorded AArch64 ELF")
    shutil.copyfile(RECORD / "compare.BUILD.bazel.tmpl", repository / "BUILD.bazel")
    (repository / "REPO.bazel").touch()


def show_result(path):
    result = json.loads(path.read_text())
    summary = result["summary"]
    expected = json.loads((RECORD / SUMMARY).read_text())
    if result["phase"] != "compared" or type(result["status"]) is not int or result["status"] != 0:
        raise ValueError("ClickHouseStrippedParity did not complete successfully")
    fields = {
        "PARITY_ARCH": expected["arch"],
        "PARITY_STATUS": 0,
        "PARITY_STRIPPED_CMP_STATUS": 0,
        "CMAKE_REFERENCE_BYTES": expected["bytes"],
        "CMAKE_REFERENCE_SHA256": expected["cmake_sha256"],
    }
    for producer in ("CMAKE", "BAZEL"):
        fields[producer + "_STRIPPED_SHA256"] = expected["stripped_sha256"]
        fields[producer + "_STRIPPED_BYTES"] = expected["stripped_bytes"]
    for field, value in fields.items():
        if type(summary[field]) is not type(value) or summary[field] != value:
            raise ValueError(f"ClickHouseStrippedParity {field}: expected {value}, got {summary[field]}")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        if len(sys.argv) != 3:
            raise ValueError("Expected ELF REPOSITORY or --show-result STATUS")
        if sys.argv[1] == "--show-result":
            show_result(Path(sys.argv[2]))
        else:
            prepare(Path(sys.argv[1]), Path(sys.argv[2]))
    except (OSError, ValueError, KeyError, TypeError, EOFError, zlib.error) as error:
        print(f"Recorded ARM64 CMake comparison: {error}", file=sys.stderr)
        sys.exit(2)
