#!/usr/bin/env python3
"""Verify a native ARM CMake reference and compare its stripped Bazel binary."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from types import SimpleNamespace

from verify_stripped_hash import compare, elf_machine, recorded_reference, sha256, write_summary


def check(args, record):
    reference_status = json.loads((args.reference / "status.json").read_text())
    if not isinstance(reference_status, dict):
        raise ValueError("CMake status.json must contain an object")
    record["CMAKE_REFERENCE_STATUS"] = reference_status.get("status")
    record["CMAKE_REFERENCE_PHASE"] = reference_status.get("phase")
    if (type(reference_status.get("status")) is not int or reference_status["status"] != 0
            or reference_status.get("phase") != "built"):
        raise ValueError("Expected CMake status 0 and phase built for a complete reference")

    expected, _ = recorded_reference(SimpleNamespace(
        arch="aarch64",
        reference_summary=args.reference / "cmake-reference-summary.json",
        reference_invocation=None,
    ))
    record["CMAKE_COMPILER_SHA256"] = expected["compiler_sha256"]
    record["CMAKE_REFERENCE_EXPECTED_SHA256"] = expected["cmake_sha256"]
    record["CMAKE_REFERENCE_EXPECTED_BYTES"] = expected["bytes"]
    record["CMAKE_EXPECTED_PLAIN_STRIPPED_SHA256"] = expected["plain_stripped_sha256"]
    record["EXPECTED_STRIPPED_SHA256"] = expected["stripped_sha256"]
    record["EXPECTED_STRIPPED_BYTES"] = expected["stripped_bytes"]
    if "rustc_sha256" in expected:
        record["CMAKE_RUSTC_SHA256"] = expected["rustc_sha256"]
    if "cargo_sha256" in expected:
        record["CMAKE_CARGO_SHA256"] = expected["cargo_sha256"]

    record["BAZEL_C_COMPILER_SHA256"] = sha256(args.cc)
    record["BAZEL_COMPILER_SHA256"] = sha256(args.cxx)
    for name, compiler, digest in (
        ("C", args.cc, record["BAZEL_C_COMPILER_SHA256"]),
        ("C++", args.cxx, record["BAZEL_COMPILER_SHA256"]),
    ):
        if digest != expected["compiler_sha256"]:
            raise ValueError(f"Bazel {name} compiler differs from the recorded CMake compiler")
        if elf_machine(compiler) != 183:
            raise ValueError(f"Bazel {name} compiler is not a native AArch64 ELF")

    reference = args.reference / "clickhouse.cmake"
    record["CMAKE_REFERENCE_BYTES"] = reference.stat().st_size
    record["CMAKE_REFERENCE_SHA256"] = sha256(reference)
    if record["CMAKE_REFERENCE_BYTES"] != expected["bytes"]:
        raise ValueError("CMake original does not match the recorded byte count")
    if record["CMAKE_REFERENCE_SHA256"] != expected["cmake_sha256"]:
        raise ValueError("CMake original does not match the recorded SHA256")
    for name, executable in (("CMake", reference), ("Bazel", args.clickhouse)):
        if elf_machine(executable) != 183:
            raise ValueError(f"{name} ClickHouse is not an AArch64 ELF")

    # llvm-strip is an entry point of the verified LLVM multicall compiler.
    # The large stripped copies stay temporary; the declared output retains
    # hashes, cmp output, and section details rather than duplicate binaries.
    with tempfile.TemporaryDirectory(prefix="clickhouse-native-strip-", dir=args.output.parent) as temporary:
        scratch = Path(temporary)
        strip = scratch / "llvm-strip"
        try:
            os.link(args.cxx.resolve(strict=True), strip)
        except OSError:
            shutil.copy2(args.cxx.resolve(strict=True), strip)
        compare(reference, args.clickhouse, strip, scratch, record, expected["stripped_sha256"])
        if record["CMAKE_PLAIN_STRIPPED_SHA256"] != expected["plain_stripped_sha256"]:
            raise ValueError("Plain llvm-strip output differs from the recorded CMake comparison")
        if (record["CMAKE_STRIPPED_SHA256"] != expected["stripped_sha256"]
                or record["CMAKE_STRIPPED_BYTES"] != expected["stripped_bytes"]):
            raise ValueError("Normalized CMake SHA-256 or size differs from the recorded CMake comparison")
        if record["PARITY_STATUS"]:
            with (scratch / "stripped-cmp.log").open("rb") as log:
                record["PARITY_ERROR"] = log.read(2048).decode(errors="replace").strip() or "Normalized ELF mismatch"
        else:
            for name in ("stripped.sha256", "stripped-cmp.log"):
                shutil.copyfile(scratch / name, args.output / name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("reference", "clickhouse", "cc", "cxx", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    for name in ("reference", "clickhouse", "cc", "cxx", "output"):
        setattr(args, name, getattr(args, name).absolute())
    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    record = {"PARITY_ARCH": "aarch64", "PARITY_STATUS": 2}
    with tempfile.TemporaryDirectory(dir=output.parent, ignore_cleanup_errors=True) as scratch:
        args.output = Path(scratch) / "summary"
        args.output.mkdir()
        try:
            check(args, record)
            if record["PARITY_STATUS"] == 0:
                write_summary(record, args.output)
                (args.output / "status.json").write_text(json.dumps({
                    "status": 0, "phase": "compared", "error": None, "summary": record,
                }, indent=2) + "\n")
                args.output.replace(output)
                return 0
        except (OSError, ValueError, subprocess.CalledProcessError) as error:
            record.update(PARITY_STATUS=2, PARITY_ERROR=str(error)[:1024])
        fields = ("PARITY_STATUS", "PARITY_ERROR", "PARITY_STRIPPED_CMP_STATUS",
                  "CMAKE_STRIPPED_SHA256", "CMAKE_STRIPPED_BYTES", "BAZEL_STRIPPED_SHA256", "BAZEL_STRIPPED_BYTES")
        print("ClickHouseStrippedParity: " + json.dumps({k: record[k] for k in fields if k in record}),
              file=sys.stderr)
        return record["PARITY_STATUS"]


if __name__ == "__main__":
    raise SystemExit(main())
