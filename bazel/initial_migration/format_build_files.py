#!/usr/bin/env python3
"""Format ClickHouse BUILD files without changing CMake source or link order."""

import argparse
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[2]


def format_build_files(root=ROOT, check=False):
    """Include new generated overlays, but not submodules or Bazel outputs."""
    listing = subprocess.check_output(
        [
            "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard",
            "--", "BUILD", "BUILD.bazel", "**/BUILD", "**/BUILD.bazel",
            "*.BUILD", "*.BUILD.bazel",
        ],
        cwd=root,
    ).decode().split("\0")
    files = [name for name in dict.fromkeys(listing) if name and (root / name).is_file()]
    subprocess.run(
        [
            "buildifier", "-config=" + str(root / ".buildifier.json"), "-type=build",
            "-mode=" + ("check" if check else "fix"),
            *files,
        ],
        cwd=root,
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Report unformatted files without changing them.")
    args = parser.parse_args()
    format_build_files(check=args.check)


if __name__ == "__main__":
    main()
