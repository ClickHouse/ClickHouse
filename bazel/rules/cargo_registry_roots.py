"""Print versioned vendor directories for the crates.io packages in Cargo.lock."""

import json
from pathlib import Path
import re
import sys
import tomllib


def registry_roots(lock):
    with lock.open("rb") as stream:
        packages = tomllib.load(stream)["package"]
    roots = set()
    for package in packages:
        if "source" not in package:
            continue
        if package["source"] != "registry+https://github.com/rust-lang/crates.io-index":
            raise ValueError(f"{lock}: unsupported source for {package['name']}: {package['source']}")
        if not re.fullmatch(r"[0-9a-f]{64}", package.get("checksum", "")):
            raise ValueError(f"{lock}: missing or invalid checksum for {package['name']}")
        root = f"{package['name']}-{package['version']}"
        if root in roots:
            raise ValueError(f"{lock}: duplicate vendor directory {root}")
        roots.add(root)
    if not roots:
        raise ValueError(f"{lock}: no registry packages")
    return sorted(roots)


if __name__ == "__main__":
    print(json.dumps(registry_roots(Path(sys.argv[1]))))
