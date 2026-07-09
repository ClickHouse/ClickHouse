#!/usr/bin/env python3
"""Generate (or verify) images.txt - the list of external service images baked
into the clickhouse/integration-images-cache image.

The list is derived from tests/integration/compose/*.yml. Only images that are
version-locked inline in the compose files are included:
  - `${VAR:-default}` placeholders are resolved to their defaults;
  - images without a tag or with the mutable `latest` tag are excluded (they
    must keep being pulled per job, otherwise the preseed would silently pin
    a stale `latest`);
  - `clickhouse/*` images whose tag comes from a placeholder are excluded:
    those are per-commit CI images (see IMAGES_ENV in
    ci/jobs/scripts/integration_tests_configs.py) and are pulled fresh anyway.

The build of clickhouse/integration-images-cache pulls exactly this list (see
pull.sh); its build context is only this directory, so the list has to be a
committed file rather than parsed from the compose files at build time. The
`integration_images_cache` style check runs this script with --check to fail
CI when the compose files and images.txt drift apart. Regenerate with:

    python3 ci/docker/integration-images-cache/generate_images_list.py --update

Updating images.txt changes this directory's digest, which is what triggers
the rebuild of the cache image.
"""

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE_DIR = REPO_ROOT / "tests" / "integration" / "compose"
IMAGES_TXT = Path(__file__).resolve().parent / "images.txt"

# Images referenced by compose files that must never be preseeded.
EXCLUDED = {
    # Built locally from tests/integration/compose/Dockerfile.raftkeeper,
    # never pulled from a registry.
    "raftkeeper:test",
}

HEADER = """\
# External service images baked into clickhouse/integration-images-cache.
# AUTO-GENERATED - do not edit by hand. Regenerate with:
#   python3 ci/docker/integration-images-cache/generate_images_list.py --update
# The `integration_images_cache` style check fails when this file is out of
# sync with tests/integration/compose/*.yml.
"""


def collect_images() -> list:
    images = set()
    for compose_file in sorted(COMPOSE_DIR.glob("*.yml")):
        content = compose_file.read_text()
        for m in re.finditer(r"^\s+image:\s+(.+)$", content, re.MULTILINE):
            # Strip inline YAML comments from unquoted values.
            raw = re.sub(r"\s+#.*$", "", m.group(1).strip())
            had_placeholder = "${" in raw
            resolved = re.sub(
                r"\$\{(\w+)(?::-([^}]*))?\}", lambda m: m.group(2) or "", raw
            )
            if not resolved or resolved in EXCLUDED:
                continue
            _, _, tag = resolved.rpartition(":")
            if ":" not in resolved or tag == "latest":
                continue  # mutable reference - keep pulling it per job
            if had_placeholder and resolved.startswith("clickhouse/"):
                continue  # per-commit CI image, tag resolved at job runtime
            images.add(resolved)
    return sorted(images)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--update", action="store_true", help="rewrite images.txt in place"
    )
    mode.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if images.txt is out of sync with the compose files",
    )
    args = parser.parse_args()

    images = collect_images()
    generated = HEADER + "\n".join(images) + "\n"

    if args.update:
        IMAGES_TXT.write_text(generated)
        print(f"Wrote {len(images)} image(s) to {IMAGES_TXT}")
        return 0

    if args.check:
        current = IMAGES_TXT.read_text() if IMAGES_TXT.exists() else ""
        if current != generated:
            current_images = {
                line.strip()
                for line in current.splitlines()
                if line.strip() and not line.startswith("#")
            }
            missing = sorted(set(images) - current_images)
            stale = sorted(current_images - set(images))
            print(
                f"{IMAGES_TXT.relative_to(REPO_ROOT)} is out of sync with "
                "tests/integration/compose/*.yml."
            )
            if missing:
                print("  missing from images.txt: " + ", ".join(missing))
            if stale:
                print("  no longer referenced by compose files: " + ", ".join(stale))
            print(
                "Regenerate with: python3 "
                "ci/docker/integration-images-cache/generate_images_list.py --update"
            )
            return 1
        print(f"{IMAGES_TXT.relative_to(REPO_ROOT)} is in sync ({len(images)} images)")
        return 0

    print("\n".join(images))
    return 0


if __name__ == "__main__":
    sys.exit(main())
