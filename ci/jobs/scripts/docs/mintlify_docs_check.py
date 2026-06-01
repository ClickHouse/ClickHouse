#!/usr/bin/env python3
"""
Run the Mintlify docs checks for a slice of a larger docs site.

Self-contained: a consuming repo vendors only this file. The host needs
``docker``, ``git`` and ``python3`` -- ``mint`` is provided by the docs image,
never installed locally.

Steps:
  1. Pull the docs image (``clickhouse/docs-builder`` by default).
  2. Shallow + sparse clone the aggregator docs repo (``docs`` and the docs CI
     scripts at ``ci/jobs/scripts/docs``).
  3. Overlay one or more local folders onto it (e.g. drop the locally edited
     Python client docs over ``integrations/language-clients/python``).
  4. Run the checks inside the image against the overlaid docs.

The check definitions live in ``DEFAULT_CHECKS`` so they are declared once and
shared: this driver runs them via ``docker run``, while the Praktika job
(``ci/jobs/docs_job_mintlify.py``) imports the same list and runs them natively
inside the container. Add a new check here and both pick it up.

A check can be any shell command, including ``python3 <script>``. The clone
root is mounted into the container and checks run from the docs root, so a
Python check script committed at ``ci/jobs/scripts/docs`` is referenced relative
to the docs root -- ``python3 ../ci/jobs/scripts/docs/foo.py`` -- which resolves
identically here and in the Praktika job. A consuming repo therefore gets those
scripts for free from the clone; it never needs to vendor them.
"""

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile

# The checks to run (name, shell command), executed from the docs root inside
# the image. Kept here, independent of any CI framework, as the single source
# of truth shared with the Praktika job.
#
# A command may invoke a Python check script committed at ci/jobs/scripts/docs,
# referenced relative to the docs root, e.g.:
#     ("Frontmatter lint", "python3 ../ci/jobs/scripts/docs/frontmatter_lint.py ."),
DEFAULT_CHECKS = [
    ("Validate docs.json", "mint validate"),
    ("Check for broken links", "mint broken-links"),
]


def run(cmd, **kw):
    print("+ " + " ".join(shlex.quote(c) for c in cmd), flush=True)
    return subprocess.run(cmd, **kw)


def clone_aggregator(repo, ref, sparse, dest):
    run(["git", "clone", "--filter=blob:none", "--no-checkout", repo, dest], check=True)
    run(["git", "-C", dest, "sparse-checkout", "init", "--cone"], check=True)
    run(["git", "-C", dest, "sparse-checkout", "set", *sparse], check=True)
    run(["git", "-C", dest, "fetch", "--depth", "1", "origin", ref], check=True)
    run(["git", "-C", dest, "checkout", "FETCH_HEAD"], check=True)


def overlay(src, dest, mirror):
    src = os.path.abspath(src)
    if not os.path.isdir(src):
        raise FileNotFoundError(f"Overlay source is not a directory: {src}")
    print(f"Overlay {src} -> {dest} ({'mirror' if mirror else 'merge'})", flush=True)
    if mirror and os.path.exists(dest):
        shutil.rmtree(dest)
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    shutil.copytree(src, dest, dirs_exist_ok=True)


def check_in_image(image, mount_root, workdir, name, command):
    # Mount the whole clone root (not just the docs root) so check commands can
    # reach sibling CI scripts (e.g. ../ci/jobs/scripts/docs) the same way the
    # native Praktika run does. Checks still run from the docs root.
    print(f"\n=== {name} ===", flush=True)
    return run(
        ["docker", "run", "--rm", "-v", f"{mount_root}:/work", "-w", f"/work/{workdir}",
         image, "sh", "-lc", command]
    ).returncode == 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--image", default="clickhouse/docs-builder",
                   help="Docker image providing mint (default: %(default)s).")
    p.add_argument("--no-pull", action="store_true", help="Skip docker pull.")
    p.add_argument("--repo", required=True,
                   help="Aggregator docs repo to shallow/sparse clone.")
    p.add_argument("--ref", default="HEAD", help="Branch/tag/sha (default: HEAD).")
    p.add_argument("--sparse", action="append", default=None,
                   help="Sparse-checkout path (repeatable; default: the docs root "
                        "plus ci/jobs/scripts/docs).")
    p.add_argument("--docs-root", default="docs",
                   help="Dir containing docs.json within the clone (default: docs).")
    p.add_argument("--overlay", action="append", default=[], metavar="SRC:DEST[:mirror]",
                   help="Overlay a local folder onto DEST (relative to docs root); "
                        "repeatable.")
    p.add_argument("--workdir", help="Where to clone (default: a temp dir).")
    args = p.parse_args(argv)

    if not args.no_pull:
        run(["docker", "pull", args.image], check=True)

    base = args.workdir or tempfile.mkdtemp(prefix="docs-check-")
    clone_dir = os.path.abspath(os.path.join(base, "aggregator"))
    sparse = args.sparse or [args.docs_root, "ci/jobs/scripts/docs"]
    clone_aggregator(args.repo, args.ref, sparse, clone_dir)

    docs_root = os.path.join(clone_dir, args.docs_root)
    if not os.path.isfile(os.path.join(docs_root, "docs.json")):
        raise FileNotFoundError(f"No docs.json in docs root: {docs_root}")

    for spec in args.overlay:
        parts = spec.split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid --overlay '{spec}'. Expected SRC:DEST[:mirror].")
        mirror = len(parts) >= 3 and parts[2].lower() in ("1", "true", "mirror", "yes")
        overlay(parts[0], os.path.join(docs_root, parts[1]), mirror)

    results = [(name, check_in_image(args.image, clone_dir, args.docs_root, name, command))
               for name, command in DEFAULT_CHECKS]

    print("\n=== Summary ===", flush=True)
    for name, ok in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}", flush=True)
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())