#!/usr/bin/env python3
"""
Run the Mintlify docs checks for a slice of a larger docs site.

Run this inside the docs image (``clickhouse/docs-builder``), which provides
``mint`` along with ``git`` and ``python3``; nothing is installed locally. A
consuming repo vendors nothing -- its CI runs the image as the job container,
fetches this one file from the aggregator, and runs it.

Steps:
  1. Shallow + sparse clone the aggregator docs repo (the docs root and the
     docs CI scripts at ``ci/jobs/scripts/docs``).
  2. Replace one or more folders in it with local folders (e.g. drop the
     locally edited Python client docs over
     ``integrations/language-clients/python``). The destination is wiped first,
     so deletions and renames in the local source are reflected -- the docs sync
     is one-way, so the checks see exactly what the next sync would produce.
  3. Run the checks from the docs root against the resulting docs.

The check definitions live in ``DEFAULT_CHECKS`` so they are declared once and
shared: this driver runs them directly, as does the Praktika job
(``ci/jobs/docs_job_mintlify.py``), which imports the same list. Add a new check
here and both pick it up.

A check can be any shell command, including ``python3 <script>``. Checks run
from the docs root, so a Python check script committed at
``ci/jobs/scripts/docs`` is referenced relative to the docs root --
``python3 ../ci/jobs/scripts/docs/foo.py`` -- which resolves because the sparse
clone includes that directory. A consuming repo therefore gets those scripts
for free from the clone; it never needs to vendor them.
"""

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile

# The checks to run (name, shell command), executed from the docs root. Kept
# here, independent of any CI framework, as the single source of truth shared
# with the Praktika job.
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
    # Fetch only what the checks need: the tip of `ref` with no history
    # (--depth 1), only the trees/blobs under the sparse paths (blob:none filter
    # + sparse-checkout), and nothing else. `git clone` without --depth would
    # download the aggregator's entire commit history -- enormous for a repo like
    # ClickHouse -- so build the repo with `git init` + a single shallow fetch
    # instead, which never materialises history or out-of-slice files.
    run(["git", "init", "-q", dest], check=True)
    run(["git", "-C", dest, "remote", "add", "origin", repo], check=True)
    run(["git", "-C", dest, "config", "remote.origin.promisor", "true"], check=True)
    run(["git", "-C", dest, "config", "remote.origin.partialclonefilter", "blob:none"], check=True)
    run(["git", "-C", dest, "sparse-checkout", "init", "--cone"], check=True)
    run(["git", "-C", dest, "sparse-checkout", "set", *sparse], check=True)
    run(["git", "-C", dest, "fetch", "--depth", "1", "--filter=blob:none", "origin", ref], check=True)
    run(["git", "-C", dest, "checkout", "FETCH_HEAD"], check=True)


def replace(src, dest):
    src = os.path.abspath(src)
    if not os.path.isdir(src):
        raise FileNotFoundError(f"Replace source is not a directory: {src}")
    print(f"Replace {dest} with {src}", flush=True)
    # Wipe dest first: the docs sync is one-way (the consuming repo is the source
    # of truth), so the checks must see exactly what the next sync produces. A
    # merge would leave stale files from the cloned aggregator behind, hiding
    # deletions and renames in the source repo.
    if os.path.exists(dest):
        shutil.rmtree(dest)
    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
    shutil.copytree(src, dest)


def run_check(docs_root, name, command):
    # Checks run from the docs root. A command may reach a sibling CI script via
    # the docs root (e.g. ../ci/jobs/scripts/docs/foo.py); that path resolves
    # because the sparse clone includes ci/jobs/scripts/docs. `mint` and friends
    # come from the docs image this script is expected to run inside.
    print(f"\n=== {name} ===", flush=True)
    return run(["sh", "-lc", command], cwd=docs_root).returncode == 0


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", required=True,
                   help="Aggregator docs repo to shallow/sparse clone.")
    p.add_argument("--ref", default="HEAD", help="Branch/tag/sha (default: HEAD).")
    p.add_argument("--sparse", action="append", default=None,
                   help="Sparse-checkout path (repeatable; default: the docs root "
                        "plus ci/jobs/scripts/docs).")
    p.add_argument("--docs-root", default="docs",
                   help="Dir containing docs.json within the clone (default: docs).")
    p.add_argument("--replace", action="append", default=[], metavar="SRC:DEST",
                   help="Replace DEST (relative to docs root) with the local folder "
                        "SRC, wiping DEST first; repeatable.")
    p.add_argument("--workdir", help="Where to clone (default: a temp dir).")
    args = p.parse_args(argv)

    base = args.workdir or tempfile.mkdtemp(prefix="docs-check-")
    clone_dir = os.path.abspath(os.path.join(base, "aggregator"))
    sparse = args.sparse or [args.docs_root, "ci/jobs/scripts/docs"]
    clone_aggregator(args.repo, args.ref, sparse, clone_dir)

    docs_root = os.path.join(clone_dir, args.docs_root)
    if not os.path.isfile(os.path.join(docs_root, "docs.json")):
        raise FileNotFoundError(f"No docs.json in docs root: {docs_root}")

    for spec in args.replace:
        parts = spec.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid --replace '{spec}'. Expected SRC:DEST.")
        replace(parts[0], os.path.join(docs_root, parts[1]))

    results = [(name, run_check(docs_root, name, command))
               for name, command in DEFAULT_CHECKS]

    print("\n=== Summary ===", flush=True)
    for name, ok in results:
        print(f"[{'PASS' if ok else 'FAIL'}] {name}", flush=True)
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    sys.exit(main())