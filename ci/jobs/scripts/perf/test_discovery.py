"""Harness-side discovery and classification of performance tests.

The single place that answers "which files are perf tests?" and "what does this
test need / do?" for the CI jobs (performance_tests.py, collect_clickhouse_profiles.py).

The requires_* predicates read only root attributes, so this module stays
import-cheap: a job that merely detects S3 tests must not pull in boto3 /
seaweedfs_service / dataset_download. perf.py keeps its own in-process reads of
run-time attributes (run_all_queries, ...) because it must run standalone with no
ci/ imports; this module is deliberately the harness-side counterpart only.
"""

import os
from xml.etree import ElementTree


def list_test_files(perf_dir):
    """Sorted basenames of the perf tests in a directory.

    A perf test is a `*.xml` file; `*.xml.inc` fragments are `<include>` targets,
    not tests, and are excluded here (as they are by compare.sh's `ls *.xml`).
    Sorted so test selection (keyword / batch sharding) is deterministic rather
    than dependent on filesystem order.
    """
    return sorted(f for f in os.listdir(perf_dir) if f.endswith(".xml"))


def test_requires_s3(test_path):
    """Whether a test declares that it needs the job-local S3 endpoint."""
    return ElementTree.parse(test_path).getroot().get("requires_s3") == "1"


def test_requires_read_dataset(test_path):
    """Whether a test needs the shared TPC-H dataset attached before startup."""
    return ElementTree.parse(test_path).getroot().get("requires_s3_read_dataset") == "1"


def _iter_queries(parent, source_dir, stack):
    """Yield `<query>` elements, descending into `<include>`d `<fragment>` files.

    A detection-only counterpart to perf.py's expand_includes: it follows the same
    `<include file="..."/>` -> `<fragment>` structure (paths relative to the file
    that declares them, cycle-guarded), but only needs to find elements, not splice
    or rewrite them. A missing or malformed fragment is skipped here; perf.py
    reports the real error when it runs the test.
    """
    for element in parent:
        if element.tag == "include":
            filename = element.get("file")
            if not filename:
                continue
            path = os.path.realpath(os.path.join(source_dir, filename))
            if path in stack:
                continue
            try:
                fragment = ElementTree.parse(path).getroot()
            except (OSError, ElementTree.ParseError):
                continue
            yield from _iter_queries(fragment, os.path.dirname(path), stack | {path})
            continue
        if element.tag == "query":
            yield element
        yield from _iter_queries(element, source_dir, stack)


def test_has_shell_query(test_path):
    """Whether a test contains a `<query type="shell">`, including via `<include>`.

    Profile collection runs each test against a single instrumented server started
    without an HTTP port, invoking perf.py without `--binary` / `--http-port`. Shell
    queries build `$CLICKHOUSE_BINARY` / `$CLICKHOUSE_LOCAL` from `--binary` and
    `$CLICKHOUSE_URL` from `--http-port`, so they would pick up the wrong executable
    and an endpoint that is not listening; such tests are skipped for profile
    collection - including a shell query pulled in from a shared fragment. A parse
    error is treated as "no shell query" so the test still runs and perf.py reports
    the real error.
    """
    try:
        root = ElementTree.parse(test_path).getroot()
    except ElementTree.ParseError:
        return False
    source_dir = os.path.dirname(os.path.abspath(test_path))
    return any(
        q.get("type") == "shell"
        for q in _iter_queries(root, source_dir, {os.path.realpath(test_path)})
    )
