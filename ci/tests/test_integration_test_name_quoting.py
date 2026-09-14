"""
Tests for shell-safe passing of pytest test node IDs through the CI plumbing.

A parametrized integration test node ID can contain spaces, parentheses and quotes
when the test is parametrized with SQL, for example:

    test_refreshable_mat_view/test.py::test_simple_append[False-True-SELECT now() as a, number as b FROM numbers(2)]

Such a node ID travels through two shell (`shell=True`) boundaries before it reaches
pytest:

  1. `python -m ci.praktika run <job> --test <node id>` joins the `--test` values and the
     runner interpolates them into a `docker run ...` command executed by the host shell
     (`ci.praktika.__main__` / `ci.praktika.runner`).
  2. Inside the job, `ci.jobs.integration_test_job` builds the pytest command as a single
     string and runs it through the shell again.

If the node ID is interpolated unquoted, the shell splits it on spaces and chokes on the
parentheses (`syntax error near unexpected token '('`), so the test can never be selected.
Both boundaries must quote each node ID so it survives as a single, unmangled argument.

See ClickHouse/ClickHouse#91610.
"""

import os
import shlex
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.integration_test_job import quote_tests

# A representative parametrized node ID exercising spaces, parentheses, commas and the
# `[...]` parametrization payload - i.e. the exact shape from the issue.
PARAMETRIZED = (
    "test_refreshable_mat_view/test.py::test_simple_append"
    "[False-True-SELECT now() as a, number as b FROM numbers(2)]"
)
PLAIN = "test_storage_s3/test.py"
PLAIN_CASE = "test_storage_s3/test.py::test_simple"


# --- quote_tests (the job-side pytest command builder) -------------------------------


def test_quote_tests_roundtrips_parametrized_node_id():
    """A parametrized node ID must survive the shell as exactly one argument."""
    assert shlex.split(quote_tests([PARAMETRIZED])) == [PARAMETRIZED]


def test_quote_tests_keeps_multiple_node_ids_separate():
    """Each node ID stays a distinct argument even when one contains spaces."""
    tests = [PLAIN, PARAMETRIZED, PLAIN_CASE]
    assert shlex.split(quote_tests(tests)) == tests


def test_quote_tests_does_not_disturb_plain_module_paths():
    """Plain module paths need no quoting - the common case must stay readable."""
    assert quote_tests([PLAIN, PLAIN_CASE]) == f"{PLAIN} {PLAIN_CASE}"


def test_quote_tests_empty():
    assert quote_tests([]) == ""


# --- praktika CLI --test quoting (the host-shell / docker-run boundary) --------------


def _praktika_test_arg(test_values):
    """Reproduce how `ci.praktika.__main__` turns `--test` values into the string the
    runner interpolates into the `docker run ...` command run by the host shell."""
    return " ".join(shlex.quote(t) for t in test_values)


def test_praktika_test_arg_roundtrips_parametrized_node_id():
    """The runner interpolates this into a shell command; splitting must recover the
    original single argument (what docker then passes as one argv to the job)."""
    assert shlex.split(_praktika_test_arg([PARAMETRIZED])) == [PARAMETRIZED]


def test_praktika_test_arg_keeps_multiple_values_separate():
    values = [PLAIN, PARAMETRIZED]
    assert shlex.split(_praktika_test_arg(values)) == values
