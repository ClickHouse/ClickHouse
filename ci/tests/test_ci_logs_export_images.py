"""
Guards the hard-coded tables of `tests/integration/helpers/ci_logs_export.py`
against silent rot.

The helper decides per instance whether to export its system log tables, and it
must answer "does this container run the binary under test?". That cannot be
probed before the container starts, so the helper carries an explicit map of the
integration-test Docker images to the environment variable holding the tag built
for the current commit. Two ways for that map to go stale, both silent - the
suites just stop exporting logs, and nothing turns red:

  - a new image derived from `clickhouse/integration-test` is added (the way
    `clickhouse/integration-test-with-unity-catalog` was) and not listed here;
  - the environment variable of a listed image is renamed in `IMAGES_ENV`, so the
    tag never matches.

The `_watcher` materialized views also run as `ci_logs_sender`, whose profile
pins the export settings (short timeouts, async inserts, no cache pollution)
regardless of the settings of the query that triggered the view. The user comes
from the config the functional tests install, which the base server config does
not ship, so its absence would only show up as a per-table warning in the pytest
log.

See ClickHouse/ClickHouse#116031 (review).
"""

import importlib.util
import os
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "ci"))

from ci.defs.defs import DOCKERS
from ci.jobs.scripts import log_export
from ci.jobs.scripts.integration_tests_configs import IMAGES_ENV

BASE_IMAGE = "clickhouse/integration-test"


def _load_helper():
    """Load the helper by path: importing it as `helpers.ci_logs_export` would
    put the integration-test `helpers` package on the path of the whole run, and
    `helpers.cluster` needs third-party modules this job does not have."""
    path = REPO_ROOT / "tests" / "integration" / "helpers" / "ci_logs_export.py"
    spec = importlib.util.spec_from_file_location("_ci_logs_export", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


HELPER = _load_helper()


def _images_running_the_binary_under_test():
    """The integration-test images whose containers run the ClickHouse binary of
    the commit under test: the base image and every image whose Dockerfile is
    `FROM clickhouse/integration-test`."""
    images = {BASE_IMAGE}
    for docker in DOCKERS:
        dockerfile = REPO_ROOT / docker.path / "Dockerfile"
        if not dockerfile.exists():
            continue
        for line in dockerfile.read_text().splitlines():
            match = re.match(r"\s*FROM\s+(\S+)", line, re.IGNORECASE)
            if match and match.group(1).split(":")[0] == BASE_IMAGE:
                images.add(docker.name)
    return images


def test_every_current_binary_image_is_listed():
    assert (
        HELPER.CURRENT_BINARY_IMAGE_TAG_ENV.keys()
        == _images_running_the_binary_under_test()
    )


@pytest.mark.parametrize("image", sorted(HELPER.CURRENT_BINARY_IMAGE_TAG_ENV))
def test_tag_environment_variable_matches_images_env(image):
    assert HELPER.CURRENT_BINARY_IMAGE_TAG_ENV[image] == IMAGES_ENV[image]


def test_eligibility_follows_the_tag_of_the_image(monkeypatch):
    monkeypatch.setenv("DOCKER_BASE_TAG", "abcdef")
    monkeypatch.setenv("DOCKER_BASE_WITH_UNITY_CATALOG_TAG", "123456")
    assert HELPER.runs_binary_under_test(BASE_IMAGE, "abcdef")
    assert not HELPER.runs_binary_under_test(BASE_IMAGE, "123456")
    assert HELPER.runs_binary_under_test(
        "clickhouse/integration-test-with-unity-catalog", "123456"
    )
    # An old release image, and a mock HTTP service that is not a ClickHouse server
    assert not HELPER.runs_binary_under_test("clickhouse/clickhouse-server", "abcdef")
    assert not HELPER.runs_binary_under_test("clickhouse/python-bottle", "abcdef")


def test_sender_user_config_is_the_one_functional_tests_install():
    config = Path(HELPER.SENDER_USER_CONFIG).read_text()
    assert os.path.basename(HELPER.SENDER_USER_CONFIG) == "ci_logs_sender.yaml"
    assert f"    {HELPER.SENDER_USER}:" in config
    assert "constraints:" in config
    assert "async_insert: 1" in config


def _extra_column_names(extra_columns):
    """The names of the columns declared by an EXTRA_COLUMNS string, in order,
    without the index declarations."""
    names = []
    for item in extra_columns.split(","):
        item = item.strip()
        if not item or item.startswith("INDEX "):
            continue
        names.append(item.split()[0])
    return names


def _aliases(expression):
    """The aliases of a SELECT expression list, in order. Only the alias of the
    whole element counts, so that the `AS` of a nested `CAST(x AS UInt32)` is not
    mistaken for one."""
    elements = []
    depth = 0
    current = []
    for char in expression:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            elements.append("".join(current))
            current = []
        else:
            current.append(char)
    elements.append("".join(current))
    names = []
    for element in elements:
        found = re.findall(r"\bAS (\w+)", element)
        assert found, f"no alias in {element!r}"
        names.append(found[-1])
    return names


class _FakeInfo:
    """The CI run identity `log_export` builds the expression from."""

    repo_name = "ClickHouse/ClickHouse"
    pr_number = 0
    sha = "0" * 40
    job_name = "Some job"
    instance_type = "c5.large"
    instance_id = "i-01234567"


SETUP_LOG_CLUSTER = (
    REPO_ROOT / "ci" / "jobs" / "scripts" / "functional_tests" / "setup_log_cluster.sh"
)


def _shell_default(variable):
    """The default value of a `VAR=${VAR:-"..."}` assignment in
    setup_log_cluster.sh."""
    match = re.search(
        "^" + variable + r"=\$\{" + variable + r':-"(.*)"\}$',
        SETUP_LOG_CLUSTER.read_text(),
        re.MULTILINE,
    )
    assert match, f"no default of {variable} in {SETUP_LOG_CLUSTER}"
    return match.group(1)


def test_destination_structure_is_shared_with_the_functional_tests():
    """The structure hash of a destination table is computed from the columns,
    so the functional and the integration tests only share a table while these
    two declarations are identical."""
    assert _shell_default("EXTRA_COLUMNS") == HELPER.EXTRA_COLUMNS
    assert _shell_default("EXTRA_ORDER_BY_COLUMNS") == HELPER.EXTRA_ORDER_BY_COLUMNS


def test_job_expression_follows_the_column_order(monkeypatch):
    """A `SELECT {expression}, *` in a different order than EXTRA_COLUMNS gives
    the local sender table a different header than the destination table, so
    `Distributed` converts every exported batch by name and logs a warning for
    each of them - which `system.text_log` then exports as well."""
    monkeypatch.setattr(log_export, "Info", _FakeInfo)
    assert _aliases(log_export.extra_columns_expression(0)) == _extra_column_names(
        HELPER.EXTRA_COLUMNS
    )


def test_helper_expression_follows_the_column_order(monkeypatch):
    """The integration tests take the expression in two parts and insert the
    per-server `test_name` and `node_name` between them."""
    monkeypatch.setattr(log_export, "Info", _FakeInfo)
    monkeypatch.delenv(HELPER.EXTRA_COLUMNS_EXPRESSION_HEAD_ENV, raising=False)
    monkeypatch.delenv(HELPER.EXTRA_COLUMNS_EXPRESSION_TAIL_ENV, raising=False)
    expected = _extra_column_names(HELPER.EXTRA_COLUMNS)
    # The default expression of a local run
    assert _aliases(HELPER._extra_columns_expression("test", "node")) == expected
    # And the one built from the values the CI job provides
    monkeypatch.setenv(
        HELPER.EXTRA_COLUMNS_EXPRESSION_HEAD_ENV,
        log_export.extra_columns_expression_head(0),
    )
    monkeypatch.setenv(
        HELPER.EXTRA_COLUMNS_EXPRESSION_TAIL_ENV,
        log_export.extra_columns_expression_tail(),
    )
    assert _aliases(HELPER._extra_columns_expression("test", "node")) == expected


def test_cache_directory_of_a_local_run_is_per_session(monkeypatch):
    """Without the CI identity the markers would be shared by every run on the
    machine, so a transient outage in one run would suppress the export in all
    the later ones."""
    for name in (
        "CLICKHOUSE_CI_LOGS_CACHE_DIR",
        HELPER.EXTRA_COLUMNS_EXPRESSION_HEAD_ENV,
        HELPER.EXTRA_COLUMNS_EXPRESSION_TAIL_ENV,
        "INTEGRATION_TESTS_RUN_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("CLICKHOUSE_CI_LOGS_HOST", "logs.example.com")
    monkeypatch.setenv(HELPER.LOCAL_RUN_ID_ENV, "0" * 32)
    first = HELPER._cache_dir()
    monkeypatch.setenv(HELPER.LOCAL_RUN_ID_ENV, "1" * 32)
    assert HELPER._cache_dir() != first
    # The workers of one run share the id, and so the markers
    monkeypatch.setenv(HELPER.LOCAL_RUN_ID_ENV, "0" * 32)
    assert HELPER._cache_dir() == first


def test_a_successful_probe_wins_over_a_later_failure(tmp_path):
    """One pytest-xdist worker hitting a transient failure must not disable the
    export for the whole job after another worker has already connected."""
    assert HELPER._disabled_reason(tmp_path) is None
    (tmp_path / "disabled").write_text("cannot connect")
    assert HELPER._disabled_reason(tmp_path) == "cannot connect"
    (tmp_path / "connected").touch()
    assert HELPER._disabled_reason(tmp_path) is None
