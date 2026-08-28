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
