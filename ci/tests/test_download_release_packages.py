"""
Tests for `download_release_packages.download_packages`.

The upgrade check downloads the previous release `.deb` packages before it can
install and start the old server. These tests pin two properties:

  * a failure of an *essential* package (one that `install_packages` installs)
    is reported as a hard, attributable error instead of being silently skipped;
  * a failure of a non-essential asset (e.g. `clickhouse-keeper`) is tolerated,
    so an unrelated hiccup does not waste the whole job.

See https://github.com/ClickHouse/ClickHouse/pull/102817#issuecomment-4726000791
"""

import os
import sys

import pytest

# `download_release_packages` imports sibling modules from `tests/ci` by bare
# name, so put that directory on `sys.path` only while importing it and remove
# it again afterwards to avoid leaking it into the rest of the pytest session.
_CI_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "tests", "ci")
)
sys.path.insert(0, _CI_DIR)
try:
    # pylint: disable=import-error
    import download_release_packages as drp
    from build_download_helper import DownloadException
finally:
    sys.path.remove(_CI_DIR)


class FakeRelease:
    def __init__(self, *package_names):
        self.assets = {name: f"http://example/{name}" for name in package_names}

    def __str__(self):
        return "v1.2.3.4-stable"


def _make_fake_download(fail_substrings=()):
    calls = []

    def fake(url, path, retries=None):
        calls.append({"url": url, "path": str(path), "retries": retries})
        for sub in fail_substrings:
            if sub in str(path):
                raise DownloadException(f"simulated failure (HTTP 404) for {sub}")

    return fake, calls


def test_all_packages_succeed(tmp_path, monkeypatch):
    fake, calls = _make_fake_download()
    monkeypatch.setattr(drp, "download_build_with_progress", fake)
    release = FakeRelease(
        "clickhouse-common-static_1_amd64.deb",
        "clickhouse-server_1_amd64.deb",
        "clickhouse-client_1_amd64.deb",
    )

    drp.download_packages(release, dest_path=tmp_path)

    assert len(calls) == 3
    # The essential packages are retried with the larger, dedicated budget.
    assert all(c["retries"] == drp.RELEASE_PACKAGE_DOWNLOAD_RETRIES for c in calls)


def test_non_essential_failure_is_tolerated(tmp_path, monkeypatch):
    fake, _ = _make_fake_download(fail_substrings=("clickhouse-keeper",))
    monkeypatch.setattr(drp, "download_build_with_progress", fake)
    release = FakeRelease(
        "clickhouse-common-static_1_amd64.deb",
        "clickhouse-server_1_amd64.deb",
        "clickhouse-client_1_amd64.deb",
        "clickhouse-keeper_1_amd64.deb",
    )

    # Only an unused extra package failed - the job can still run.
    drp.download_packages(release, dest_path=tmp_path)


def test_essential_failure_raises_with_reason(tmp_path, monkeypatch):
    fake, _ = _make_fake_download(fail_substrings=("clickhouse-server",))
    monkeypatch.setattr(drp, "download_build_with_progress", fake)
    release = FakeRelease(
        "clickhouse-common-static_1_amd64.deb",
        "clickhouse-server_1_amd64.deb",
        "clickhouse-client_1_amd64.deb",
    )

    with pytest.raises(DownloadException) as excinfo:
        drp.download_packages(release, dest_path=tmp_path)

    message = str(excinfo.value)
    assert "required release package" in message
    # The original per-package reason is preserved for diagnostics.
    assert "clickhouse-server_1_amd64.deb" in message
    assert "HTTP 404" in message


def test_missing_required_package_raises(tmp_path, monkeypatch):
    # A partially published release: every published asset downloads fine, but a
    # required package is absent from the release metadata entirely, so it never
    # enters the download loop. This must still fail loudly instead of letting a
    # later `install_packages` die with an opaque `dpkg` glob error.
    fake, calls = _make_fake_download()
    monkeypatch.setattr(drp, "download_build_with_progress", fake)
    release = FakeRelease(
        "clickhouse-common-static_1_amd64.deb",
        "clickhouse-server_1_amd64.deb",
        # `clickhouse-client_..._amd64.deb` is missing.
    )

    with pytest.raises(DownloadException) as excinfo:
        drp.download_packages(release, dest_path=tmp_path)

    message = str(excinfo.value)
    assert "required release package" in message
    assert "clickhouse-client_" in message
    assert "not found in the release assets" in message
    # The packages that were present still got downloaded.
    assert len(calls) == 2


def test_missing_dbg_is_required_only_in_debug_mode(tmp_path, monkeypatch):
    # The debug-symbols package is only installed (and thus required) in debug
    # mode, so a release without it is fine for a non-debug run but must fail a
    # debug run.
    fake, _ = _make_fake_download()
    monkeypatch.setattr(drp, "download_build_with_progress", fake)
    release = FakeRelease(
        "clickhouse-common-static_1_amd64.deb",
        "clickhouse-server_1_amd64.deb",
        "clickhouse-client_1_amd64.deb",
        # `clickhouse-common-static-dbg_..._amd64.deb` is missing.
    )

    # Non-debug run: the debug package is not needed.
    drp.download_packages(release, dest_path=tmp_path, debug=False)

    # Debug run: the missing debug package is now a required, attributable error.
    with pytest.raises(DownloadException) as excinfo:
        drp.download_packages(release, dest_path=tmp_path, debug=True)

    message = str(excinfo.value)
    assert "clickhouse-common-static-dbg_" in message
    assert "not found in the release assets" in message
