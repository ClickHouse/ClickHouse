"""
Tests for `get_previous_release_tag`, the resolver that picks the baseline the
Upgrade check installs and starts before upgrading to the build under test.

These tests pin two properties:

  * the resolved baseline is the newest release strictly below the version under
    test, regardless of whether that release's metadata already lists uploaded
    packages (package availability is validated later, at the download boundary);
  * a first page that cannot answer fails loudly instead of falling through to an
    older release from a later page.

See https://github.com/ClickHouse/ClickHouse/pull/114376 (Upgrade check on
`e1cb0dee5e9` resolved `v25.1.4.53-stable` as the baseline for a 26.8.1.1 build).
"""

import os
import sys

import pytest

# `get_previous_release_tag` imports sibling modules from `tests/ci` by bare
# name, so put that directory on `sys.path` only while importing it and remove
# it again afterwards to avoid leaking it into the rest of the pytest session.
_CI_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "tests", "ci")
)
sys.path.insert(0, _CI_DIR)
try:
    # pylint: disable=import-error
    import get_previous_release_tag as gprt
    from version_helper import get_version_from_string
finally:
    sys.path.remove(_CI_DIR)


def _asset(tag):
    version = tag.lstrip("v").split("-", maxsplit=1)[0]
    name = f"clickhouse-common-static_{version}_amd64.deb"
    return [{"name": name, "state": "uploaded", "browser_download_url": "http://x"}]


def _release(tag, with_assets=True):
    return {"tag_name": tag, "assets": _asset(tag) if with_assets else []}


def _install_pager(monkeypatch, pages):
    """Stub `get_gh_api` with a canned pager and record the pages requested."""
    requested = []

    class FakeResponse:
        def __init__(self, payload):
            self.ok = True
            self.reason = "OK"
            self._payload = payload

        def json(self):
            return self._payload

        def raise_for_status(self):
            pass

    def fake_get_gh_api(url, **kwargs):
        page = kwargs.get("params", {}).get("page", 1)
        requested.append(page)
        return FakeResponse(pages.get(page, []))

    monkeypatch.setattr(gprt, "get_gh_api", fake_get_gh_api)
    return requested


def test_degraded_first_page_does_not_fall_through_to_a_stale_release(monkeypatch):
    """The reported regression: page 1 lists the correct release without assets."""
    requested = _install_pager(
        monkeypatch,
        {
            1: [
                _release("v26.7.3.19-stable", with_assets=False),
                _release("v26.6.2.160-stable", with_assets=False),
            ],
            3: [_release("v25.1.4.53-stable")],
        },
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.7.3.19-stable"
    assert requested == [1]


def test_newer_release_without_assets_beats_older_one_with_assets(monkeypatch):
    _install_pager(
        monkeypatch,
        {
            1: [
                _release("v25.1.4.53-stable"),
                _release("v26.7.3.19-stable", with_assets=False),
            ]
        },
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.7.3.19-stable"


def test_happy_path(monkeypatch):
    _install_pager(
        monkeypatch,
        {
            1: [
                _release("v26.8.1.1-stable"),
                _release("v26.7.3.19-stable"),
                _release("v26.6.2.160-stable"),
            ]
        },
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.7.3.19-stable"


def test_selection_is_by_version_not_by_creation_order(monkeypatch):
    """The endpoint orders by `created_at`: an LTS patch is published after 26.7.3."""
    _install_pager(
        monkeypatch,
        {
            1: [
                _release("v25.8.30.16-lts"),
                _release("v26.7.3.19-stable"),
                _release("v26.3.18.32-lts"),
            ]
        },
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.7.3.19-stable"


def test_first_page_without_an_older_release_raises(monkeypatch):
    requested = _install_pager(
        monkeypatch,
        {
            1: [_release("v26.8.1.1-stable"), _release("v26.9.1.1-stable")],
            2: [_release("v25.1.8.25-stable")],
        },
    )

    with pytest.raises(gprt.ReleaseNotFoundException):
        gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert requested == [1]


def test_none_server_version_resolves_the_newest_release(monkeypatch):
    """`download_last_release` passes None and expects the newest release."""
    _install_pager(
        monkeypatch,
        {
            1: [
                _release("v25.8.30.16-lts"),
                _release("v26.8.1.1-stable"),
                _release("v26.7.3.19-stable"),
            ]
        },
    )

    resolved = gprt.get_previous_release(None)

    assert str(resolved) == "v26.8.1.1-stable"
