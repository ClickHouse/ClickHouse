"""
Tests for `get_previous_release_tag`, the resolver that picks the baseline the
Upgrade check installs and starts before upgrading to the build under test.

These tests pin three properties:

  * the resolved baseline is the newest release on the first page strictly below
    the version under test that already has an uploaded package, so a release
    whose packages are still being published is skipped rather than failing the
    download later;
  * only the first page is consulted, and it must be a complete page, so a
    degraded response fails loudly instead of answering from an older page;
  * selection is by version, not by the position the endpoint returns entries in,
    which is roughly newest-created first and so not version order.

The page scope is sound because a complete page spans far more releases than are
published between two minor bumps, and it fails closed: the resolver raises rather
than answering when the page cannot show that it reaches past its own answer.

See https://github.com/ClickHouse/ClickHouse/pull/114376 (Upgrade check on
`e1cb0dee5e9` resolved `v25.1.4.53-stable` as the baseline for a 26.8.1.1 build).
"""

import logging
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
    from version_helper import get_version_from_string, get_version_from_tag
finally:
    sys.path.remove(_CI_DIR)


def _asset(tag):
    version = tag.lstrip("v").split("-", maxsplit=1)[0]
    name = f"clickhouse-common-static_{version}_amd64.deb"
    return [{"name": name, "state": "uploaded", "browser_download_url": "http://x"}]


def _release(tag, with_assets=True):
    return {"tag_name": tag, "assets": _asset(tag) if with_assets else []}


# Spelled out rather than read from the module so that every case below fails
# on its own subject rather than on a missing attribute.
_PAGE = 100


def _full_page(releases, filler="v99", with_assets=True):
    """Pad to a complete page; a short page 1 is rejected as degraded.

    `filler` picks whether the padding sorts above every version under test
    (the default, so it can never be selected) or below it.
    """
    padding = [
        _release(f"{filler}.1.1.{i}-stable", with_assets)
        for i in range(1, _PAGE - len(releases) + 1)
    ]
    return releases + padding


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

    def fake_get_gh_api(_url, **kwargs):
        page = kwargs.get("params", {}).get("page", 1)
        requested.append(page)
        return FakeResponse(pages.get(page, []))

    monkeypatch.setattr(gprt, "get_gh_api", fake_get_gh_api)
    return requested


def test_degraded_first_page_does_not_fall_through_to_a_stale_release(monkeypatch):
    """The reported regression: no usable candidate on page 1, stale one on page 3."""
    requested = _install_pager(
        monkeypatch,
        {
            1: _full_page(
                [
                    _release("v26.7.3.19-stable", with_assets=False),
                    _release("v26.6.2.160-stable", with_assets=False),
                ]
            ),
            3: _full_page([_release("v25.1.4.53-stable")]),
        },
    )

    with pytest.raises(gprt.ReleaseNotFoundException):
        gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert requested == [1]


def test_assetless_newest_below_is_skipped_for_the_next_newest_with_assets(
    monkeypatch, caplog
):
    """The release publish window: the newest release below has no package yet.

    The skip has to be visible in the job log: `main` configures logging at INFO,
    so a lower level leaves the substitution silent.
    """
    _install_pager(
        monkeypatch,
        {
            1: _full_page(
                [
                    _release("v26.7.3.19-stable", with_assets=False),
                    _release("v26.7.2.59-stable"),
                    # A page always carries releases older than the answer as well.
                    _release("v26.6.2.160-stable"),
                ]
            )
        },
    )

    with caplog.at_level(logging.INFO, logger=gprt.logger.name):
        resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.7.2.59-stable"
    skips = [
        r
        for r in caplog.records
        if r.levelno >= logging.WARNING and "26.7.3.19" in r.getMessage()
    ]
    assert skips, f"the skipped release was not reported: {caplog.text!r}"


def test_happy_path(monkeypatch):
    _install_pager(
        monkeypatch,
        {
            1: _full_page(
                [
                    _release("v26.8.1.1-stable"),
                    _release("v26.7.3.19-stable"),
                    _release("v26.6.2.160-stable"),
                ]
            )
        },
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.7.3.19-stable"


def test_selection_is_by_version_not_by_feed_position(monkeypatch):
    """An LTS patch for an older branch can be returned ahead of a newer release."""
    _install_pager(
        monkeypatch,
        {
            1: _full_page(
                [
                    _release("v25.8.30.16-lts"),
                    _release("v26.7.3.19-stable"),
                    _release("v26.3.18.32-lts"),
                ]
            )
        },
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.7.3.19-stable"


def test_first_page_without_an_older_release_raises(monkeypatch):
    requested = _install_pager(
        monkeypatch,
        {
            1: _full_page([_release("v26.8.1.1-stable"), _release("v26.9.1.1-stable")]),
            2: _full_page([_release("v25.1.8.25-stable")]),
        },
    )

    with pytest.raises(gprt.ReleaseNotFoundException):
        gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert requested == [1]


def test_truncated_first_page_raises(monkeypatch):
    """A short page 1 cannot be trusted to hold the newest release below."""
    requested = _install_pager(
        monkeypatch,
        {
            1: [
                _release("v25.1.4.53-stable"),
                _release("v24.8.14.39-lts"),
                _release("v24.3.18.7-lts"),
            ],
            2: _full_page([_release("v26.7.3.19-stable")]),
        },
    )

    with pytest.raises(gprt.ReleaseNotFoundException):
        gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert requested == [1]


def test_full_first_page_answers_without_reading_later_pages(monkeypatch):
    """A complete page 1 is authoritative even when page 2 holds a newer release."""
    requested = _install_pager(
        monkeypatch,
        {
            1: _full_page([_release("v26.6.2.160-stable")], filler="v20"),
            2: _full_page([_release("v26.7.3.19-stable")]),
        },
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == "v26.6.2.160-stable"
    assert requested == [1]


def _newest_below(page, server_version):
    """The answer the selection rule should give, computed from the page itself.

    Deliberately independent of the resolver, and taking the whole page rather
    than a hand-listed subset of it, so the padding cannot drift out of the
    expectation: this states the rule instead of repeating one recorded output.
    """
    version = get_version_from_string(server_version)
    tags = [r["tag_name"] for r in page]
    below = [t for t in tags if get_version_from_tag(t) < version]
    return max(below, key=get_version_from_tag)


def test_a_full_page_answers_across_several_minor_versions(monkeypatch):
    """Page 1 in the shape the real feed has: many minors, newest near the top.

    A complete page is authoritative because it spans far more releases than are
    published between two minor bumps, so the newest release below the build
    under test is on it. This pins the selection rule against that fixture with
    an independently computed expectation.
    """
    page = _full_page(
        [
            _release(t)
            for t in [
                "v26.8.1.1-stable",
                "v26.7.3.19-stable",
                "v26.7.2.59-stable",
                "v26.6.2.160-stable",
                "v26.5.4.72-stable",
                "v26.3.18.32-lts",
                "v25.8.30.16-lts",
            ]
        ],
        filler="v20",
    )
    _install_pager(monkeypatch, {1: page})

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == _newest_below(page, "26.8.1.1")


def test_a_full_page_entirely_older_than_the_build_still_answers_from_page_one(
    monkeypatch,
):
    """The boundary: every eligible entry is more than a minor behind.

    No entry is a near miss below the build under test, which is the shape a long
    release gap produces, and the padding sorts above it so the newest entry on
    the page is not the answer. The resolver must still answer from page 1, with
    the newest eligible entry rather than the first or last one it happens to see.
    """
    page = _full_page(
        [
            _release(t)
            for t in [
                "v26.3.18.32-lts",
                "v26.1.7.44-stable",
                "v25.8.30.16-lts",
                "v25.3.24.11-lts",
            ]
        ]
    )
    requested = _install_pager(
        monkeypatch,
        {1: page, 2: _full_page([_release("v26.7.3.19-stable")])},
    )

    resolved = gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert str(resolved) == _newest_below(page, "26.8.1.1")
    assert requested == [1]


def test_an_answer_at_the_pages_version_floor_raises(monkeypatch):
    """A complete page whose only eligible entry is its own oldest one.

    Nothing on the page sorts below the answer, so the page gives no evidence that
    it reaches past it and an even newer eligible release may sit off it. That is
    the one shape a complete page cannot vouch for, so it must raise rather than
    return a baseline it cannot justify.
    """
    page = _full_page([_release("v25.3.24.11-lts")])
    requested = _install_pager(
        monkeypatch,
        {1: page, 2: _full_page([_release("v26.7.3.19-stable")])},
    )

    with pytest.raises(gprt.ReleaseNotFoundException):
        gprt.get_previous_release(get_version_from_string("26.8.1.1"))

    assert requested == [1]


def test_none_server_version_resolves_the_newest_release(monkeypatch):
    """`download_last_release` passes None and expects the newest release."""
    _install_pager(
        monkeypatch,
        {
            1: _full_page(
                [
                    _release("v25.8.30.16-lts"),
                    _release("v26.8.1.1-stable"),
                    _release("v26.7.3.19-stable"),
                ],
                filler="v20",
            )
        },
    )

    resolved = gprt.get_previous_release(None)

    assert str(resolved) == "v26.8.1.1-stable"


def test_none_server_version_with_no_recognized_tag_raises(monkeypatch):
    """`download_last_release` must get a diagnostic, not a bare IndexError.

    A complete page whose tags all fail `TAG_REGEXP` leaves nothing to choose
    from, which is the only way the caller reaches an empty candidate list.
    """
    _install_pager(
        monkeypatch,
        {1: [{"tag_name": f"nightly-{i}", "assets": []} for i in range(_PAGE)]},
    )

    with pytest.raises(gprt.ReleaseNotFoundException):
        gprt.get_previous_release(None)


def test_find_previous_release_reports_not_found_for_no_releases():
    assert gprt.find_previous_release(None, []) == (False, None)


def _job_configs():
    """`JobConfigs` plus praktika's path helper, imported lazily.

    `ci/defs/job_configs.py` does `from praktika import ...`, so `ci/` itself has
    to be importable for `import praktika` to resolve to `ci/praktika`.
    """
    # pylint: disable=import-outside-toplevel
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    for path in (root, os.path.join(root, "ci")):
        if path not in sys.path:
            sys.path.insert(0, path)

    from ci.defs.job_configs import JobConfigs
    from ci.praktika.utils import Utils

    return JobConfigs, Utils


@pytest.mark.parametrize("job_attr", ["ci_tests", "upgrade_test_jobs"])
def test_the_resolver_is_in_its_consumers_cache_digests(job_attr):
    """Both jobs that depend on the resolver must digest it.

    `Digest.calc_job_digest` hashes the files `traverse_paths` yields for
    `include_paths`, and `hook_cache` reuses a cached success while the digest is
    unchanged. The resolver lives under `tests/ci`, which `./ci` does not cover,
    so without an explicit entry a change to it leaves the job cache-skippable:
    for `ci_tests` the cases above never run against it, and for the Upgrade
    check the production consumer is never exercised.
    """
    job_configs, utils = _job_configs()

    job = getattr(job_configs, job_attr)
    if isinstance(job, list):  # parametrize() yields a list of param sets
        job = job[0]
    digested = utils.traverse_paths(job.digest_config.include_paths, [])

    assert "./tests/ci/get_previous_release_tag.py" in digested, (
        f"The Upgrade check baseline resolver is not in {job.name}'s cache digest, "
        f"so that job can be skipped when it changes: "
        f"{job.digest_config.include_paths}"
    )


def test_pages_built_here_match_the_page_size_the_resolver_requests():
    """Guards the padding above against a change to the resolver's page size."""
    assert gprt.RELEASES_PER_PAGE == _PAGE
