"""
Tests for the GitHub API read retry policy.

The policy lives in `GH.api_get` (`ci/praktika/gh.py`);
`build_download_helper.get_gh_api` is a thin shim over it that adds the robot-token
failover and the `APIException` class its callers catch. The upgrade check resolves the
previous release through the shim during setup, so a GitHub API blip there wastes the
whole job without ever starting a ClickHouse binary.

Split accordingly: policy cases drive `GH.api_get`, failover and error-class cases drive
the shim.

  * statuses that are transient by definition (5xx, 429) and transport errors are retried
    over a window long enough to ride out a short outage, with a capped exponential
    backoff;
  * the backoff base is the caller-supplied `sleep`, so callers that ask for no sleep at
    all (`pr_info.RETRY_SLEEP = 0`) still get none;
  * every other 4xx keeps its existing behaviour, including the 403/404 auth failover and
    the 403 bodies the rate-limit predicate does not match.

`get_gh_api` is not the only unauthenticated GitHub HTTP call in `tests/ci`
(`ci_utils.py` and `github_helper.py` have their own); it is the one the release-tag
lookup and PR info resolution go through.
"""

import importlib.util
import os
import types

import pytest
import requests

# The shim inserts the repository root on `sys.path` and imports `ci.praktika.gh`, so
# these tests must patch the SAME module object. Importing `praktika.gh` instead would
# load a second, distinct module (praktika/__init__.py appends `ci/` to `sys.path`), and
# the monkeypatch would silently miss while the code hit the real network.
import ci.praktika.gh as gh_module
from ci.praktika.gh import GH

# Load the shim directly from its file so we do not have to put the whole `tests/ci`
# directory on `sys.path` for the entire pytest session (which would risk shadowing
# equally-named modules in other tests).
_BDH_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "tests", "ci", "build_download_helper.py"
)
_spec = importlib.util.spec_from_file_location("build_download_helper", _BDH_PATH)
bdh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bdh)

# GitHub's secondary rate limit and abuse detection return 403 with bodies that
# the `rate limit exceeded` predicate in `get_gh_api` does not match.
SECONDARY_RATELIMIT_BODY = (
    b"You have exceeded a secondary rate limit. Please wait a few minutes"
)
PRIMARY_RATELIMIT_BODY = b"API rate limit exceeded for 1.2.3.4."

URL = "https://api.github.com/repos/o/r/releases/tags/v1"


class FakeResponse:
    def __init__(self, status_code: int, content: bytes = b""):
        self.status_code = status_code
        # The rate-limit branch reads the private attribute.
        self._content = content
        self.ok = status_code < 400

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Error", response=self)

    def json(self):
        return {}


def _install_fake_transport(monkeypatch, statuses, body, transport_error, sleeps):
    """Patch `GH`'s requests/time so no test ever sleeps or reaches the network."""
    calls = {"count": 0, "requests": []}
    auth_seen = []

    def fake_get(url, **get_kwargs):
        index = calls["count"]
        calls["count"] += 1
        # A snapshot per attempt: the headers mapping is mutated in place by the failover,
        # so a reference would report every attempt as carrying the final header.
        calls["requests"].append(
            (url, {k: dict(v) if k == "headers" else v for k, v in get_kwargs.items()})
        )
        auth_seen.append("Authorization" in (get_kwargs.get("headers") or {}))
        if transport_error:
            raise transport_error("transport failure")
        status = statuses[index] if index < len(statuses) else statuses[-1]
        return FakeResponse(status, body)

    monkeypatch.setattr(gh_module.requests, "get", fake_get)
    monkeypatch.setattr(gh_module, "time", types.SimpleNamespace(sleep=sleeps.append))
    return calls, auth_seen


def _run_api_get(
    monkeypatch, statuses, *, body: bytes = b"", transport_error=None, **kwargs
):
    """Drive the real `GH.api_get` against scripted responses.

    Returns (attempts, sleeps, outcome). `statuses` is consumed one per attempt; the last
    entry repeats.
    """
    sleeps: list = []
    calls, auth_seen = _install_fake_transport(
        monkeypatch, statuses, body, transport_error, sleeps
    )
    try:
        GH.api_get(URL, **kwargs)
        outcome = "SUCCESS"
    except Exception as e:  # pylint: disable=broad-except
        outcome = type(e).__name__
    return calls["count"], sleeps, outcome, auth_seen


def _run_shim(
    monkeypatch,
    statuses,
    *,
    body: bytes = b"",
    token_preset: bool = False,
    transport_error=None,
    **kwargs,
):
    """Drive the real `get_gh_api` shim, which delegates the policy to `GH.api_get`."""
    sleeps: list = []
    calls, auth_seen = _install_fake_transport(
        monkeypatch, statuses, body, transport_error, sleeps
    )

    class FakeRobotToken:
        ROBOT_TOKEN = "preset-token" if token_preset else None

        @staticmethod
        def get_best_robot_token():
            return "fetched-token"

    monkeypatch.setattr(bdh, "grt", FakeRobotToken)

    try:
        bdh.get_gh_api(URL, **kwargs)
        outcome = "SUCCESS"
    except Exception as e:  # pylint: disable=broad-except
        outcome = type(e).__name__
    return calls["count"], sleeps, outcome, auth_seen


# --------------------------------------------------------------------------------------
# Policy cases: GH.api_get owns the backoff.
# --------------------------------------------------------------------------------------


# Row 1: the reported production failure (504), plus the boundaries of the `>= 500`
# contract. Each must span a window long enough to ride out a short GitHub outage, where
# before every status shared one fixed 4 x 3 s window.
@pytest.mark.parametrize("status", [500, 502, 503, 504, 599])
def test_persistent_5xx_uses_exponential_backoff(monkeypatch, status):
    attempts, sleeps, outcome, _ = _run_api_get(monkeypatch, [status])

    assert outcome == "RuntimeError"
    assert attempts == gh_module.API_GET_RETRIES_COUNT
    assert sleeps == [3, 6, 12, 24]
    assert sum(sleeps) == 45


# Row 2: must-not-regress. A blip that clears still succeeds on the attempt that gets a
# good response. Deliberately does not assert the sleep pattern, so that it keeps guarding
# the success path independently of the backoff formula.
def test_5xx_that_clears_succeeds(monkeypatch):
    attempts, _, outcome, _ = _run_api_get(monkeypatch, [504, 504, 200])

    assert outcome == "SUCCESS"
    assert attempts == 3


# Row 3: the growing sleep stays capped, so raising the retry count extends the total
# window instead of exploding a single sleep.
def test_backoff_is_capped(monkeypatch):
    _, sleeps, outcome, _ = _run_api_get(monkeypatch, [503], retries=10)

    assert outcome == "RuntimeError"
    assert sleeps == [3, 6, 12, 24, 48, 60, 60, 60, 60]
    assert max(sleeps) == gh_module.API_GET_RETRY_MAX_BACKOFF


# Row 4: `pr_info.RETRY_SLEEP = 0` feeds five call sites that deliberately ask for no
# sleep. The backoff multiplies the caller's `sleep`, so zero stays zero. This is the only
# row that distinguishes `sleep * 2**i` from a hardcoded base.
def test_zero_sleep_caller_is_unaffected(monkeypatch):
    attempts, sleeps, outcome, _ = _run_api_get(monkeypatch, [504], sleep=0)

    assert outcome == "RuntimeError"
    assert attempts == gh_module.API_GET_RETRIES_COUNT
    assert sleeps == [0, 0, 0, 0]
    assert sum(sleeps) == 0


# Row 5a: 429 is the only status that is both a 4xx and retryable, so it pins the
# retryable-status set independently of the `>= 500` threshold.
def test_bare_429_gets_backoff(monkeypatch):
    attempts, sleeps, outcome, _ = _run_api_get(monkeypatch, [429])

    assert outcome == "RuntimeError"
    assert sleeps == [3, 6, 12, 24]
    assert attempts == gh_module.API_GET_RETRIES_COUNT


# Row 8a-policy: a terminal 4xx keeps the flat sleep, and 499 pins the `>= 500` threshold
# from below. Without this row a predicate that always returns True would pass.
@pytest.mark.parametrize("status", [401, 422, 499])
def test_terminal_4xx_keeps_flat_sleep(monkeypatch, status):
    attempts, sleeps, outcome, _ = _run_api_get(monkeypatch, [status])

    assert outcome == "RuntimeError"
    assert attempts == gh_module.API_GET_RETRIES_COUNT
    assert sleeps == [3, 3, 3, 3]
    assert sum(sleeps) == 12


# Row 9: a transport error carries no status at all. A read timeout was attempt 1 of the
# reported production failure, and is not a `ConnectionError` subclass, so both classes
# are exercised.
@pytest.mark.parametrize(
    "transport_error",
    [requests.ConnectionError, requests.ReadTimeout],
)
def test_transport_error_gets_backoff(monkeypatch, transport_error):
    attempts, sleeps, outcome, _ = _run_api_get(
        monkeypatch, [200], transport_error=transport_error
    )

    assert outcome == "RuntimeError"
    assert attempts == gh_module.API_GET_RETRIES_COUNT
    assert sleeps == [3, 6, 12, 24]


# Row 10: the retry diagnostic is the only trace a CI operator gets, so pin the marker and
# the two fields the message promises. Reverting to a bare one-line message, or dropping
# the attempt or the delay, must redden this row. The stream is pinned in both directions:
# stdout carries caller data (row 10b), so echoing the diagnostic there too is a defect.
def test_retry_log_names_attempt_and_delay_on_stderr(monkeypatch, capsys):
    _run_api_get(monkeypatch, [504])

    captured = capsys.readouterr()
    lines = [l for l in captured.err.splitlines() if "WARNING" in l]
    assert lines
    assert "attempt 1 of 5" in lines[0]
    assert "retrying in 3 seconds" in lines[0]
    assert "WARNING" not in captured.out


# Row 10b: `tests/docker_scripts/upgrade_runner.sh:30` captures the stdout of
# `get_previous_release_tag.py` into a shell variable and splices it unquoted into
# `git clone --branch=$var`, so anything this read path writes to stdout turns a retried
# read into a corrupted clone.
def test_stdout_stays_parseable_across_a_retried_read(monkeypatch, capsys):
    _run_api_get(monkeypatch, [504, 504, 200])
    print("v25.8.1.100-lts")

    assert capsys.readouterr().out == "v25.8.1.100-lts\n"


# Row 10c: the budget reset skips the sleep, so an unbounded one is a tight loop against
# api.github.com. `on_http_error` is documented as an extension point, so the bound has to
# hold for a callback that always accepts, not only for the in-tree shim (rows 6, 7, 8c).
# The transport is capped so that removing the guard fails this row instead of hanging it:
# past the cap the request raises, which the loop treats as an ordinary error and exhausts.
def test_budget_resets_are_bounded(monkeypatch):
    sleeps: list = []
    calls, _ = _install_fake_transport(monkeypatch, [404], b"", None, sleeps)
    unguarded = gh_module.requests.get
    ceiling = gh_module.API_GET_RETRIES_COUNT * (
        1 + gh_module.API_GET_MAX_BUDGET_RESETS
    )

    def capped_get(url, **get_kwargs):
        if calls["count"] >= ceiling:
            raise AssertionError(
                f"budget resets are unbounded: over {ceiling} requests"
            )
        return unguarded(url, **get_kwargs)

    monkeypatch.setattr(gh_module.requests, "get", capped_get)

    with pytest.raises(RuntimeError, match="Unable to request data from GH API"):
        GH.api_get(URL, on_http_error=lambda _e: True)

    # Pinned as a literal, not derived from the constant: deriving both the expectation
    # and the ceiling from `API_GET_MAX_BUDGET_RESETS` lets a change to it stay green.
    assert gh_module.API_GET_MAX_BUDGET_RESETS == 1
    # One attempt is spent triggering the single allowed reset, then a fresh full budget.
    assert calls["count"] == 1 + gh_module.API_GET_RETRIES_COUNT
    assert calls["count"] <= ceiling
    # Only the attempts after the allowance is spent reach the sleep block.
    assert sleeps == [3, 3, 3, 3]


# Row 11: exhaustion raises. `get_output_with_retries` returns '' on exhaustion by
# default, which a caller cannot tell apart from an empty result; this path must not
# acquire that behaviour.
def test_api_get_raises_on_exhaustion_rather_than_returning_falsy(monkeypatch):
    sleeps: list = []
    _install_fake_transport(monkeypatch, [504], b"", None, sleeps)

    with pytest.raises(RuntimeError, match="Unable to request data from GH API"):
        GH.api_get(URL)


# --------------------------------------------------------------------------------------
# Shim cases: build_download_helper.get_gh_api owns the token failover and the error class.
# --------------------------------------------------------------------------------------


# Row 6: the 403 rate-limit failover still sets the auth header, still resets the attempt
# counter, and its retry budget is unchanged.
def test_403_ratelimit_failover_unchanged(monkeypatch):
    attempts, sleeps, outcome, auth_seen = _run_shim(
        monkeypatch, [403], body=PRIMARY_RATELIMIT_BODY
    )

    assert outcome == "APIException"
    assert auth_seen[0] is False and auth_seen[1] is True
    # The failover resets the attempt counter once, so the budget is one attempt longer
    # than the retry count (measured identical on the tree before this change).
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT + 1
    assert sleeps == [3, 3, 3, 3]


# Row 7: the 404 failover still fires exactly once, granting a fresh budget with a token.
# Byte-identical to the behaviour before this change.
def test_404_failover_unchanged(monkeypatch):
    attempts, sleeps, outcome, auth_seen = _run_shim(monkeypatch, [404])

    assert outcome == "APIException"
    assert auth_seen[0] is False and auth_seen[1] is True
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT + 1
    assert sleeps == [3, 3, 3, 3]


# Row 8b: no 4xx changes at all, and no failover for any. Without a preset token the
# failover guard is live, so `auth_seen` catches a widened 403 predicate granting these a
# token and a reset.
@pytest.mark.parametrize(
    "status,body",
    [
        (401, b""),
        (422, b""),
        (499, b""),
        (403, SECONDARY_RATELIMIT_BODY),
        (403, b"You have triggered an abuse detection mechanism."),
    ],
)
def test_4xx_budgets_unchanged_and_no_failover(monkeypatch, status, body):
    attempts, sleeps, outcome, auth_seen = _run_shim(monkeypatch, [status], body=body)

    assert outcome == "APIException"
    assert not any(auth_seen)
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [3, 3, 3, 3]
    assert sum(sleeps) == 12


# Row 8c: the 404 budget once the token is already set, so the failover cannot fire a
# second time. Distinct from row 7, which owns the case where it does fire.
def test_404_budget_unchanged_with_token(monkeypatch):
    attempts, sleeps, outcome, _ = _run_shim(monkeypatch, [404], token_preset=True)

    assert outcome == "APIException"
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [3, 3, 3, 3]
    assert sum(sleeps) == 12


# Row 5b: a bare 429 carries no `rate limit exceeded` body, so it gets the backoff but no
# failover. The policy half of this case lives on `GH.api_get`.
def test_bare_429_gets_no_failover(monkeypatch):
    attempts, sleeps, outcome, auth_seen = _run_shim(monkeypatch, [429])

    assert outcome == "APIException"
    assert not any(auth_seen)
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [3, 6, 12, 24]


# Row 12: the shim must translate praktika's RuntimeError into APIException, which is the
# error boundary its callers already handle. A leaked RuntimeError would cross it.
def test_shim_raises_apiexception_not_runtimeerror(monkeypatch):
    sleeps: list = []
    _install_fake_transport(monkeypatch, [504], b"", None, sleeps)
    monkeypatch.setattr(
        bdh, "grt", types.SimpleNamespace(ROBOT_TOKEN=None, get_best_robot_token=str)
    )

    with pytest.raises(bdh.APIException):
        bdh.get_gh_api(URL)

    # The class has to stay catchable, which is what `report.py` relies on.
    try:
        bdh.get_gh_api(URL)
    except bdh.APIException:
        caught = True
    assert caught


# Row 13: a success is returned unchanged through the shim, so callers still get the
# response object and not a truthy placeholder.
def test_shim_returns_response_on_success(monkeypatch):
    sleeps: list = []
    _install_fake_transport(monkeypatch, [200], b"", None, sleeps)
    monkeypatch.setattr(
        bdh, "grt", types.SimpleNamespace(ROBOT_TOKEN=None, get_best_robot_token=str)
    )

    response = bdh.get_gh_api(URL)

    assert response.status_code == 200
    assert sleeps == []


# Row 14: every caller option must survive the shim's delegation to `GH.api_get`, which is
# a second hop the pre-change single-function path did not have. The two live shapes are
# `get_previous_release_tag.py:69` (params + timeout) and `pr_info.py:427` (Accept header);
# dropping `**kwargs` from the delegation leaves both silently unsent.
def test_caller_request_options_survive_delegation(monkeypatch):
    sleeps: list = []
    calls, _ = _install_fake_transport(monkeypatch, [504, 504, 200], b"", None, sleeps)
    monkeypatch.setattr(
        bdh, "grt", types.SimpleNamespace(ROBOT_TOKEN=None, get_best_robot_token=str)
    )

    bdh.get_gh_api(
        URL,
        sleep=0,
        params={"page": 1, "per_page": 100},
        headers={"Accept": "application/vnd.github.v3.diff"},
        timeout=10,
    )

    # Every retried attempt, not just the first: an option cleared after a failure would
    # otherwise be invisible.
    assert calls["count"] == 3
    for url, sent in calls["requests"]:
        assert url == URL
        assert sent["params"] == {"page": 1, "per_page": 100}
        assert sent["headers"]["Accept"] == "application/vnd.github.v3.diff"
        assert sent["timeout"] == 10
    # The shim must forward the caller's `sleep`, which `pr_info.py` sets to 0.
    assert sleeps == [0, 0]


# Row 15: the failover must install a usable bearer, not merely an `Authorization` key.
# The other failover rows assert only that the key appeared, so a wrong or empty token
# would pass them.
def test_failover_sends_the_fetched_token(monkeypatch):
    sleeps: list = []
    calls, auth_seen = _install_fake_transport(monkeypatch, [404], b"", None, sleeps)
    monkeypatch.setattr(
        bdh,
        "grt",
        types.SimpleNamespace(
            ROBOT_TOKEN=None, get_best_robot_token=lambda: "fetched-token"
        ),
    )

    with pytest.raises(bdh.APIException):
        bdh.get_gh_api(
            URL,
            sleep=0,
            params={"page": 1},
            headers={"Accept": "application/vnd.github.v3.diff"},
            timeout=10,
        )

    assert auth_seen[0] is False and auth_seen[1] is True
    _, before = calls["requests"][0]
    assert "Authorization" not in (before.get("headers") or {})
    # The failover restarts the budget, so it is also where a caller option, the bearer, or
    # the caller's sleep can be lost. Assert them on every post-reset attempt, not just the
    # first authenticated one.
    assert calls["count"] == bdh.DOWNLOAD_RETRIES_COUNT + 1
    for _, sent in calls["requests"][1:]:
        assert sent["params"] == {"page": 1}
        assert sent["headers"]["Accept"] == "application/vnd.github.v3.diff"
        assert sent["headers"]["Authorization"] == "Bearer fetched-token"
        assert sent["timeout"] == 10
    assert sleeps == [0, 0, 0, 0]
