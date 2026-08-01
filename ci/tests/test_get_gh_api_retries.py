"""
Tests for `build_download_helper.get_gh_api`.

`get_gh_api` is the single chokepoint for every GitHub API call in `tests/ci`.
The upgrade check resolves the previous release through it during setup, so a
GitHub API blip there wastes the whole job without ever starting a ClickHouse
binary. These tests pin the retry policy:

  * statuses that are transient by definition (5xx, 429) and transport errors
    are retried over a window long enough to ride out a short outage, with a
    capped exponential backoff;
  * the backoff base is the caller-supplied `sleep`, so callers that ask for no
    sleep at all (`pr_info.RETRY_SLEEP = 0`) still get none;
  * every 4xx keeps its existing behaviour, including the 403/404 auth failover
    and the 403 bodies the rate-limit predicate does not match.
"""

import importlib.util
import os
import types

import pytest
import requests

# Load the module directly from its file so we do not have to put the whole
# `tests/ci` directory on `sys.path` for the entire pytest session (which would
# risk shadowing equally-named modules in other tests).
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


def _run(
    monkeypatch,
    statuses,
    *,
    body: bytes = b"",
    token_preset: bool = False,
    transport_error=None,
    **kwargs,
):
    """Drive the real `get_gh_api` against scripted responses.

    Returns (attempts, sleeps, outcome). `statuses` is consumed one per attempt;
    the last entry repeats. Never sleeps for real: the whole suite must stay fast.
    """
    sleeps = []
    calls = {"count": 0}
    auth_seen = []

    def fake_get(_url, **get_kwargs):
        index = calls["count"]
        calls["count"] += 1
        auth_seen.append("Authorization" in (get_kwargs.get("headers") or {}))
        if transport_error:
            raise transport_error("transport failure")
        status = statuses[index] if index < len(statuses) else statuses[-1]
        return FakeResponse(status, body)

    monkeypatch.setattr(bdh.requests, "get", fake_get)
    monkeypatch.setattr(bdh, "time", types.SimpleNamespace(sleep=sleeps.append))

    class FakeRobotToken:
        ROBOT_TOKEN = "preset-token" if token_preset else None

        @staticmethod
        def get_best_robot_token():
            return "fetched-token"

    monkeypatch.setattr(bdh, "grt", FakeRobotToken)

    try:
        bdh.get_gh_api("https://api.github.com/repos/o/r/releases/tags/v1", **kwargs)
        outcome = "SUCCESS"
    except Exception as e:  # pylint: disable=broad-except
        outcome = type(e).__name__

    return calls["count"], sleeps, outcome, auth_seen


# Row 1: the reported production failure (504), plus the boundaries of the
# `>= 500` contract. Each must span a window long enough to ride out a short
# GitHub outage, where before every status shared one fixed 4 x 3 s window.
@pytest.mark.parametrize("status", [500, 502, 503, 504, 599])
def test_persistent_5xx_uses_exponential_backoff(monkeypatch, status):
    attempts, sleeps, outcome, _ = _run(monkeypatch, [status])

    assert outcome == "APIException"
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [3, 6, 12, 24]
    assert sum(sleeps) == 45


# Row 2: must-not-regress. A blip that clears still succeeds on the attempt that
# gets a good response. Deliberately does not assert the sleep pattern, so that it
# keeps guarding the success path independently of the backoff formula.
def test_5xx_that_clears_succeeds(monkeypatch):
    attempts, _, outcome, _ = _run(monkeypatch, [504, 504, 200])

    assert outcome == "SUCCESS"
    assert attempts == 3


# Row 3: the growing sleep stays capped, so raising the retry count extends the
# total window instead of exploding a single sleep.
def test_backoff_is_capped(monkeypatch):
    _, sleeps, outcome, _ = _run(monkeypatch, [503], retries=10)

    assert outcome == "APIException"
    assert sleeps == [3, 6, 12, 24, 48, 60, 60, 60, 60]
    assert max(sleeps) == bdh.DOWNLOAD_RETRY_MAX_BACKOFF


# Row 4: `pr_info.RETRY_SLEEP = 0` feeds five call sites that deliberately ask
# for no sleep. The backoff multiplies the caller's `sleep`, so zero stays zero.
# This is the only row that distinguishes `sleep * 2**i` from a hardcoded base.
def test_zero_sleep_caller_is_unaffected(monkeypatch):
    attempts, sleeps, outcome, _ = _run(monkeypatch, [504], sleep=0)

    assert outcome == "APIException"
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [0, 0, 0, 0]
    assert sum(sleeps) == 0


# Row 5: a bare 429 carries no `rate limit exceeded` body, so it gets neither the
# auth failover nor (before this) any backoff growth.
def test_bare_429_gets_backoff(monkeypatch):
    _, sleeps, outcome, _ = _run(monkeypatch, [429])

    assert outcome == "APIException"
    assert sleeps == [3, 6, 12, 24]


# Row 6: the 403 rate-limit failover still sets the auth header, still resets the
# attempt counter, and its retry budget is unchanged.
def test_403_ratelimit_failover_unchanged(monkeypatch):
    attempts, sleeps, outcome, auth_seen = _run(
        monkeypatch, [403], body=PRIMARY_RATELIMIT_BODY
    )

    assert outcome == "APIException"
    assert auth_seen[0] is False and auth_seen[1] is True
    # The failover resets the attempt counter once, so the budget is one attempt
    # longer than the retry count (measured identical on the tree before this change).
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT + 1
    assert sleeps == [3, 3, 3, 3]


# Row 7: the 404 failover still fires exactly once, granting a fresh budget with
# a token. Byte-identical to the behaviour before this change.
def test_404_failover_unchanged(monkeypatch):
    attempts, sleeps, outcome, auth_seen = _run(monkeypatch, [404])

    assert outcome == "APIException"
    assert auth_seen[0] is False and auth_seen[1] is True
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT + 1
    assert sleeps == [3, 3, 3, 3]


# Row 8a: no 4xx changes at all, and no failover for any of them. Without a preset
# token the failover guard is not short-circuited, so `auth_seen` observes whether
# the 403 predicate stayed narrow: widening it would grant these a token and a reset.
@pytest.mark.parametrize(
    "status,body",
    [
        (401, b""),
        (422, b""),
        (403, SECONDARY_RATELIMIT_BODY),
        (403, b"You have triggered an abuse detection mechanism."),
    ],
)
def test_4xx_budgets_unchanged_and_no_failover(monkeypatch, status, body):
    attempts, sleeps, outcome, auth_seen = _run(monkeypatch, [status], body=body)

    assert outcome == "APIException"
    assert not any(auth_seen)
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [3, 3, 3, 3]
    assert sum(sleeps) == 12


# Row 8b: the 404 budget once the token is already set, so the failover cannot fire
# a second time. Distinct from row 7, which owns the case where it does fire.
def test_404_budget_unchanged_with_token(monkeypatch):
    attempts, sleeps, outcome, _ = _run(monkeypatch, [404], token_preset=True)

    assert outcome == "APIException"
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [3, 3, 3, 3]
    assert sum(sleeps) == 12


# Row 9: a transport error carries no status at all. A read timeout was attempt 1
# of the reported production failure, and is not a `ConnectionError` subclass, so
# both classes are exercised.
@pytest.mark.parametrize(
    "transport_error",
    [requests.ConnectionError, requests.ReadTimeout],
)
def test_transport_error_gets_backoff(monkeypatch, transport_error):
    attempts, sleeps, outcome, _ = _run(
        monkeypatch, [200], transport_error=transport_error
    )

    assert outcome == "APIException"
    assert attempts == bdh.DOWNLOAD_RETRIES_COUNT
    assert sleeps == [3, 6, 12, 24]
