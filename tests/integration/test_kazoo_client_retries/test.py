#!/usr/bin/env python3
"""
Regression tests for `KazooClientWithImplicitRetries` (`helpers/kazoo_client.py`).

The wrapper retries kazoo operations to absorb transient CI flakiness. `create`
is special: it is not naturally idempotent under retry. If a previous attempt
committed on the server but the response was lost (transient `ConnectionLoss`
during, e.g., cluster reconfiguration), the retry then sees `NodeExistsError`
even though the create logically succeeded.

This file pins the wrapper's contract with mocks so the heavyweight Keeper
fixtures are not needed.
"""

from unittest.mock import patch

import pytest
from kazoo.client import KazooClient
from kazoo.exceptions import ConnectionLoss, NodeExistsError

from helpers.kazoo_client import KazooClientWithImplicitRetries


def _make_client():
    # `KazooClient.__init__` accepts `hosts` and does not connect until `start`.
    return KazooClientWithImplicitRetries(hosts="ignored:9181")


def test_first_attempt_node_exists_still_raises():
    """First-attempt `NodeExistsError` must remain a hard error.

    Tests that rely on `pytest.raises(NodeExistsError)` to detect duplicate
    creates (e.g. `test_keeper_session::test_create2_errors_existing_and_missing_parent`)
    depend on this contract.
    """
    get_calls = []

    def fake_get(_self, path):
        get_calls.append(path)
        return (b"value-on-disk", None)

    with patch.object(KazooClient, "create", side_effect=NodeExistsError), patch.object(
        KazooClient, "get", fake_get
    ):
        with pytest.raises(NodeExistsError):
            _make_client().create("/foo", b"value-on-disk")
    # The wrapper must not consult `get` on the first attempt.
    assert get_calls == []


def test_retry_swallows_node_exists_when_data_matches():
    """First attempt `ConnectionLoss`, retry sees `NodeExistsError`, `get` matches.

    Simulates the exact race observed in
    `test_keeper_reconfig_replace_leader_in_one_command`:
      1. `create(/foo, b"x")` reaches the server, but the response is lost.
      2. `KazooRetry` catches `ConnectionLoss` and retries.
      3. The retry sees `NodeExistsError`.
      4. The wrapper reads `/foo`. Value is `b"x"`.
      5. The wrapper returns `/foo` as if the original create succeeded.
    """
    call_count = [0]

    def fake_create(_self, *_args, **_kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ConnectionLoss
        raise NodeExistsError

    with patch.object(KazooClient, "create", fake_create), patch.object(
        KazooClient, "get", return_value=(b"x", None)
    ):
        assert _make_client().create("/foo", b"x") == "/foo"
    assert call_count[0] == 2


def test_retry_raises_when_data_differs():
    """First attempt `ConnectionLoss`, retry `NodeExistsError`, existing data differs.

    Must not silently treat as success: the existing value was not produced by
    our attempt, so the caller's intent was not satisfied.
    """
    call_count = [0]

    def fake_create(_self, *_args, **_kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ConnectionLoss
        raise NodeExistsError

    with patch.object(KazooClient, "create", fake_create), patch.object(
        KazooClient, "get", return_value=(b"different", None)
    ):
        with pytest.raises(NodeExistsError):
            _make_client().create("/foo", b"new")
    assert call_count[0] == 2


def test_retry_re_raises_when_get_itself_fails():
    """If verification cannot complete, re-raise the original `NodeExistsError`."""
    call_count = [0]

    def fake_create(_self, *_args, **_kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ConnectionLoss
        raise NodeExistsError

    with patch.object(KazooClient, "create", fake_create), patch.object(
        KazooClient, "get", side_effect=RuntimeError("get failed")
    ):
        with pytest.raises(NodeExistsError):
            _make_client().create("/foo", b"x")


def test_include_data_returns_tuple():
    """When `include_data=True`, kazoo returns `(path, stat)`. Preserve that."""
    call_count = [0]

    def fake_create(_self, *_args, **_kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ConnectionLoss
        raise NodeExistsError

    with patch.object(KazooClient, "create", fake_create), patch.object(
        KazooClient, "get", return_value=(b"x", "fake-stat")
    ):
        assert _make_client().create("/foo", b"x", include_data=True) == (
            "/foo",
            "fake-stat",
        )


def test_first_attempt_success_unchanged():
    """When `create` succeeds on the first attempt, the wrapper passes through unchanged."""
    with patch.object(
        KazooClient, "create", return_value="/foo"
    ), patch.object(KazooClient, "get", side_effect=AssertionError("get must not be called")):
        assert _make_client().create("/foo", b"x") == "/foo"


def test_kwargs_positional_path_value():
    """The wrapper extracts `path` and `value` from kwargs for the existing-data check."""
    call_count = [0]

    def fake_create(_self, *_args, **_kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise ConnectionLoss
        raise NodeExistsError

    with patch.object(KazooClient, "create", fake_create), patch.object(
        KazooClient, "get", return_value=(b"y", None)
    ):
        assert _make_client().create(path="/bar", value=b"y") == "/bar"
