"""
Regression test for `no-parallel-replicas` / `no-async-insert` being inert against an
externally injected `--client-option`.

`ci/jobs/scripts/stress/stress.py` appends `enable_parallel_replicas=1` (and
`async_insert=1`) to `--client-option` for a random ~20% of stress workers, without
looking at test tags. `clickhouse-test` places `--client-option` values AFTER the
randomized settings on purpose, so with `--allow_repeated_settings` (last occurrence
wins) nothing downstream could turn the feature back off, and the tag's only other
enforcement is a skip gated on `--no-parallel-replicas`, which the stress job never
passes. Every tagged test therefore ran with the feature on in those workers; test
`04241_parameterized_view_used_twice` aborts the server that way
("Coordination mode mismatch for stream ...").

`TestCase.add_effective_settings` now appends a tag-derived feature-off override last.
This test pins that contract: present and last for a tagged test, absent for an
untagged one, in both the returned option string (used by the `.sql` command pattern)
and `CLICKHOUSE_CLIENT_OPT` (used by `.sh` tests through `shell_config.sh`).
"""

import os
import runpy
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")

# Load clickhouse-test without running __main__. runpy.run_path handles the missing
# .py extension.
_ct = runpy.run_path(_CLICKHOUSE_TEST)

TestCase = _ct["TestCase"]

# What stress.py injects, in the order it builds it.
_INJECTED_PARALLEL_REPLICAS = (
    " --enable_parallel_replicas=1 --max_parallel_replicas=3"
    " --cluster_for_parallel_replicas='parallel_replicas'"
    " --parallel_replicas_for_non_replicated_merge_tree=1"
)
_INJECTED_ASYNC_INSERT = " --async_insert=1"


def _make_case(tags, injected, effective_settings=None):
    # add_effective_settings only reads these attributes; bypass the heavy __init__,
    # which needs a full args namespace and an on-disk test file. `--client-option`
    # values reach it through base_client_options (snapshotted from
    # CLICKHOUSE_CLIENT_OPT, which main() populates before any test runs) as well as
    # through the client_options argument.
    case = TestCase.__new__(TestCase)
    case.tags = set(tags)
    case.effective_settings = dict(effective_settings or {})
    case.effective_merge_tree_settings = {}
    case.base_url_params = ""
    case.base_client_options = injected
    return case


def _apply(tags, client_options, effective_settings=None):
    saved = {
        name: os.environ.get(name)
        for name in ("CLICKHOUSE_CLIENT_OPT", "CLICKHOUSE_URL_PARAMS")
    }
    os.environ["CLICKHOUSE_CLIENT_OPT"] = client_options
    os.environ.pop("CLICKHOUSE_URL_PARAMS", None)
    try:
        case = _make_case(tags, client_options, effective_settings)
        returned = case.add_effective_settings(client_options)
        return returned, os.environ.get("CLICKHOUSE_CLIENT_OPT", "")
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


def _assert_override_wins(text, setting, injected_value):
    """The feature-off override must exist and follow every injected occurrence."""
    off = f"--{setting} 0"
    assert off in text, text
    assert text.rindex(off) > text.rindex(f"--{setting}={injected_value}"), text


def test_tagged_test_gets_parallel_replicas_disabled_last():
    for effective_settings in ({}, {"max_threads": 4}):
        returned, env = _apply(
            ["no-parallel-replicas"],
            _INJECTED_PARALLEL_REPLICAS,
            effective_settings,
        )
        for text in (returned, env):
            _assert_override_wins(text, "enable_parallel_replicas", 1)
            assert "--allow_repeated_settings" in text, text
            # Only the alias is pinned; the other injected settings are inert once the
            # feature is off, and unsetting them would change unrelated behaviour.
            assert "--max_parallel_replicas 0" not in text, text
        assert "--async_insert 0" not in returned, returned


def test_tagged_test_gets_async_insert_disabled_last():
    returned, env = _apply(["no-async-insert"], _INJECTED_ASYNC_INSERT)
    for text in (returned, env):
        _assert_override_wins(text, "async_insert", 1)
    assert "--enable_parallel_replicas 0" not in returned, returned


def test_both_tags_are_handled_together():
    returned, env = _apply(
        ["no-parallel-replicas", "no-async-insert"],
        _INJECTED_PARALLEL_REPLICAS + _INJECTED_ASYNC_INSERT,
    )
    for text in (returned, env):
        _assert_override_wins(text, "enable_parallel_replicas", 1)
        _assert_override_wins(text, "async_insert", 1)


def test_untagged_test_is_not_overridden():
    returned, env = _apply(["long", "no-fasttest"], _INJECTED_PARALLEL_REPLICAS)
    for text in (returned, env):
        assert "--enable_parallel_replicas 0" not in text, text
        assert "--async_insert 0" not in text, text
        # The injection itself is untouched for a test that does not opt out.
        assert "--enable_parallel_replicas=1" in text, text


def test_no_tags_at_all_is_not_overridden():
    # `tags` is None for a test file without a Tags line.
    for tags in (set(), None):
        case = _make_case([], _INJECTED_PARALLEL_REPLICAS)
        case.tags = tags
        assert case.feature_tag_overrides() == ""
