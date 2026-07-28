"""Regression tests: no CI job may fetch the obsolete chcache credential.

CI selects `sccache` for every build type (`ci/defs/defs.py`:
`COMPILER_CACHE = COMPILER_CACHE_LEGACY = "sccache"`), so nothing reads the four
`CH_*` variables that chcache consumes (`rust/chcache/src/config.rs`).

Invariants pinned below: no reference under `ci/`, no `CH_*` export from the
shared cache-setup helper, no second copy of the block in the jobs that build a
binary, and an unaffected sccache/ctcache setup.
"""

import importlib
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the
# path for `import praktika` to resolve to `ci/praktika`.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.build_clickhouse import setup_build_caches_env

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CI_ROOT = _REPO_ROOT / "ci"
_SELF = Path(__file__).resolve()

# The four variables chcache reads; it requires all of them, so a partial
# export would leave it half-configured rather than cleanly defaulted.
_CH_VARS = ("CH_HOSTNAME", "CH_USER", "CH_PASSWORD", "CH_USE_LOCAL_CACHE")


class _Info:
    """Minimal stand-in for `ci.praktika.info.Info`."""

    def __init__(self, pr_number=1, is_local_run=False):
        self.pr_number = pr_number
        self.is_local_run = is_local_run


def _ci_python_files():
    for path in sorted(_CI_ROOT.rglob("*.py")):
        # ci/tmp is the job scratch directory, not source.
        if "tmp" in path.relative_to(_CI_ROOT).parts:
            continue
        if path.resolve() == _SELF:
            continue
        yield path


def _grep_ci(needle):
    hits = []
    for path in _ci_python_files():
        text = path.read_text(encoding="utf-8", errors="replace")
        for lineno, line in enumerate(text.splitlines(), 1):
            if needle in line:
                hits.append(f"{path.relative_to(_REPO_ROOT)}:{lineno}: {line.strip()}")
    return hits


def _forbid_shell(monkeypatch):
    """Make any shell-out fail the way a transient SSM error does in CI.

    `ci/defs/defs.py` imports `praktika` as a top-level package, so
    `praktika.utils` and `ci.praktika.utils` are distinct module objects and
    `praktika.secret` resolves `Shell` through the former. Both are patched, so
    this guard cannot silently let a real `aws ssm get-parameter` through.
    """

    def explode(*args, **kwargs):
        raise RuntimeError("command failed with, exit_code 255")

    patched = 0
    for module_name in ("praktika.utils", "ci.praktika.utils"):
        module = importlib.import_module(module_name)
        monkeypatch.setattr(module.Shell, "get_output", staticmethod(explode))
        patched += 1
    assert patched == 2, "both praktika aliases must be patched"


def _clear_ch_vars(monkeypatch):
    for name in _CH_VARS:
        monkeypatch.delenv(name, raising=False)


def test_no_chcache_credential_reference_under_ci():
    """No `ci/` module may reference the chcache secret or its password variable.

    This is the guard that stops a third copy of the block from reappearing.
    """
    assert _grep_ci("chcache_secret") == []
    assert _grep_ci("CH_PASSWORD") == []


def test_setup_build_caches_env_exports_no_ch_vars(monkeypatch):
    """The shared cache setup must export none of the four chcache variables."""
    _clear_ch_vars(monkeypatch)
    _forbid_shell(monkeypatch)

    setup_build_caches_env(_Info(pr_number=1, is_local_run=False))

    assert [name for name in _CH_VARS if name in os.environ] == []


def test_sccache_configuration_is_untouched(monkeypatch):
    """Removing the chcache block must not disturb the sccache setup."""
    _forbid_shell(monkeypatch)
    for name in (
        "SCCACHE_DIR",
        "SCCACHE_BUCKET",
        "SCCACHE_S3_KEY_PREFIX",
        "SCCACHE_S3_READ_ONLY",
        "SCCACHE_S3_NO_CREDENTIALS",
        "SCCACHE_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "CTCACHE_DIR",
    ):
        monkeypatch.delenv(name, raising=False)

    setup_build_caches_env(_Info(pr_number=1, is_local_run=False))
    assert os.environ["SCCACHE_DIR"]
    assert os.environ["SCCACHE_BUCKET"]
    assert os.environ["SCCACHE_S3_KEY_PREFIX"] == "ccache/sccache"
    # A PR build must not write to the shared bucket.
    assert os.environ["SCCACHE_S3_READ_ONLY"] == "true"
    # The clang-tidy cache setup shares the non-local branch.
    assert os.environ["CTCACHE_DIR"]

    setup_build_caches_env(_Info(pr_number=1, is_local_run=True))
    assert os.environ["SCCACHE_S3_NO_CREDENTIALS"] == "true"


def test_no_second_copy_in_jobs_that_build_a_binary():
    """The bugfix-validation and fast-test jobs must not keep their own copy."""
    validator = (_CI_ROOT / "jobs" / "unit_tests_bugfix_validation_job.py").read_text()
    # It must reach the cache configuration only through the shared helper.
    assert "CH_PASSWORD" not in validator
    assert validator.count("setup_build_caches_env") == 2  # import + call

    fast_test = (_CI_ROOT / "jobs" / "fast_test.py").read_text()
    assert "CH_PASSWORD" not in fast_test


def test_modules_import_and_secrets_list_has_no_chcache_password():
    """Deleting the imports must not leave a dangling name behind."""
    import ci.defs.defs as defs
    import ci.jobs.fast_test  # noqa: F401  (import must succeed)

    names = []
    for secret in defs.SECRETS:
        names.extend(secret.name if isinstance(secret.name, list) else [secret.name])
    assert "chcache_password" not in names
