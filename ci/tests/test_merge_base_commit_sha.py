"""Tests for the `merge_base_commit_sha` store in
`ci.jobs.scripts.workflow_hooks.store_data`.

`Docs check (Mintlify)` interpolates this key into a `git fetch` refspec and its guard
rejects only an EMPTY value, and `gh api` prints its error body on stdout, so a non-2xx
response is a non-empty non-revision unless the store rejects it.

These tests pin the two properties that make the stored value a revision: a failing read is
retried and then raises rather than returning its body, and a successful read is a full
commit id or nothing is stored. They also pin that the pre-hook keeps going, since the keys
written after this block have their own consumers.

The real helper runs against a fake `gh` on `PATH` so the actual subprocess and retry code
is exercised; only `time.sleep` is neutralized.
"""

import ast
import os
import stat
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `store_data` imports `praktika` by bare name, so put `ci/` on the path too.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.praktika.gh as gh_mod
from ci.jobs.scripts.workflow_hooks.store_data import _is_commit_sha
from ci.praktika.settings import Settings

_KEY = "merge_base_commit_sha"
_SHA = "d8392cfb8badedec0cb9497ed9fb2f39a5c21431"
_HEAD = "fd6e0e4b4bea1643d2650e3e712d84d419e9356c"
_HOOK_PATH = os.path.join(
    os.path.dirname(__file__), "../jobs/scripts/workflow_hooks/store_data.py"
)

# The two bodies observed in CI, verbatim. Both are non-empty, which is why the reader's
# empty-only guard let them past.
_BODY_404 = (
    '{"message":"Not Found","documentation_url":'
    '"https://docs.github.com/rest/commits/commits#compare-two-commits","status":"404"}'
)
_BODY_500 = (
    '{"message":"the diff for this comparison is temporarily unavailable",'
    '"status":"500"}'
)


@pytest.fixture
def fake_gh(tmp_path, monkeypatch):
    """Install a fake `gh` on PATH; returns an installer, an invocation counter and the argv.

    The counter is what distinguishes "retried" from "gave up": both end with the key absent,
    so asserting only the stored data cannot tell them apart. The recorded argv is what pins
    the command actually issued, since a fake that ignores its arguments would keep a fetch
    aimed at the wrong revision green.
    """
    counter = tmp_path / "invocations"
    argv_log = tmp_path / "argv"

    def install(body, exit_code):
        script = tmp_path / "gh"
        script.write_text(
            "#!/bin/bash\n"
            f'echo x >> "{counter}"\n'
            f'printf "%s\\n" "$*" >> "{argv_log}"\n'
            f"{body}\n"
            f"exit {exit_code}\n"
        )
        script.chmod(script.stat().st_mode | stat.S_IEXEC)

    def emit(text):
        """A fake-`gh` body writing `text` to stdout, the stream `gh` puts its body on.

        The text is written to a file and `cat`ed rather than interpolated into the script:
        embedding it would render a newline as the two characters backslash and n, so the
        multi-line case would silently exercise a single-line input instead.
        """
        payload = tmp_path / f"body-{abs(hash(text))}"
        payload.write_text(f"{text}\n" if text else "")
        return f'cat "{payload}"'

    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ['PATH']}")
    # Otherwise every failing case pays the real 4+8+16s backoff ladder.
    monkeypatch.setattr(gh_mod.time, "sleep", lambda _delay: None)

    def invocations():
        return len(counter.read_text().splitlines()) if counter.exists() else 0

    def argv_all():
        return argv_log.read_text().splitlines() if argv_log.exists() else []

    install.invocations = invocations
    install.argv_all = argv_all
    install.argv = lambda: argv_all()[-1] if argv_all() else ""
    install.emit = emit
    return install


class _FakeInfo:
    """Stand-in for `praktika.info.Info`, exposing only what this block reads and writes."""

    def __init__(self, pr_number=12345, sha=_HEAD):
        self.pr_number = pr_number
        self.sha = sha
        self.kv = {}

    def store_kv_data(self, key, value):
        self.kv[key] = value


def _merge_base_statement():
    """The production `if info.pr_number > 0:` block that stores the key.

    The module is read, not imported: importing it would run the whole hook, which fetches
    changed files, a build digest and the master commit list. The statement is then EXECUTED
    rather than pattern-matched, so a guard amended with `and False` cannot stay green.
    """
    with open(_HOOK_PATH, "r", encoding="utf-8") as f:
        tree = ast.parse(f.read())
    main_blocks = [
        node
        for node in tree.body
        if isinstance(node, ast.If) and "__main__" in ast.unparse(node.test)
    ]
    assert len(main_blocks) == 1, "expected exactly one `if __name__ == '__main__':` block"

    def stores_the_key(node):
        return any(
            isinstance(inner, ast.Constant) and inner.value == _KEY
            for inner in ast.walk(node)
        )

    guards = [n for n in main_blocks[0].body if isinstance(n, ast.If) and stores_the_key(n)]
    assert len(guards) == 1, f"expected exactly one statement storing {_KEY}, got {len(guards)}"
    index = main_blocks[0].body.index(guards[0])
    return guards[0], main_blocks[0].body[index + 1 :]


def _store(info=None, and_then=0):
    """Run the production block against `info`; returns the info it wrote into.

    `and_then` runs that many of the statements that FOLLOW it in `__main__`, so a cell can
    assert against a key some later production code stores rather than a stand-in for one.
    """
    info = info if info is not None else _FakeInfo()
    statement, rest = _merge_base_statement()
    module = sys.modules["ci.jobs.scripts.workflow_hooks.store_data"]
    namespace = dict(vars(module))
    namespace["info"] = info
    namespace["changed_files"] = ["tests/integration/test_foo/test.py", "src/Core/Defines.h"]
    exec(  # noqa: S102 - the statements come from this repo's own hook
        compile(
            ast.Module(body=[statement] + rest[:and_then], type_ignores=[]),
            _HOOK_PATH,
            "exec",
        ),
        namespace,
    )
    return info


# --- an error body is never stored as a revision -----------------------------


@pytest.mark.parametrize("body", [_BODY_404, _BODY_500], ids=["404", "500"])
def test_an_error_body_is_not_stored(fake_gh, body):
    """The two recorded classes, one behaviour.

    Assert the key is ABSENT rather than merely different from the body: an inequality
    assertion is satisfied by storing some other non-revision, which is the same defect.
    """
    fake_gh(fake_gh.emit(body), 1)
    info = _store()
    assert _KEY not in info.kv


def test_a_failed_read_still_lets_the_later_keys_be_stored(fake_gh):
    """The risk the switch to a raising read introduces: this is a pre-hook, and the keys
    written after this block have their own consumers.

    Assert a key the NEXT production statement stores, not merely that nothing raised: an
    exception escaping to `__main__` would abort the hook with this block's own key absent
    either way, so the two are indistinguishable from inside this block alone.
    """
    fake_gh(fake_gh.emit(_BODY_404), 1)
    info = _store(and_then=1)
    assert _KEY not in info.kv
    assert info.kv["changed_integration_tests"] == ["tests/integration/test_foo/test.py"]


def test_a_transient_failure_is_retried_and_then_stored(fake_gh, tmp_path):
    """A transient failure must be retried rather than surfaced as an absent key.

    The invocation count is the only thing that separates a retry from a give-up here; a
    stored-value assertion alone passes for a read that succeeded first time.
    """
    marker = tmp_path / "attempted"
    fake_gh(
        f'if [ ! -f "{marker}" ]; then touch "{marker}"; '
        'echo "gh: Server Error (HTTP 502)" >&2; exit 1; fi\n'
        f"{fake_gh.emit(_SHA)}",
        0,
    )
    info = _store()
    assert info.kv[_KEY] == _SHA
    assert fake_gh.invocations() == 2


def test_a_persistent_server_error_exhausts_the_retries(fake_gh):
    fake_gh('echo "gh: Server Error (HTTP 502)" >&2', 1)
    info = _store()
    assert _KEY not in info.kv
    assert fake_gh.invocations() == Settings.MAX_RETRIES_GH


def test_a_non_retryable_class_breaks_out_early(fake_gh):
    """Retrying a bad token cannot help. Pins that the early break was not defeated."""
    fake_gh('echo "gh: Bad credentials (HTTP 401)" >&2', 1)
    info = _store()
    assert _KEY not in info.kv
    assert fake_gh.invocations() == 1


# --- exit code 0 is not by itself a revision ---------------------------------


@pytest.mark.parametrize(
    "output,case",
    [
        # `gh api -q` on an absent field: rc=0, empty. Measured.
        ("", "absent field"),
        # `-q .merge_base_commit.commit.message` instead of `.sha`: rc=0, prose. Measured.
        (
            "Merge pull request #114815 from vitlibar/timeseries-range-fixes\n\n"
            "Fix handling of negative timestamps",
            "commit message",
        ),
        ("null", "json null rendered by jq"),
    ],
)
def test_a_successful_read_of_a_non_revision_is_not_stored(fake_gh, output, case):
    """The class a raising read cannot cover, and so the reason the value is validated:
    `gh` exits 0 and the body is simply not a commit id."""
    fake_gh(fake_gh.emit(output), 0)
    # The fake really does put `output` on stdout, newlines included: a fake that rendered a
    # newline as two characters would leave the multi-line case testing a single-line input.
    assert subprocess.run(["gh"], capture_output=True, text=True).stdout == (
        f"{output}\n" if output else ""
    )
    info = _store()
    assert _KEY not in info.kv, case


def test_a_commit_id_is_stored_verbatim_and_unretried(fake_gh):
    fake_gh(fake_gh.emit(_SHA), 0)
    info = _store()
    assert info.kv[_KEY] == _SHA
    # A needless retry on the happy path would triple every PR's pre-hook latency.
    assert fake_gh.invocations() == 1


def test_the_issued_command_compares_master_against_the_head_revision(fake_gh):
    """Assert the recorded argv: the read could name the wrong revision or the wrong field
    and every value-only assertion above would still pass."""
    fake_gh(fake_gh.emit(_SHA), 0)
    _store(_FakeInfo(sha=_HEAD))
    argv = fake_gh.argv()
    assert f"repos/ClickHouse/ClickHouse/compare/master...{_HEAD}" in argv
    assert ".merge_base_commit.sha" in argv


def test_nothing_is_read_when_there_is_no_pr(fake_gh):
    """The guard stays load-bearing: a master run has no merge base to store."""
    fake_gh(fake_gh.emit(_SHA), 0)
    info = _store(_FakeInfo(pr_number=0))
    assert _KEY not in info.kv
    assert fake_gh.invocations() == 0


# --- the reader's own guard, once nothing invalid is stored -------------------


def test_the_reader_makes_no_fetch_when_the_key_is_absent(monkeypatch, tmp_path):
    """With the store fixed, the reader's empty-only guard becomes load-bearing, so pin it.

    Assert that no `git fetch` is ISSUED, not merely that the guard returned False: a reader
    that fetched garbage and failed would also return False, which is the behaviour being
    replaced.
    """
    import ci.jobs.docs_job_mintlify as mintlify

    fetches = []

    class _ReaderInfo:
        pr_number = 12345
        sha = _HEAD
        pr_labels = []

        def get_changed_files(self):
            return ["src/Core/Defines.h"]

        def get_kv_data(self, key):
            assert key == _KEY
            return None

    monkeypatch.setattr(mintlify, "Info", _ReaderInfo)
    monkeypatch.setattr(mintlify, "check_readonly_copies", lambda _files: True)
    monkeypatch.setattr(
        mintlify.Shell, "check", lambda cmd, **kw: fetches.append(cmd) or True
    )

    assert mintlify._protected_docs_guard() is False
    assert fetches == []


# --- the predicate itself ----------------------------------------------------


@pytest.mark.parametrize("value", [_SHA, "0" * 40, "a" * 40, "0123456789abcdef" * 2 + "01234567"])
def test_is_commit_sha_accepts_full_lowercase_hex(value):
    assert _is_commit_sha(value)


@pytest.mark.parametrize(
    "value",
    [
        "",
        None,
        _SHA[:39],
        _SHA + "0",
        _SHA.upper(),
        "g" * 40,
        # A partial match must not be accepted: a body that happens to embed a commit id
        # would otherwise pass, and it is still not a refspec.
        f"{_SHA}\n{_SHA}",
        f" {_SHA}",
        f"{_SHA} ",
        f"prefix {_SHA}",
        _BODY_404,
        _BODY_500,
    ],
)
def test_is_commit_sha_rejects_everything_else(value):
    assert not _is_commit_sha(value)
