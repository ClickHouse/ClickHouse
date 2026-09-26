"""
Regression test for the dictionary coverage check in tests/fuzz/update_dict.sh.

The check compares the binary-derived all.dict against the source-derived
dictionary with `comm`. Both inputs are sorted with LC_ALL=C, so `comm` has to
compare with that same collation: it merges its inputs assuming they are sorted
the way it itself would sort them, and it exits non-zero when it diagnoses
disorder. The libFuzzer job container sets LC_ALL=en_US.UTF-8
(ci/docker/test-base/Dockerfile), and the quoted multi-word keywords order
differently under UTF-8 collation than under C ('"ADD INDEX"' before '"ADD"'
in C, after it in en_US), so an unpinned `comm` diagnosed disorder, exited 1,
and killed the script through `set -e` inside the command substitution, before
the coverage verdict was ever evaluated. That aborted every nightly fuzzer run
for a week and looked like a generic CalledProcessError from the Python wrapper.

The check therefore has to distinguish three outcomes, and both of the failing
ones exit 1, so these tests assert on the messages rather than on the status:
  - the dictionaries cover each other -> success, no error,
  - a token is missing -> the "tokens present ..." message naming the token,
  - the comparison itself broke -> the "failed to compare" message.

The two sorts must also run as plain commands rather than inside process
substitutions: `set -e` does not observe a process substitution's exit status,
so an unreadable all.dict made the check pass silently with an empty diff, and
an unreadable source.dict made it report every token as missing.

The check text is extracted verbatim between the BEGIN/END markers in
update_dict.sh and run against synthetic dictionaries under a UTF-8 collating
locale, which is what makes the pre-change control (test_unpinned_comm_aborts)
reproduce the original abort.
"""

import os
import re
import stat
import subprocess
import textwrap

import pytest

_UPDATE_DICT = os.path.join(
    os.path.dirname(__file__), "..", "..", "tests", "fuzz", "update_dict.sh"
)

# A pair of tokens whose relative order differs between C and UTF-8 collation:
# the discriminator is the quote (0x22) against the space (0x20).
_MULTIWORD = '"ADD INDEX"'
_SINGLE = '"ADD"'


def _extract_check() -> str:
    """The coverage check, verbatim, from between the BEGIN/END markers."""
    text = open(_UPDATE_DICT, encoding="utf-8").read()
    m = re.search(
        r"# BEGIN: dictionary coverage check\n(.*?)\n\s*# END: dictionary coverage check",
        text,
        re.DOTALL,
    )
    assert m, "BEGIN/END dictionary-coverage-check markers not found in update_dict.sh"
    return textwrap.dedent(m.group(1))


def _utf8_collating_locale():
    """A locale whose collation actually reorders the token pair, or None.

    Availability is not enough: C.utf8 is a UTF-8 locale but collates by byte
    value exactly like C, so it would not exercise the mismatch at all. So the
    locale is chosen by measuring that it inverts the pair, which makes the
    pre-change control non-vacuous by construction.
    """
    try:
        available = subprocess.run(
            ["locale", "-a"], capture_output=True, text=True, timeout=60
        ).stdout.split()
    except (OSError, subprocess.SubprocessError):
        return None
    c_order = [_MULTIWORD, _SINGLE]
    for loc in available:
        low = loc.lower()
        if "utf" not in low or low.startswith("c."):
            continue
        out = subprocess.run(
            ["sort", "-u"],
            input="\n".join(c_order) + "\n",
            capture_output=True,
            text=True,
            env={**os.environ, "LC_ALL": loc},
            timeout=60,
        )
        if out.returncode == 0 and out.stdout.split("\n")[:2] == c_order[::-1]:
            return loc
    return None


def _revert_to_pre_change(check: str) -> str:
    """The check as it shipped before this fix, rebuilt from the current text.

    All three hardenings have to go, or the arm tests a hybrid that never
    existed: the unpinned `comm` in a bare assignment is what let `set -e`
    abort the script silently, the `if !` wrapper would have caught that same
    non-zero status and reported it, and the process substitutions are what hid
    a failing sort. Every substitution is asserted, so this stops matching
    loudly if the check is reworded rather than silently degenerating into a
    no-op.
    """
    sorts = (
        'LC_ALL=C sort -u "$OUTPUT_DIR/all.dict" > "$TMP_DIR/all.sorted"\n'
        'LC_ALL=C sort -u "$TMP_DIR/source.dict" > "$TMP_DIR/source.sorted"\n'
    )
    assert sorts in check, "expected the two materializing sorts in the extracted check"
    check = check.replace(sorts, "")
    guard = (
        "if ! MISSING_TOKENS=$(LC_ALL=C comm -23"
        ' "$TMP_DIR/all.sorted" "$TMP_DIR/source.sorted"); then\n'
    )
    assert guard in check, f"expected {guard!r} in the extracted check"
    check = check.replace(
        guard,
        "MISSING_TOKENS=$(comm -23"
        ' <(LC_ALL=C sort -u "$OUTPUT_DIR/all.dict")'
        ' <(LC_ALL=C sort -u "$TMP_DIR/source.dict"))\n',
    )
    failed = '    echo "error: failed to compare the binary-derived and source-derived dictionaries."\n    exit 1\nfi\n'
    assert failed in check, "expected the compare-failure branch in the extracted check"
    return check.replace(failed, "")


@pytest.fixture
def utf8_locale():
    """A UTF-8 collating locale, or skip. The job container has en_US.UTF-8."""
    loc = _utf8_collating_locale()
    if loc is None:
        pytest.skip(
            "no UTF-8 collating locale available (the coverage check can only "
            "abort where the collation differs from C); generate one with "
            "`locale-gen en_US.UTF-8`"
        )
    return loc


def _run_check(
    tmp_path,
    all_tokens,
    source_tokens,
    locale,
    *,
    unpin=False,
    shim=None,
    remove=(),
):
    """Run the extracted check over synthetic dictionaries. Returns the result.

    all_tokens/source_tokens are written in C-sorted order, exactly as
    update_dict.sh writes them. `unpin` reverts to the pre-change command.
    `shim`, when given, is the body of a fake `comm` placed first on PATH, to
    make the comparison itself fail. `remove` names input dictionaries to delete
    after writing, to make a sort fail.
    """
    out_dir = tmp_path / "out"
    tmp_dir = tmp_path / "tmp"
    out_dir.mkdir()
    tmp_dir.mkdir()
    (out_dir / "all.dict").write_text("\n".join(all_tokens) + "\n", encoding="utf-8")
    (tmp_dir / "source.dict").write_text(
        "\n".join(source_tokens) + "\n", encoding="utf-8"
    )
    for name in remove:
        {"all.dict": out_dir / "all.dict", "source.dict": tmp_dir / "source.dict"}[
            name
        ].unlink()

    check = _extract_check()
    if unpin:
        check = _revert_to_pre_change(check)

    env = {**os.environ, "LC_ALL": locale}
    if shim is not None:
        bindir = tmp_path / "bin"
        bindir.mkdir()
        fake = bindir / "comm"
        fake.write_text(f"#!/bin/bash\n{shim}\n", encoding="utf-8")
        fake.chmod(fake.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        env["PATH"] = f"{bindir}:{env['PATH']}"

    script = (
        "set -euo pipefail\n"
        f'OUTPUT_DIR="{out_dir}"\n'
        f'TMP_DIR="{tmp_dir}"\n' + check + "\n"
    )
    return subprocess.run(
        ["bash", "-c", script], capture_output=True, text=True, timeout=60, env=env
    )


_MISSING_MSG = "tokens present in the binary-derived all.dict are missing"
_COMPARE_FAILED_MSG = "failed to compare"


def test_covered_dictionaries_pass(tmp_path, utf8_locale):
    # The source dictionary is a superset, so nothing is missing. The extra
    # token sits between the pair whose order the collations disagree on, which
    # is what lets `comm` diagnose disorder when it is not pinned.
    res = _run_check(
        tmp_path,
        [_MULTIWORD, _SINGLE],
        [_MULTIWORD, '"ADD JOIN"', _SINGLE],
        utf8_locale,
    )
    assert res.returncode == 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
    assert _MISSING_MSG not in res.stdout
    assert _COMPARE_FAILED_MSG not in res.stdout


def test_missing_token_is_reported(tmp_path, utf8_locale):
    # The multi-word token is absent from the source dictionary: the check must
    # fail and name it, which is the gap this check exists to catch.
    res = _run_check(
        tmp_path, [_MULTIWORD, _SINGLE], ['"ADD JOIN"', _SINGLE], utf8_locale
    )
    assert res.returncode == 1
    assert _MISSING_MSG in res.stdout
    assert _MULTIWORD in res.stdout
    assert _COMPARE_FAILED_MSG not in res.stdout


def test_comparison_failure_is_reported_distinctly(tmp_path, utf8_locale):
    # A broken comparison must report itself, not masquerade as a coverage gap.
    res = _run_check(
        tmp_path,
        [_MULTIWORD, _SINGLE],
        [_MULTIWORD, _SINGLE],
        utf8_locale,
        shim="exit 3",
    )
    assert res.returncode == 1
    assert _COMPARE_FAILED_MSG in res.stdout
    assert _MISSING_MSG not in res.stdout


def test_unpinned_comm_aborts(tmp_path, utf8_locale):
    # The pre-change command: `comm` inherits the container's UTF-8 collation,
    # diagnoses the C-sorted input as unsorted and exits 1, so `set -e` kills
    # the script inside the command substitution and neither verdict is
    # reached. Same fixture as the missing-token case, to show that the abort
    # hides a real gap rather than reporting it.
    res = _run_check(
        tmp_path,
        [_MULTIWORD, _SINGLE],
        ['"ADD JOIN"', _SINGLE],
        utf8_locale,
        unpin=True,
    )
    assert res.returncode == 1
    assert "not in sorted order" in res.stderr
    assert _MISSING_MSG not in res.stdout
    assert _COMPARE_FAILED_MSG not in res.stdout


def test_unreadable_all_dict_does_not_pass_silently(tmp_path, utf8_locale):
    # The worst outcome: with the sort in a process substitution `set -e` never
    # saw it fail, so an unreadable all.dict yielded an empty diff and the check
    # reported success, disarming the gate entirely.
    res = _run_check(
        tmp_path,
        [_MULTIWORD, _SINGLE],
        [_MULTIWORD, '"ADD JOIN"', _SINGLE],
        utf8_locale,
        remove=("all.dict",),
    )
    assert res.returncode != 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
    assert "all.dict" in res.stderr
    assert _MISSING_MSG not in res.stdout


def test_unreadable_source_dict_is_not_reported_as_a_gap(tmp_path, utf8_locale):
    # An unreadable source.dict sorted to nothing, so every token looked
    # missing: a broken comparison masquerading as a coverage gap.
    res = _run_check(
        tmp_path,
        [_MULTIWORD, _SINGLE],
        [_MULTIWORD, '"ADD JOIN"', _SINGLE],
        utf8_locale,
        remove=("source.dict",),
    )
    assert res.returncode != 0, f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
    assert "source.dict" in res.stderr
    assert _MISSING_MSG not in res.stdout
