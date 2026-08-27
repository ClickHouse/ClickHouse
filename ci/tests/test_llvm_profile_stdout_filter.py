"""Regression test for the `LLVM Profile Error:` stdout leak (see #97020). The messages can
arrive glued onto real output, so the transform strips the message SPAN, not the whole line,
and must run after the other in-place stdout rewrites."""

import ast
import os
import runpy
from pathlib import Path

import pytest

_CT = Path(__file__).resolve().parents[2] / "tests" / "clickhouse-test"
# runpy loads clickhouse-test without running __main__; only the pure helper is used.
_strip = runpy.run_path(str(_CT))["strip_llvm_profile_errors_in_file"]
_MERGE_FMT = b"LLVM Profile Error: Profile Merging of file cl-0_0.profraw failed: "
_WRITE_FMT = b'LLVM Profile Error: Failed to write file "cl-0_0.profraw": '
_MERGE = _MERGE_FMT + b"Success"
_WRITE = _WRITE_FMT + b"Invalid argument"
_BAD_ERRNO = b'LLVM Profile Error: Failed to write file "x": Not an errno\n'
_WARNING = b"LLVM Profile Warning: merging\n200\n"
_STRERROR = {os.strerror(e) for e in range(256)}
# Pairs where one strerror value is a prefix of another, e.g. `No such device` and
# `No such device or address`. Derived from libc, never from the pattern under test.
_PREFIX_PAIRS = sorted(
    (s, l) for s in _STRERROR for l in _STRERROR if s != l and l.startswith(s)
)


@pytest.mark.parametrize(
    "stdout,expected",
    [
        (b"LLVM Profile Error: Invalid profile data to merge\n200\n", b"200\n"),
        (_MERGE + b"\n200\n", b"200\n"),
        (_WRITE + b"\n200\n", b"200\n"),
        # Glued onto real output: only the message goes, the `200` stays.
        (_MERGE + b"200\n400\n", b"200\n400\n"),
        # No marker: byte-identical, so the arms above cannot pass by rewriting all.
        (b"200\n400\n", b"200\n400\n"),
        (b"\xff" + _WRITE + b"\xfe200\n", b"\xff\xfe200\n"),
        # `Warning:` is out of scope; an unknown errno keeps the whole message.
        (_WARNING, _WARNING),
        (_BAD_ERRNO, _BAD_ERRNO),
    ],
)
def test_span_stripped_and_real_output_preserved(tmp_path, stdout, expected):
    path = tmp_path / "stdout"
    path.write_bytes(stdout)
    _strip(str(path))
    assert path.read_bytes() == expected


@pytest.mark.parametrize("short,long_", _PREFIX_PAIRS)
@pytest.mark.parametrize("kind", [_MERGE_FMT, _WRITE_FMT])
def test_ambiguous_errno_tail_is_left_whole(tmp_path, kind, short, long_):
    # Two readings of one byte stream: the runtime printed `long_`, or it printed `short`
    # and the test's own output opens with the rest. Stripping either eats the other's
    # output, so the message survives intact and the diff stays loud.
    data = kind + long_.encode() + b"\n200\n"
    path = tmp_path / "stdout"
    path.write_bytes(data)
    _strip(str(path))
    assert path.read_bytes() == data


@pytest.mark.parametrize("short,long_", _PREFIX_PAIRS)
@pytest.mark.parametrize("kind", [_MERGE_FMT, _WRITE_FMT])
def test_delimited_errno_tail_is_still_stripped(tmp_path, kind, short, long_):
    # A newline after the shorter value settles which reading applies, so the guard above
    # must not fire and the noise must still go.
    real = long_[len(short) :].encode() + b"\n200\n"
    path = tmp_path / "stdout"
    path.write_bytes(kind + short.encode() + b"\n" + real)
    _strip(str(path))
    assert path.read_bytes() == real


def test_libc_still_reports_prefix_pairs(tmp_path):
    # Without a pair the two arms above match nothing and prove nothing.
    assert _PREFIX_PAIRS, "libc reports no prefix pairs; the arms above are vacuous"


def test_called_after_the_other_stdout_rewrites():
    nodes = [
        n
        for fn in ast.walk(ast.parse(_CT.read_bytes()))
        if isinstance(fn, ast.FunctionDef) and fn.name == "run_single_test"
        for n in ast.walk(fn)
    ]
    calls, returns = {}, [n.lineno for n in nodes if isinstance(n, ast.Return)]
    for n in nodes:
        arg = n.args[0] if isinstance(n, ast.Call) and n.args else None
        if getattr(arg, "attr", None) == "stdout_file":
            calls.setdefault(getattr(n.func, "id", ""), []).append(n.lineno)
    rewrites = calls.get("replace_in_file", []) + calls.get("replace_in_file_re", [])
    strip_at = calls.get("strip_llvm_profile_errors_in_file", [])
    # After every in-place rewrite, so it sees their output, and before the return.
    assert rewrites, "no stdout_file rewrites found; run_single_test moved"
    assert len(strip_at) == 1, strip_at
    assert max(rewrites) < strip_at[0] < min(returns), (rewrites, strip_at, returns)
