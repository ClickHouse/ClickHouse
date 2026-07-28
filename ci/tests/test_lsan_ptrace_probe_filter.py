"""
Regression tests for the LSan `TestPTrace` sanitizer-log filter in tests/clickhouse-test.

compiler-rt's `TestPTrace` (contrib/llvm-project/compiler-rt/lib/sanitizer_common/
sanitizer_stoptheworld_linux_libcdep.cpp) forks a short-lived child that issues one throwaway
`ptrace` call to find out whether `ptrace` is blocked before the stop-the-world tracer attaches to
the real threads.  If that child dies from a signal, LSan prints two lines back to back:

    WARNING: ptrace appears to be blocked (is seccomp enabled?). LeakSanitizer may hang.
    Child exited with signal N.

Both spellings are ambiguous on their own, so the filter requires two conditions together: the
test carries the `kills-processes-by-cmdline` tag (only such a test can kill the probe child, which
inherits the client's argv), and the reported signal is not SIGSYS (which is how seccomp kills the
child when it really denies the ptrace call).

The tag is load-bearing rather than cosmetic.  `TestPTrace` ignores the result of its
`internal_waitpid` and reads a possibly untouched `wstatus`, so an EINTR-interrupted wait reports a
stale signal number: with a SIGINT raced against a genuinely seccomp-blocked probe, 812 of 2000
real blocks reported a stale non-SIGSYS signal, versus 0 of 2000 without the race.  Requiring the
tag confines that upstream misreport to the one test that already tolerates it and leaves every
other test's diagnostics untouched.

`02435_rollback_cancelled_queries` only reaches the benign case through a timing race, so it
cannot pin this behaviour.  These tests drive the runner's own filtering path so that the intended
suppression, the preservation of a real diagnostic, and the tag scoping all stay verified.
"""

import runpy
import signal
from pathlib import Path

_CLICKHOUSE_TEST = str(
    Path(__file__).resolve().parent.parent.parent / "tests" / "clickhouse-test"
)

# runpy.run_path handles the missing .py extension and the hyphen in the name.
_ct = runpy.run_path(_CLICKHOUSE_TEST)
find_benign_lsan_ptrace_probe_lines = _ct["find_benign_lsan_ptrace_probe_lines"]
LSAN_PTRACE_WARNING = _ct["LSAN_PTRACE_WARNING"]
KILLS_PROCESSES_BY_CMDLINE_TAG = _ct["KILLS_PROCESSES_BY_CMDLINE_TAG"]
# Aliased away from a Test* name so pytest does not try to collect it as a test class.
RunnerTestCase = _ct["TestCase"]
RunnerTestSuite = _ct["TestSuite"]

# compiler-rt's `Report` prefixes every line with ==PID==.
_PID = 4242
_WARNING = f"=={_PID}=={LSAN_PTRACE_WARNING}. LeakSanitizer may hang."
_UAF = f"=={_PID}==ERROR: AddressSanitizer: heap-use-after-free on address 0x602000000010"


def _child_exit(sig):
    return f"=={_PID}==Child exited with signal {int(sig)}."


def _benign_pair(sig=signal.SIGKILL):
    return [_WARNING, _child_exit(sig)]


def _real_seccomp_pair():
    # Verified against a real SECCOMP_SET_MODE_FILTER policy for SYS_ptrace: SECCOMP_RET_KILL_THREAD,
    # SECCOMP_RET_KILL_PROCESS and SECCOMP_RET_TRAP all report WTERMSIG == SIGSYS.
    return [_WARNING, _child_exit(signal.SIGSYS)]


class _Args:
    """Stub runner args: anything this test does not care about reads as falsy."""

    def __init__(self, tmp_path):
        self.debug_log_file = str(tmp_path / "case.debuglog")
        self.testcase_database = "test_db"

    def __getattr__(self, name):
        return None


def _kept(lines, tmp_path, extra_log_lines=None, tagged=True):
    """Lines that survive the real sanitizer-log filtering in `TestCase.process_result_impl`.

    The filter is not reimplemented here: sanitizer logs are written where the runner looks for
    them and the production method is called, so disconnecting the filter from the runner fails
    these tests too.
    """
    case = RunnerTestCase.__new__(RunnerTestCase)
    case.fatal_sanitizer_prefix = str(tmp_path / "case.stderr-fatal")
    case.stdout_file = str(tmp_path / "case.stdout")
    case.stderr_file = str(tmp_path / "case.stderr")
    case.testcase_args = _Args(tmp_path)
    case.args = case.testcase_args
    case.name = "probe_case"
    case.suite = None
    case.tags = {KILLS_PROCESSES_BY_CMDLINE_TAG} if tagged else set()
    # reference_file=None with a falsy --record makes process_result_impl return right after the
    # sanitizer-log filtering, so the returned description carries exactly the filter's output.
    case.reference_file = None

    for index, content in enumerate([lines] + list(extra_log_lines or [])):
        path = f"{case.fatal_sanitizer_prefix}.{_PID + index}"
        with open(path, "w", encoding="utf-8") as handle:
            handle.write("".join(f"{line}\n" for line in content))

    description = RunnerTestCase.process_result_impl(case, None, 0.0).description or ""
    reported = [line for line in description.splitlines() if line.startswith(f"=={_PID}==")]
    assert ("Path:" in description) == bool(reported), description
    return reported


def test_benign_probe_killed_by_sigkill_is_dropped(tmp_path):
    assert _kept(_benign_pair(signal.SIGKILL), tmp_path) == []


def test_benign_probe_killed_by_sigint_is_dropped(tmp_path):
    assert _kept(_benign_pair(signal.SIGINT), tmp_path) == []


def test_benign_probe_killed_by_the_signals_seen_in_ci_is_dropped(tmp_path):
    # The only signals this pair has ever carried in CI (180 days: 13 hits with 42, 5 with 36,
    # zero with SIGSYS), both real-time signals rather than the SIGINT/SIGKILL thread_cancel
    # sends: WTERMSIG reports whatever signal reaped the probe, so the check must not be narrowed
    # to the signals a test sends directly.
    for sig in (42, 36):
        assert _kept(_benign_pair(sig), tmp_path) == [], sig


def test_benign_probe_killed_by_any_non_sigsys_signal_is_dropped(tmp_path):
    for sig in (signal.SIGTERM, signal.SIGSEGV, signal.SIGABRT, signal.SIGRTMIN, 42, 64):
        assert _kept(_benign_pair(sig), tmp_path) == [], sig


def test_repeated_benign_pairs_are_dropped(tmp_path):
    assert _kept(_benign_pair(signal.SIGKILL) + _benign_pair(signal.SIGINT), tmp_path) == []


def test_real_seccomp_block_is_kept(tmp_path):
    lines = _real_seccomp_pair()
    assert _kept(lines, tmp_path) == lines


def test_real_seccomp_block_is_kept_after_a_benign_pair(tmp_path):
    # The decision is per occurrence: a benign pair earlier in the same log must not hide a
    # genuine seccomp block later in it.
    assert _kept(_benign_pair() + _real_seccomp_pair(), tmp_path) == _real_seccomp_pair()


def test_real_seccomp_block_is_kept_before_a_benign_pair(tmp_path):
    assert _kept(_real_seccomp_pair() + _benign_pair(), tmp_path) == _real_seccomp_pair()


def test_warning_without_a_child_exit_line_is_kept(tmp_path):
    assert _kept([_WARNING], tmp_path) == [_WARNING]


def test_warning_as_the_last_line_is_kept(tmp_path):
    # Guards the index + 1 lookahead against running off the end of the log.
    assert _kept([_UAF, _WARNING], tmp_path) == [_UAF, _WARNING]


def test_child_exit_without_the_warning_is_kept(tmp_path):
    lines = [_child_exit(signal.SIGSEGV)]
    assert _kept(lines, tmp_path) == lines


def test_non_adjacent_warning_and_child_exit_are_kept(tmp_path):
    # Only a back-to-back pair is the probe; anything in between means these lines are unrelated.
    lines = [_WARNING, _UAF, _child_exit(signal.SIGKILL)]
    assert _kept(lines, tmp_path) == lines


def test_real_diagnostic_next_to_a_benign_pair_is_kept(tmp_path):
    assert _kept(_benign_pair() + [_UAF], tmp_path) == [_UAF]


def test_real_diagnostic_in_another_log_file_is_kept(tmp_path):
    # The runner globs several sanitizer logs per test; a benign pair in one must not affect
    # what is reported from the others.
    assert _kept(_benign_pair(), tmp_path, extra_log_lines=[[_UAF]]) == [_UAF]


def test_benign_pair_alone_produces_no_report(tmp_path):
    assert _kept([], tmp_path, extra_log_lines=[_benign_pair()]) == []


def test_untagged_test_keeps_the_pair(tmp_path):
    # Without the tag nothing is dropped, whatever the signal: a test that does not kill by
    # /proc/*/cmdline cannot produce the benign case, and this is what keeps an EINTR-mangled
    # signal number from hiding a real seccomp block elsewhere.
    for sig in (signal.SIGKILL, signal.SIGINT, 42, 36):
        assert _kept(_benign_pair(sig), tmp_path, tagged=False) == _benign_pair(sig), sig


def test_untagged_test_keeps_a_real_seccomp_block(tmp_path):
    assert _kept(_real_seccomp_pair(), tmp_path, tagged=False) == _real_seccomp_pair()


def test_empty_log_has_nothing_to_drop(tmp_path):
    assert _kept([], tmp_path) == []
    assert find_benign_lsan_ptrace_probe_lines([], True) == set()


def test_helper_drops_nothing_without_the_tag(tmp_path):
    assert find_benign_lsan_ptrace_probe_lines(_benign_pair(), False) == set()
    assert find_benign_lsan_ptrace_probe_lines(_benign_pair(), True) == {0, 1}


def test_suite_loader_reads_the_tag_from_the_test_file():
    # End to end: the tag has to survive the runner's own suite loader, not just be spelled
    # correctly in the test file, and it must not leak onto other tests.
    suite_dir = str(Path(_CLICKHOUSE_TEST).parent / "queries" / "0_stateless")
    tagged = "02435_rollback_cancelled_queries.sh"
    untagged = "02434_cancel_insert_when_client_dies.sh"
    all_tags, _, _ = RunnerTestSuite.read_test_tags_and_random_settings_limits(
        suite_dir, [tagged, untagged]
    )
    assert KILLS_PROCESSES_BY_CMDLINE_TAG in all_tags[tagged]
    assert KILLS_PROCESSES_BY_CMDLINE_TAG not in all_tags[untagged]
