"""
Regression tests for sanitizer-report classification in ci/jobs/ast_fuzzer_job.py.

run-fuzzer.sh points every runtime at *SAN_OPTIONS=log_path, so a reporting
process writes its own sanitizer.log.<pid> and an EXIT trap merges them all into
stderr.log/server.log. A report that kills the CLIENT leaves the server alive, so
status.tsv is "0 0 134" and the job lands in the catch-all branch, which reported
an opaque "Client failure (see logs)" plus a fuzzer.log tail and never named the
finding - and fuzzer.log holds no sanitizer text at all, so the paste was empty of
it. The parser that can name the report was gated behind `status != ERROR`, i.e.
skipped on exactly that branch, leaving CIDB's test_name empty: the finding was
unsearchable and read as benign.

These tests drive the real run_fuzz_job against a seeded temp workspace (the
container run, the ownership fix and the host `dmesg` read are stubbed; nothing
else is) and pin:

  - a report is NAMED and the run is red in every exit-code branch, including
    the benign (0, 137, 143) one - *SAN_OPTIONS reaches the server and the
    ignored client probes, so a runtime can report while the fuzzer exits 0,
  - the sanitized-build OOM leniency is vetoed by a real finding, while a
    benign "failed to allocate" report alone keeps today's lenient verdict,
  - benign-ness is decided on the diagnostic the parser selected: not on the
    file (a recoverable warning does not abort, so one file can hold a benign
    record followed by a real one) and not on the normalized name (which drops
    "failed to allocate"),
  - every report file is parsed, so a benign one cannot shadow a real one, and
    an OOM record ordered first in the merged log cannot be the only thing named,
  - the report path is the parser's only input, so it can never name the normal
    shutdown's "Received signal 15",
  - an upstream ERROR is not downgraded to FAIL by the added sub-results, and
    BuzzHouse's own diagnostic is preserved,
  - a missing/malformed status.tsv still names the report (that path ends in
    `complete_job`, which calls `sys.exit`),
  - a report that cannot be classified at all fails CLOSED with its own named
    sub-result instead of leaving the run green - whether the parser raised, the
    report could not be read, or a tool the parser shells out to was missing,
  - the merged-log parse, which selects whichever record was concatenated first,
    does not emit a FAIL row for a record the per-report pass calls benign,
  - stale state from a previous run - status.tsv above all - is removed before
    the container runs, fail-closed.
"""

import os
import shutil
import sys
import textwrap

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.ast_fuzzer_job as job
from ci.praktika.result import Result

# ---------------------------------------------------------------------------
# Fixtures: real sanitizer output shapes.
#
# Verbatim excerpt of the report the failing run produced (PR #111194,
# AST fuzzer (amd_msan), artifact sanitizer.log.914): a use-after-free, which
# MSan words as "created by a heap deallocation".
MSAN_UAF_REPORT = textwrap.dedent("""\
    ==914==WARNING: MemorySanitizer: use-of-uninitialized-value
        #0 0x555b2c5597fe in DB::decideParensEmission(DB::IAST const&, DB::IAST::FormatStateStacked&) src/Parsers/IAST.cpp:387:9
        #1 0x555b2c5599ea in DB::IAST::format(DB::WriteBuffer&, DB::IAST::FormatSettings const&) src/Parsers/IAST.cpp:485:25
        #2 0x555b2c61b0a1 in DB::ASTExecuteAsQuery::formatQueryImpl(DB::WriteBuffer&) const src/Parsers/Access/ASTExecuteAsQuery.cpp:38:19
        #3 0x555b2c5f2233 in DB::ASTQueryWithOutput::formatImpl(DB::WriteBuffer&) const src/Parsers/ASTQueryWithOutput.cpp:32:5
        #6 0x555b2c5541aa in DB::IAST::formatWithSecretsOneLine() const src/Parsers/IAST.cpp:239:12
        #7 0x555b29b0f7c1 in DB::QueryFuzzer::fuzzMain(std::shared_ptr<DB::IAST>&) src/Common/QueryFuzzer.cpp:7701:29
        #8 0x555b28d0182b in DB::Client::processWithASTFuzzer(std::basic_string_view<char>) programs/client/FuzzLoop.cpp:259:24

      Uninitialized value was created by a heap deallocation
        #0 0x555b28c8f0dd in operator delete(void*)
        #1 0x555b2c552e11 in DB::intrusive_ptr_release(DB::IAST const*) src/Parsers/IAST.cpp:71:17
        #4 0x555b29b02f19 in DB::QueryFuzzer::fuzz(std::shared_ptr<DB::IAST>&) src/Common/QueryFuzzer.cpp:6182:21
        #7 0x555b29b0f4a3 in DB::QueryFuzzer::fuzzMain(std::shared_ptr<DB::IAST>&) src/Common/QueryFuzzer.cpp:7683:5

    SUMMARY: MemorySanitizer: use-of-uninitialized-value src/Parsers/IAST.cpp:387:9 in DB::decideParensEmission(DB::IAST const&, DB::IAST::FormatStateStacked&)
    Exiting
    """)

# Recoverable allocator pressure. Note there is NO colon after the tool name:
# compiler-rt emits Report("WARNING: MemorySanitizer failed to allocate 0x%zx
# bytes\n", size) (lib/msan/msan_allocator.cpp) and keeps running under
# allocator_may_return_null=1. That is why it matches neither parser start
# pattern, which is what lets a complete file be handed to the parser unsplit.
BENIGN_ALLOC_REPORT = (
    "==500==WARNING: MemorySanitizer failed to allocate 0x100000000 bytes\n"
)

# A FATAL allocator report does have the colon, and calls `Die`, so nothing
# follows it.
FATAL_OOM_REPORT = textwrap.dedent("""\
    ==501==ERROR: MemorySanitizer: out-of-memory: allocator is trying to allocate 0x10000000000 bytes
        #0 0x1 in DB::hugeAlloc() src/Alloc.cpp:7:3
    SUMMARY: MemorySanitizer: out-of-memory src/Alloc.cpp:7:3 in DB::hugeAlloc()
    """)

ASAN_UAF_REPORT = textwrap.dedent("""\
    ==600==ERROR: AddressSanitizer: heap-use-after-free on address 0x602000000010 at pc 0x000000401234
        #0 0xaa in DB::uafHere(int) src/Uaf.cpp:11:7
        #1 0xbb in DB::uafCaller() src/Uaf.cpp:22:3
    SUMMARY: AddressSanitizer: heap-use-after-free src/Uaf.cpp:11:7 in DB::uafHere(int)
    """)

TSAN_RACE_REPORT = textwrap.dedent("""\
    ==700==WARNING: ThreadSanitizer: data race (pid=700)
      Write of size 8 at 0x7b0400000010 by thread T1:
        #0 0xaa in DB::raceWrite(int) src/Race.cpp:9:5
        #1 0xbb in DB::raceCaller() src/Race.cpp:19:3
    SUMMARY: ThreadSanitizer: data race src/Race.cpp:9:5 in DB::raceWrite(int)
    """)

# UBSan renders "<loc>: runtime error: <msg>" with a bare Printf
# (lib/ubsan/ubsan_diag.cpp), so there is no ==pid== header and no tool name:
# the parser cannot tell which runtime it was and names it "Sanitizer (STID: ...)".
UBSAN_HEADERLESS_REPORT = textwrap.dedent("""\
    src/Ub.cpp:42:13: runtime error: signed integer overflow: 9223372036854775807 + 1 cannot be represented in type 'long'
        #0 0xaa in DB::overflowHere(long) src/Ub.cpp:42:13
        #1 0xbb in DB::overflowCaller() src/Ub.cpp:55:3
    """)

# What the parser sees for a report it cannot READ: its `rg` finds nothing, so
# the diagnostic comes back empty exactly as for a garbage report. The body is
# non-empty because the upload filter skips zero-byte files, and a report the job
# cannot read still has its real size on disk - an empty body would make the
# "the report is attached" assertion pass or fail for the wrong reason.
UNREADABLE_REPORT_BODY = "\x00\x01 unreadable to this job\n"

SERVER_LOG_SHUTDOWN = (
    "2026.07.21 10:00:00.000000 [ 1 ] {} <Trace> Application: Received signal 15\n"
    "2026.07.21 10:00:01.000000 [ 1 ] {} <Information> Application: shutting down\n"
)


class _JobCompleted(Exception):
    """Carries the Result that run_fuzz_job would have reported."""

    def __init__(self, result):
        super().__init__(result.status)
        self.result = result


@pytest.fixture
def run_job(tmp_path, monkeypatch):
    """Drive the real run_fuzz_job against a seeded temp workspace.

    Only the container run, the docker image, the ownership repair, the core
    collection and the host `dmesg` read are stubbed - the classification code
    under test is the real one, and so is FuzzerLogParser.
    """
    workspace = tmp_path / "ci" / "tmp" / "workspace"
    workspace.mkdir(parents=True)
    (tmp_path / "ci" / "tmp" / "clickhouse").write_text("#!/bin/sh\n")

    monkeypatch.setattr(job, "cwd", str(tmp_path))
    monkeypatch.setattr(job, "WORKSPACE_PATH", workspace)
    monkeypatch.setattr(
        job,
        "JOB_ARTIFACTS",
        tuple(
            workspace / n
            for n in ("server.log", "fuzzer.log", "stderr.log", "dmesg.log", "fatal.log")
        ),
    )
    # Re-root the PRODUCTION list onto the temp workspace instead of restating
    # it: the list itself is under test, so a name dropped from it must vanish
    # here too. raising=False keeps this file runnable against a revision that
    # predates the cleanup, so the classification tests fail on their own
    # assertions there instead of erroring out in setup.
    monkeypatch.setattr(
        job,
        "_STALE_RUN_STATE",
        tuple(workspace / p.name for p in getattr(job, "_STALE_RUN_STATE", ())),
        raising=False,
    )

    class _Image:
        def pull_image(self):
            return self

        def __str__(self):
            return "clickhouse/fuzzer:test"

    class _DockerImage:
        @staticmethod
        def get_docker_image(_name):
            return _Image()

    monkeypatch.setattr(job, "DockerImage", _DockerImage)

    class _Info:
        is_local_run = True

        def __init__(self):
            self.job_name = _Info.job_name

        def get_changed_files(self):
            return []

    monkeypatch.setattr(job, "Info", _Info)

    real_check = job.Shell.check
    real_get_output = job.Shell.get_output

    shelled = []

    def _check(command=None, *args, **kwargs):
        shelled.append(str(command))
        if str(command).startswith("dmesg "):
            # The non-sanitized branch reads the HOST's kernel ring buffer, which
            # is not an input to these tests: on a host that has seen an OOM kill
            # the real dmesg flips the verdict to ERROR and suppresses the
            # merged-log parse a test may exist to exercise. Seed an EMPTY
            # dmesg.log instead. Only this one command is intercepted - the grep
            # over dmesg.log runs for real, so the branch logic stays under test.
            (workspace / "dmesg.log").write_text("")
            return True
        return real_check(command, *args, **kwargs)

    class _Shell:
        @staticmethod
        def check(command=None, *args, **kwargs):
            if "docker run" in str(command):
                return True  # the container is what the seeded state stands in for
            return _check(command, *args, **kwargs)

        get_output = staticmethod(real_get_output)

    monkeypatch.setattr(job, "Shell", _Shell)

    class _Utils:
        @staticmethod
        def fix_ownership_after_docker(*args, **kwargs):
            pass

    monkeypatch.setattr(job, "Utils", _Utils)

    class _ClickHouseService:
        @staticmethod
        def collect_cores(_path):
            return []

    monkeypatch.setattr(job, "ClickHouseService", _ClickHouseService)
    monkeypatch.setattr(
        Result, "complete_job", lambda self, *a, **kw: (_ for _ in ()).throw(
            _JobCompleted(self)
        )
    )
    monkeypatch.chdir(tmp_path)

    def _run(
        status_tsv="0\t0\t134",
        reports=(),
        server_log="",
        fuzzer_log="fuzzing step 1\n",
        check_name="AST fuzzer (amd_msan)",
        stale=(),
        keep_preseeded=True,
        after_seed=None,
    ):
        """Seed the workspace as the runner would have left it, then classify.

        `stale` seeds files BEFORE the (stubbed) container runs, i.e. leftovers of
        a previous run, and disables the keep-preseeded opt-in so the production
        cleanup really runs.

        `after_seed` runs once the workspace is written and stands in for whatever
        the real environment did to the artifacts after the container exited - the
        ownership repair at the end of `run_fuzz_job` is what can leave a report
        unreadable.
        """
        _Info.job_name = check_name
        for name, text in stale:
            (workspace / name).write_text(text)
        if stale:
            keep_preseeded = False
        if keep_preseeded:
            monkeypatch.setenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE", "1")
        else:
            monkeypatch.delenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE", raising=False)
            # Post-container state has to be written by the stubbed container so
            # the cleanup cannot delete it; wrap Shell.check to do it there.
            def _seed():
                _write_run_output(status_tsv, reports, server_log, fuzzer_log)
                if after_seed is not None:
                    after_seed()

            monkeypatch.setattr(
                _Shell,
                "check",
                staticmethod(
                    lambda command=None, *a, **kw: (
                        (_seed(), True)[1]
                        if "docker run" in str(command)
                        else _check(command, *a, **kw)
                    )
                ),
            )
        if keep_preseeded:
            _write_run_output(status_tsv, reports, server_log, fuzzer_log)
            if after_seed is not None:
                after_seed()
        try:
            job.run_fuzz_job(check_name)
        except _JobCompleted as done:
            return done.result
        raise AssertionError("run_fuzz_job did not report a result")

    def _write_run_output(status_tsv, reports, server_log, fuzzer_log):
        (workspace / "fuzzer.log").write_text(fuzzer_log)
        # collect_sanitizer_reports merges every report into stderr.log AND
        # server.log; server_log adds whatever else the server logged.
        merged = "".join(
            f"=== sanitizer report from {name} ===\n{text}\n" for name, text in reports
        )
        (workspace / "stderr.log").write_text(merged)
        (workspace / "server.log").write_text(server_log + merged)
        for name, text in reports:
            (workspace / name).write_text(text)
        if status_tsv is not None:
            (workspace / "status.tsv").write_text(status_tsv + "\n")

    _run.workspace = workspace
    _run.shelled = shelled
    return _run


def _names(result):
    return [sub.name for sub in result.results]


def _named_sanitizer(result):
    return [n for n in _names(result) if "Sanitizer" in n]


# ---------------------------------------------------------------------------
# 1. The failure this PR is about: a client killed by MSan on a live server.
def test_client_failure_with_msan_report_is_named_and_error(run_job):
    result = run_job(
        status_tsv="0\t0\t134", reports=(("sanitizer.log.914", MSAN_UAF_REPORT),)
    )
    # Explicitly ERROR, not merely "not OK": master's verdict for a client
    # failure is ERROR and the added FAIL sub-result must not downgrade it.
    assert result.status == Result.Status.ERROR
    named = _named_sanitizer(result)
    assert len(named) == 1, _names(result)
    assert "MemorySanitizer" in named[0]
    assert "use-of-uninitialized-value" in named[0]
    assert "STID:" in named[0]
    sub = result.results[0]
    assert "src/Parsers/IAST.cpp:387" in sub.info
    assert any("sanitizer.log.914" in str(f) for f in sub.files), sub.files
    assert "Client failure (see logs)" in result.info


# 2. No report: today's behavior, byte for byte. A caught client-side exception
#    (type mismatch, parse error, access denied) must not start firing.
def test_client_failure_without_report_is_unchanged(run_job):
    result = run_job(
        status_tsv="0\t0\t134",
        reports=(),
        fuzzer_log="Code: 53. DB::Exception: Type mismatch in IN or VALUES section\n",
    )
    assert result.status == Result.Status.ERROR
    assert result.results == []
    assert "Client failure (see logs)" in result.info
    assert "Fuzzer log (last 200 lines):" in result.info
    assert "Sanitizer" not in result.info


# 3. Benign-only report: the lenient verdict is preserved.
def test_benign_allocation_report_alone_stays_ok(run_job):
    result = run_job(
        status_tsv="1\t0\t0", reports=(("sanitizer.log.500", BENIGN_ALLOC_REPORT),)
    )
    assert result.status == Result.Status.OK
    assert _named_sanitizer(result) == []
    assert "Sanitizer OOM - test considered passed" in result.info


# 4. Benign AND real together: the leniency must not swallow the real finding.
def test_benign_oom_does_not_forgive_a_real_finding(run_job):
    result = run_job(
        status_tsv="1\t0\t0",
        reports=(
            ("sanitizer.log.500", BENIGN_ALLOC_REPORT),
            ("sanitizer.log.914", MSAN_UAF_REPORT),
        ),
    )
    assert result.status != Result.Status.OK
    named = _named_sanitizer(result)
    assert any("use-of-uninitialized-value" in n for n in named), _names(result)
    assert "considered passed" not in result.info


# 5. A report left by a PREVIOUS run must not be attributed to this one.
def test_stale_report_is_not_attributed_to_a_clean_run(run_job):
    result = run_job(
        status_tsv="0\t0\t0",
        reports=(),
        stale=(("sanitizer.log.914", MSAN_UAF_REPORT),),
    )
    assert result.status == Result.Status.OK
    assert _named_sanitizer(result) == []
    assert not list(run_job.workspace.glob("sanitizer.log.*"))


# 6. Every runtime is named with its own prefix.
@pytest.mark.parametrize(
    "report,expected",
    [
        (MSAN_UAF_REPORT, "MemorySanitizer: use-of-uninitialized-value"),
        (ASAN_UAF_REPORT, "AddressSanitizer: heap-use-after-free"),
        (TSAN_RACE_REPORT, "ThreadSanitizer: data race"),
        # Headerless UBSan carries no tool name at all, so the generic name is
        # the honest one - still greppable and STID-keyed.
        (UBSAN_HEADERLESS_REPORT, "Sanitizer (STID:"),
    ],
)
def test_each_runtime_is_named(run_job, report, expected):
    result = run_job(status_tsv="0\t0\t134", reports=(("sanitizer.log.1", report),))
    named = _named_sanitizer(result)
    assert len(named) == 1, _names(result)
    assert expected in named[0]
    assert result.status != Result.Status.OK


# 7. A dead server keeps producing exactly one row for one finding: here the
#    existing parser (which reads the merged stderr.log) and the per-report pass
#    both name the same finding, so the dedup is what keeps it to one row.
def test_server_died_reports_the_finding_once(run_job):
    result = run_job(
        status_tsv="1\t0\t0", reports=(("sanitizer.log.914", MSAN_UAF_REPORT),)
    )
    named = _named_sanitizer(result)
    assert len(named) == 1, f"finding reported more than once: {_names(result)}"
    assert "use-of-uninitialized-value" in named[0]
    assert result.status != Result.Status.OK


# 8. BuzzHouse 227 keeps its own diagnostic while the report is still named.
def test_buzzhouse_diagnostic_is_preserved(run_job):
    result = run_job(
        status_tsv="0\t0\t227",
        reports=(("sanitizer.log.914", MSAN_UAF_REPORT),),
        fuzzer_log=(
            "DB::Exception: Found disallowed error code 42 while executing query\n"
        ),
        check_name="BuzzHouse (amd_msan)",
    )
    assert result.status == Result.Status.ERROR
    assert "Found disallowed error code" in result.info
    # The catch-all branch's note must not appear here - keying it on `status`
    # would have rewritten this branch's info too.
    assert "see the named sub-result(s)" not in result.info
    assert any("use-of-uninitialized-value" in n for n in _named_sanitizer(result))


# 9. An unrecognizable report is not a finding, and never crashes the job.
@pytest.mark.parametrize(
    "text", ["", "\x00\x01 garbage bytes\n", "==914==WARNING: MemorySanit"]
)
def test_unrecognizable_report_is_not_a_finding(run_job, text):
    result = run_job(status_tsv="0\t0\t0", reports=(("sanitizer.log.9", text),))
    assert result.status == Result.Status.OK
    assert _names(result) == []


# 10. A benign report with a LATER mtime must not shadow a real one.
def test_benign_newer_report_does_not_shadow_a_real_one(run_job):
    result = run_job(
        status_tsv="0\t0\t134",
        reports=(
            ("sanitizer.log.100", MSAN_UAF_REPORT),
            ("sanitizer.log.200", BENIGN_ALLOC_REPORT),
        ),
    )
    newer = run_job.workspace / "sanitizer.log.200"
    older = run_job.workspace / "sanitizer.log.100"
    assert newer.stat().st_mtime >= older.stat().st_mtime
    assert any(
        "use-of-uninitialized-value" in n for n in _named_sanitizer(result)
    ), _names(result)
    assert result.status != Result.Status.OK


# 11. Benign-ness must not be read off the parser's normalized NAME, from which
#     "failed to allocate" has been stripped.
def test_benign_name_normalization_does_not_promote_a_report(run_job):
    result = run_job(
        status_tsv="1\t0\t0", reports=(("sanitizer.log.501", FATAL_OOM_REPORT),)
    )
    from ci.jobs.scripts.log_parser import FuzzerLogParser

    report = run_job.workspace / "sanitizer.log.501"
    assert report.exists(), "post-container state must survive"
    name, _info, _files = FuzzerLogParser(
        server_log=str(report), stderr_log=str(report)
    ).parse_failure()
    # Prove the premise: the name the OOM filter must NOT be applied to has lost
    # every trace of the OOM wording, so a name-based test cannot fire.
    assert "failed to allocate" not in name
    assert "out-of-memory" not in name
    assert "MemorySanitizer" in name
    assert result.status == Result.Status.OK
    assert _named_sanitizer(result) == []


# 12. The parser must never see the real server.log, or it names the shutdown.
def test_normal_shutdown_is_never_named(run_job):
    result = run_job(
        status_tsv="0\t0\t134",
        reports=(("sanitizer.log.9", "nothing recognizable in here\n"),),
        server_log=SERVER_LOG_SHUTDOWN,
    )
    assert result.results == []
    assert not any("Received signal" in n for n in _names(result))


# 13. One file, benign record followed by a real one: the parser selects the real
#     one, so no record splitting is needed.
def test_same_file_benign_then_real_is_named(run_job):
    result = run_job(
        status_tsv="0\t0\t134",
        reports=(("sanitizer.log.500", BENIGN_ALLOC_REPORT + "\n" + MSAN_UAF_REPORT),),
    )
    assert any(
        "use-of-uninitialized-value" in n for n in _named_sanitizer(result)
    ), _names(result)
    assert result.status != Result.Status.OK


# 14. A truncated report: named when the diagnostic survived, skipped when the
#     surviving text is the benign allocation warning.
def test_truncated_real_report_is_still_named(run_job):
    truncated = MSAN_UAF_REPORT.split("SUMMARY:")[0]
    result = run_job(status_tsv="0\t0\t134", reports=(("sanitizer.log.1", truncated),))
    assert any("use-of-uninitialized-value" in n for n in _named_sanitizer(result))
    assert result.status != Result.Status.OK


def test_truncated_benign_report_stays_ok(run_job):
    truncated = BENIGN_ALLOC_REPORT.rstrip("\n")[: -len(" 0x100000000 bytes")]
    assert truncated.endswith("failed to allocate")
    result = run_job(status_tsv="0\t0\t0", reports=(("sanitizer.log.2", truncated),))
    assert result.status == Result.Status.OK
    assert _named_sanitizer(result) == []


# 15. A glued ASan-benign + headerless-UBSan file is still named.
def test_glued_benign_asan_then_headerless_ubsan_is_named(run_job):
    glued = (
        "==800==WARNING: AddressSanitizer failed to allocate 0x100000000 bytes\n\n"
        + UBSAN_HEADERLESS_REPORT
    )
    result = run_job(
        status_tsv="0\t0\t134",
        reports=(("sanitizer.log.800", glued),),
        check_name="AST fuzzer (arm_asan_ubsan)",
    )
    named = _named_sanitizer(result)
    assert named and "STID:" in named[0], _names(result)
    assert result.status != Result.Status.OK


# 16. A report with a CLEAN fuzzer exit: *SAN_OPTIONS reaches the server and the
#     ignored client probes, so the fuzzer can exit 0/143 while a runtime reported.
@pytest.mark.parametrize("exit_code", ["0", "137", "143"])
def test_report_with_clean_fuzzer_exit_is_still_red(run_job, exit_code):
    result = run_job(
        status_tsv=f"0\t0\t{exit_code}",
        reports=(("sanitizer.log.914", MSAN_UAF_REPORT),),
    )
    assert result.status != Result.Status.OK
    assert any("use-of-uninitialized-value" in n for n in _named_sanitizer(result))
    # The run must also be marked FAILED, not just reported red: is_failed is what
    # uploads the logs and the report itself as artifacts. A red verdict derived
    # only from the sub-result would name the finding while attaching nothing to
    # investigate it with.
    attached = {os.path.basename(str(f)) for f in result.files}
    assert "sanitizer.log.914" in attached, attached
    assert "server.log" in attached, attached


# 17. An OOM record ordered FIRST in the merged log must not be the only name -
#     and must not add a name of its own. The merged-log parse selects whichever
#     record the runner concatenated first; a benign selection there would emit a
#     FAIL sub-result, hence a CIDB test_name row, for something already
#     classified as benign, and the name-based dedupe cannot remove it because a
#     benign record normalizes to a DIFFERENT name (the bare tool name).
@pytest.mark.parametrize(
    "benign_report", [FATAL_OOM_REPORT, BENIGN_ALLOC_REPORT], ids=["fatal", "warning"]
)
def test_oom_record_first_does_not_hide_the_real_finding(run_job, benign_report):
    result = run_job(
        status_tsv="1\t0\t0",
        reports=(
            ("sanitizer.log.501", benign_report),
            ("sanitizer.log.914", MSAN_UAF_REPORT),
        ),
    )
    named = _named_sanitizer(result)
    real = [n for n in named if "use-of-uninitialized-value" in n]
    assert real, _names(result)
    assert len(real) == 1, f"finding reported more than once: {named}"
    # EXACT set: no extra sanitizer row for the benign record.
    assert set(named) == set(real), f"benign record named too: {named}"
    assert result.status != Result.Status.OK


# 18. A stale success tuple in status.tsv would short-circuit everything.
def test_stale_status_tsv_is_removed(run_job):
    result = run_job(
        status_tsv=None,  # this run writes none
        reports=(),
        stale=(("status.tsv", "0\t0\t0\n"), ("sanitizer.log.914", MSAN_UAF_REPORT)),
    )
    # With the stale tuple gone the run is an early abort, not a clean success.
    assert result.status == Result.Status.ERROR
    assert "aborted before writing status.tsv" in result.info


def test_clean_stale_run_state_fails_closed(run_job, monkeypatch, tmp_path):
    survivor = run_job.workspace / "status.tsv"
    survivor.write_text("0\t0\t0\n")
    monkeypatch.delenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE", raising=False)
    monkeypatch.setattr(
        job.Path, "unlink", lambda self, *a, **kw: None, raising=False
    )
    with pytest.raises(RuntimeError, match="stale classification inputs"):
        job._clean_stale_run_state()


def test_clean_stale_run_state_keeps_preseeded_input(run_job, monkeypatch):
    seeded = run_job.workspace / "status.tsv"
    seeded.write_text("1\t0\t0\n")
    monkeypatch.setenv("PRAKTIKA_FUZZER_KEEP_PRESEEDED_STATE", "1")
    job._clean_stale_run_state()
    assert seeded.exists()


# 19. Missing / malformed status.tsv: that path ends in `complete_job`, which
#     calls `sys.exit`, so the finding has to be named BEFORE it.
@pytest.mark.parametrize("status_tsv", [None, "not-a-status-line"])
def test_early_abort_still_names_the_report(run_job, status_tsv):
    result = run_job(
        status_tsv=status_tsv, reports=(("sanitizer.log.914", MSAN_UAF_REPORT),)
    )
    assert result.status == Result.Status.ERROR
    named = _named_sanitizer(result)
    assert len(named) == 1, _names(result)
    assert "use-of-uninitialized-value" in named[0]
    assert any("sanitizer.log.914" in str(f) for f in result.files), result.files


# ---------------------------------------------------------------------------
# 20. A report the parser cannot classify must fail CLOSED. Swallowing the parse
#     error would leave the run green on an unexamined report, which is exactly
#     the hidden-finding outcome this classification exists to prevent. There are
#     three distinct ways it happens and each needs its own case: the parser
#     raises, the report cannot be READ, and a tool the parser shells out to is
#     missing from PATH. Only the first raises; the other two return
#     UNKNOWN_ERROR, indistinguishably from a genuinely unrecognizable report.
def _assert_parse_failure_is_reported(result, report_name):
    assert result.status != Result.Status.OK, result.info
    named = _names(result)
    assert job.SANITIZER_PARSE_FAILURE_NAME in named, named
    sub = next(r for r in result.results if r.name == job.SANITIZER_PARSE_FAILURE_NAME)
    assert sub.status == Result.Status.FAIL
    assert report_name in sub.info, sub.info
    assert any(report_name in str(f) for f in sub.files), sub.files
    # The run must also be marked FAILED, not merely reported red. A red verdict
    # is DERIVED from the FAIL sub-result by `create_from` regardless, so the
    # status cannot witness the is_failed force - the ARTIFACT UPLOAD is what it
    # actually gates, and a named failure with nothing attached is not
    # investigable.
    attached = {os.path.basename(str(f)) for f in result.files}
    assert report_name in attached, attached
    assert "server.log" in attached, attached


def test_parser_exception_does_not_leave_the_run_green(run_job, monkeypatch):
    def _raise(self):
        raise RuntimeError("command failed with, exit_code 127")

    monkeypatch.setattr(job.FuzzerLogParser, "parse_failure", _raise)
    result = run_job(
        status_tsv="0\t0\t0", reports=(("sanitizer.log.914", MSAN_UAF_REPORT),)
    )
    _assert_parse_failure_is_reported(result, "sanitizer.log.914")


def _deny_read(monkeypatch, report_name):
    """Emulate a report the job cannot read, without using permission bits.

    A file the job cannot read is unreadable to BOTH consumers, so both facets
    have to be emulated: the parser shells out to `rg`, gets nothing and returns
    UNKNOWN_ERROR (measured: a chmod-000 report parses to exactly that, no
    raise), and the production `os.access(report, os.R_OK)` check is what tells
    that apart from a readable report holding no diagnostic. This patches the
    predicate; the callers seed the report body EMPTY, which is what the parser
    sees for an unreadable file. Patching only the predicate would leave a
    readable real report, which the parser names, so the branch under test would
    never be reached.

    Permission bits are deliberately not used: the `CI Tests` job that runs this
    file runs as root (`ci/defs/job_configs.py` marks it `+root+`, so
    `ci/praktika/runner.py` omits `--user`), and root bypasses file permissions,
    so a chmod-based fixture would have to skip in CI - leaving this fail-closed
    behavior with no coverage on the only job that runs the file.

    The production check stays `os.access`, the honest observable property.
    Everything other than the named report is delegated to the real `os.access`.
    """
    real_access = os.access

    def _access(path, mode, *args, **kwargs):
        if mode == os.R_OK and os.path.basename(str(path)) == report_name:
            return False
        return real_access(path, mode, *args, **kwargs)

    monkeypatch.setattr(job.os, "access", _access)


@pytest.mark.parametrize("status_tsv", ["0\t0\t0", "0\t0\t143"])
def test_unreadable_report_does_not_leave_the_run_green(
    run_job, monkeypatch, status_tsv
):
    # The container runs as root and the reports are chown'd afterwards, so an
    # unreadable report is a plausible CI state, not a synthetic one.
    _deny_read(monkeypatch, "sanitizer.log.914")
    result = run_job(
        status_tsv=status_tsv,
        reports=(("sanitizer.log.914", UNREADABLE_REPORT_BODY),),
    )
    _assert_parse_failure_is_reported(result, "sanitizer.log.914")


# 21. An unclassifiable report also vetoes the sanitized-build OOM leniency: it
#     may BE the real finding the leniency would forgive.
def test_unreadable_report_vetoes_the_oom_leniency(run_job, monkeypatch):
    _deny_read(monkeypatch, "sanitizer.log.914")
    result = run_job(
        status_tsv="1\t0\t0",
        reports=(
            ("sanitizer.log.500", BENIGN_ALLOC_REPORT),
            ("sanitizer.log.914", UNREADABLE_REPORT_BODY),
        ),
    )
    assert result.status != Result.Status.OK
    assert "considered passed" not in result.info
    _assert_parse_failure_is_reported(result, "sanitizer.log.914")


# 21b. A missing parser tool is the third way a report goes unclassified, and the
#      only one that raises nothing: `parse_failure` runs `rg ... | head -n10`
#      without pipefail, so with `rg` absent the pipeline still exits 0 and the
#      parser returns UNKNOWN_ERROR for a perfectly readable real report. Left
#      unhandled, that is the hidden-report outcome this classification exists to
#      prevent, reached through a second door.
def test_missing_parser_tool_does_not_leave_the_run_green(
    run_job, monkeypatch, tmp_path
):
    # PATH is restricted for real rather than `shutil.which` being patched, so
    # this drives the actual mechanism: the parser's own `rg` invocation fails,
    # `head` still exits 0 for the pipeline, and the report - readable, real, and
    # holding a use-after-free - parses to UNKNOWN_ERROR without raising.
    fake_bin = tmp_path / "fake-bin"
    fake_bin.mkdir()
    for tool in ("bash", "sh", "head", "tail", "cat", "grep", "sed", "dmesg", "zstd"):
        found = shutil.which(tool)
        if found:
            (fake_bin / tool).symlink_to(found)
    assert shutil.which("head", path=str(fake_bin)), "fixture needs head on PATH"
    assert not shutil.which("rg", path=str(fake_bin)), "fixture must hide rg"
    monkeypatch.setenv("PATH", str(fake_bin))
    result = run_job(
        status_tsv="0\t0\t0", reports=(("sanitizer.log.914", MSAN_UAF_REPORT),)
    )
    _assert_parse_failure_is_reported(result, "sanitizer.log.914")
    sub = next(
        r for r in result.results if r.name == job.SANITIZER_PARSE_FAILURE_NAME
    )
    # The missing tool is named, so the CIDB row is diagnosable rather than an
    # opaque "could not classify".
    assert "rg" in sub.info, sub.info
    assert "not available" in sub.info, sub.info


# 22. The watchdog's "terminated by signal 9" matches the same OOM pattern but is
#     NOT a sanitizer record: no sanitizer.log.* holds it, so the per-report pass
#     cannot name it and the merged-log filter must not drop it - otherwise a dead
#     server would produce no named row at all. Scoping the filter to sanitizer
#     records is what keeps this row. A NON-sanitized build is used because on a
#     sanitized one that same grep triggers the OOM leniency before the merged
#     parse is ever reached (pre-existing behavior, unchanged here).
def test_signal_9_row_survives_the_merged_log_filter(run_job):
    result = run_job(
        status_tsv="1\t0\t0",
        reports=(),
        server_log="2026.07.21 10:00:00.000000 [ 1 ] {} <Fatal> Application: Child process was terminated by signal 9\n",
        check_name="AST fuzzer (amd_debug)",
    )
    assert any("signal 9" in n for n in _names(result)), _names(result)
    # The non-sanitized branch really ran: the fixture seeded dmesg.log and the
    # grep over it was executed for real, so the branch is exercised rather than
    # mocked out of existence.
    assert (run_job.workspace / "dmesg.log").exists()
    assert any(c.startswith("dmesg ") for c in run_job.shelled), run_job.shelled
    assert any(
        "dmesg.log | grep -a -e 'Out of memory" in c for c in run_job.shelled
    ), run_job.shelled


# 23. The merged-log filter is scoped to SANITIZER records, asserted directly on
#     the predicate. `SANITIZER_OOM_PATTERN` has a non-sanitizer alternative (the
#     watchdog's "terminated by signal 9"), which no sanitizer.log.* holds and the
#     per-report pass therefore cannot name - so an UNSCOPED filter would drop the
#     only row a kernel-OOM-killed server produces. Pinned here rather than end to
#     end because for the log-prefixed spelling `parse_failure` truncates the
#     diagnostic to empty (its own bound, not touched by this change), so no
#     end-to-end fixture can currently distinguish the two variants.
@pytest.mark.parametrize(
    "name,info,expected",
    [
        # Non-sanitizer record: must keep its row.
        (
            "Child process was terminated by signal 9 (KILL) (STID: abcd-1234)",
            "Error:\nChild process was terminated by signal 9 (KILL)\n",
            False,
        ),
        # Benign sanitizer record: the per-report pass covers it, so drop the row.
        (
            "MemorySanitizer (STID: abcd-1234)",
            "Error:\n" + BENIGN_ALLOC_REPORT,
            True,
        ),
        # Real finding: never dropped.
        (
            "MemorySanitizer: use-of-uninitialized-value (STID: abcd-1234)",
            "Error:\n==914==WARNING: MemorySanitizer: use-of-uninitialized-value\n",
            False,
        ),
    ],
)
def test_merged_log_filter_is_scoped_to_sanitizer_records(name, info, expected):
    assert job._merged_row_is_benign(name, info) is expected


# ---------------------------------------------------------------------------
# The OOM pattern is shared by the leniency grep and the benign-ness filter, so
# the two can never diverge.
def test_oom_pattern_is_a_single_definition():
    import inspect
    import re

    src = inspect.getsource(job.run_fuzz_job)
    assert "SANITIZER_OOM_PATTERN" in src
    assert "out-of-memory" not in src, "the OOM regex is inlined again in run_fuzz_job"
    assert re.search(job.SANITIZER_OOM_PATTERN, BENIGN_ALLOC_REPORT)
    assert not re.search(job.SANITIZER_OOM_PATTERN, MSAN_UAF_REPORT)
