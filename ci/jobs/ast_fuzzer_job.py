#!/usr/bin/env python3
import logging
import os
import random
import subprocess
import sys
import traceback
from pathlib import Path

from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_NAME = "clickhouse/fuzzer"

# Maximum number of reproduce commands to display inline before writing to file
MAX_INLINE_REPRODUCE_COMMANDS = 20

cwd = Utils.cwd()
WORKSPACE_PATH = Path(cwd) / "ci/tmp/workspace"

# Paths of artifacts produced by the fuzzer runner script.
# Exported so tests can pre-seed them without duplicating the paths.
JOB_ARTIFACTS = (
    WORKSPACE_PATH / "server.log",
    WORKSPACE_PATH / "fuzzer.log",
    WORKSPACE_PATH / "stderr.log",
    WORKSPACE_PATH / "dmesg.log",
    WORKSPACE_PATH / "fatal.log",
)


def get_run_command(
    image: DockerImage,
    buzzhouse: bool,
    targeted_queries_file: Path | None = None,
    compatibility_setting: str | None = None,
    enable_server_fuzzer: bool = False,
) -> str:
    from ci.jobs.ci_utils import is_extended_run

    minutes = 60 if is_extended_run() else 30
    envs = [
        f"-e FUZZER_TO_RUN='{'BuzzHouse' if buzzhouse else 'AST Fuzzer'}'",
        f"-e FUZZ_TIME_LIMIT='{minutes}m'",
    ]
    if enable_server_fuzzer:
        envs.append("-e SERVER_FUZZER_ENABLED=1")
    if targeted_queries_file:
        container_queries_file = f"/workspace/{targeted_queries_file.name}"
        envs.append(f"-e TARGETED_QUERIES_FILE='{container_queries_file}'")
    if compatibility_setting:
        envs.append(f"-e FUZZER_COMPATIBILITY='{compatibility_setting}'")

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        "--tmpfs /tmp/clickhouse:mode=1777 "
        f"--volume={WORKSPACE_PATH}:/workspace "
        f"--volume={cwd}:/repo "
        f"{env_str} "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE --workdir /repo "
        f"{image} "
        "bash -c './ci/jobs/scripts/fuzzer/run-fuzzer.sh' "
    )


def _collect_targeted_queries(info: Info) -> tuple[list[str], Result]:
    targeter = Targeting(info=info)
    targeter.job_type = Targeting.STATELESS_JOB_TYPE

    # Step 1: changed/new test files in this PR
    changed_tests = targeter.get_changed_tests()
    logging.info("[targeted-fuzzer] Step 1 — changed/new tests (%d): %s",
                 len(changed_tests), ", ".join(sorted(changed_tests)) or "(none)")

    # Step 2: tests that failed in previous CI runs for this PR
    try:
        previously_failed = targeter.get_previously_failed_tests()
    except Exception as e:
        logging.warning("[targeted-fuzzer] Step 2 — failed to fetch previously-failed tests: %s", e)
        previously_failed = []
    logging.info("[targeted-fuzzer] Step 2 — previously failed tests (%d): %s",
                 len(previously_failed), ", ".join(previously_failed) or "(none)")

    # Step 3: coverage-relevant tests (direct lines, indirect callees, siblings)
    try:
        relevant_tests, relevant_tests_result = targeter.get_most_relevant_tests()
    except Exception as e:
        logging.warning("[targeted-fuzzer] Step 3 — failed to fetch coverage-relevant tests: %s", e)
        relevant_tests = []
        relevant_tests_result = Result(name="tests found by coverage", status=Result.Status.OK, info=f"Skipped: {e}")
    logging.info("[targeted-fuzzer] Step 3 — coverage-relevant tests (%d)", len(relevant_tests))

    # Merge all three sets preserving priority order (changed first)
    seen: set = set()
    tests: list = []
    for t in list(changed_tests) + list(previously_failed) + list(relevant_tests):
        if t not in seen:
            seen.add(t)
            tests.append(t)
    logging.info("[targeted-fuzzer] Total unique tests: %d", len(tests))

    stateless_tests_dir = Path(cwd) / "tests/queries/0_stateless"
    available_queries: dict[str, list[str]] = {}

    for query_file in stateless_tests_dir.rglob("*.sql"):
        base_name = query_file.stem
        available_queries.setdefault(base_name, []).append(
            f"/repo/{query_file.relative_to(cwd)}"
        )

    logging.debug("Indexed %d unique SQL query base names from %s", len(available_queries), stateless_tests_dir)

    targeted_queries: list[str] = []
    seen_queries = set()
    for test in tests:
        base_name = Path(test).stem.rstrip(".")
        matches = available_queries.get(base_name, [])
        if matches:
            logging.debug("  %s -> %s", test, matches)
        else:
            logging.debug("  %s -> no .sql file found (stem: %r)", test, base_name)
        for query_path in matches:
            if query_path not in seen_queries:
                seen_queries.add(query_path)
                targeted_queries.append(query_path)

    if targeted_queries:
        targeted_queries_file = WORKSPACE_PATH / "ci-targeted-queries.txt"
        with open(targeted_queries_file, "w", encoding="utf-8") as f:
            f.write("\n".join(targeted_queries))
        logging.info(
            "Prepared %d targeted queries for AST fuzzer:", len(targeted_queries)
        )
        for qf in targeted_queries:
            logging.info("  %s", qf)
    else:
        logging.info("No targeted queries resolved for AST fuzzer")

    return targeted_queries, relevant_tests_result


def run_fuzz_job(check_name: str):
    logging.basicConfig(level=logging.INFO)
    is_targeted = "targeted" in check_name.lower()
    buzzhouse: bool = check_name.lower().startswith("buzzhouse")

    clickhouse_binary = Path(cwd) / "ci/tmp/clickhouse"
    assert clickhouse_binary.exists(), "ClickHouse binary not found"
    clickhouse_binary.chmod(clickhouse_binary.stat().st_mode | 0o111)

    docker_image = DockerImage.get_docker_image(IMAGE_NAME).pull_image()

    WORKSPACE_PATH.mkdir(parents=True, exist_ok=True)

    info = Info()
    job_name = info.job_name
    extra_results = []
    targeted_queries_file: Path | None = None

    if is_targeted and not buzzhouse:
        targeted_queries, relevant_tests_result = _collect_targeted_queries(info=info)
        extra_results.append(relevant_tests_result)
        if not targeted_queries:
            Result.create_from(
                status=Result.Status.SKIPPED,
                info="No relevant tests found for targeted AST fuzzer",
                results=extra_results,
            ).complete_job()
        targeted_queries_file = WORKSPACE_PATH / "ci-targeted-queries.txt"

    is_old_compatibility = "old_compatibility" in check_name.lower()
    compatibility_setting: str | None = None
    if not buzzhouse:
        if is_old_compatibility:
            # The minimum version is 24.3 because that's when enable_analyzer
            # became enabled by default, and the fuzzer has a readonly constraint
            # on enable_analyzer to avoid wasting cycles on the old interpreter.
            compatibility_setting = "24.3"
        elif is_targeted:
            compatibility_setting = None
        else:
            compatibility_setting = (
                f"{random.randint(24, 27)}.{random.randint(1, 12)}"
            )
        if compatibility_setting:
            logging.info("AST fuzzer compatibility setting: %s", compatibility_setting)
        else:
            logging.info("AST fuzzer compatibility setting is not set")

    run_command = get_run_command(
        docker_image,
        buzzhouse,
        targeted_queries_file=targeted_queries_file,
        compatibility_setting=compatibility_setting,
        enable_server_fuzzer="serverfuzz" in job_name,
    )
    logging.info("Going to run %s", run_command)

    is_sanitized = "san" in info.job_name

    changed_files_path = WORKSPACE_PATH / "ci-changed-files.txt"
    with open(changed_files_path, "w") as f:
        changed_files = info.get_changed_files()
        if changed_files is None:
            if info.is_local_run:
                logging.warning(
                    "No changed files available for local run - fuzzing will not be guided by changed test cases"
                )
            changed_files = []
        else:
            logging.info("Found %d changed files to guide fuzzing", len(changed_files))
        f.write("\n".join(changed_files))

    Shell.check(command=run_command, verbose=True)

    # Fix file ownership after running docker as root
    logging.info("Fuzzer: Fixing file ownership after running docker as root")
    Utils.fix_ownership_after_docker(cwd, docker_image)

    server_log, fuzzer_log, stderr_log, dmesg_log, fatal_log = JOB_ARTIFACTS
    paths = list(JOB_ARTIFACTS)

    if buzzhouse:
        paths.extend([WORKSPACE_PATH / "fuzzerout.sql", WORKSPACE_PATH / "fuzz.json"])

    server_died = False
    server_exit_code = 0
    fuzzer_exit_code = 0
    try:
        with open(WORKSPACE_PATH / "status.tsv", "r", encoding="utf-8") as status_f:
            server_died, server_exit_code, fuzzer_exit_code = (
                status_f.readline().rstrip("\n").split("\t")
            )
            server_died = bool(int(server_died))
            server_exit_code = int(server_exit_code)
            fuzzer_exit_code = int(fuzzer_exit_code)
    except Exception:
        error_info = f"Unknown error in fuzzer runner script. Traceback:\n{traceback.format_exc()}"
        Result.create_from(status=Result.Status.ERROR, info=error_info).complete_job()

    # parse runner script exit status
    status = Result.Status.FAIL
    info = []
    is_failed = True
    if server_died:
        # Server died - status will be determined after OOM checks
        is_failed = True
    elif fuzzer_exit_code in (0, 137, 143):
        # normal exit with timeout or OOM kill
        is_failed = False
        status = Result.Status.OK
        if fuzzer_exit_code == 0:
            info.append("Fuzzer exited with success")
        elif fuzzer_exit_code == 137:
            info.append("Fuzzer killed")
        else:
            info.append("Fuzzer exited with timeout")
        info.append("\n")
    elif fuzzer_exit_code in (227,):
        # BuzzHouse exception, it means a query oracle failed, or
        # an unwanted exception was found
        status = Result.Status.ERROR
        error_info = (
            Shell.get_output(
                f"rg --text -o 'DB::Exception: Found disallowed error code.*' {fuzzer_log}"
            )
            or "BuzzHouse fuzzer exception not found, fuzzer issue?"
        )
        info.append(f"ERROR: {error_info}")
    else:
        status = Result.Status.ERROR
        # The server was alive, but the fuzzer returned some error. This might
        # be some client-side error detected by fuzzing, or a problem in the
        # fuzzer itself. Don't grep the server log in this case, because we will
        # find a message about normal server termination (Received signal 15),
        # which is confusing.
        info.append("Client failure (see logs)")
        info.append("---\nFuzzer log (last 200 lines):")
        info.extend(
            Shell.get_output(f"tail -n200 {fuzzer_log}", verbose=False).splitlines()
        )

    if is_failed:
        if is_sanitized:
            sanitizer_oom = Shell.get_output(
                f"rg --text 'Sanitizer:? (out-of-memory|out of memory|failed to allocate)|Child process was terminated by signal 9' {server_log}"
            )
            if sanitizer_oom:
                print("Sanitizer OOM")
                info.append("WARNING: Sanitizer OOM - test considered passed")
                status = Result.Status.OK
                is_failed = False
        else:
            # Check for OOM in dmesg for non-sanitized builds
            if Shell.check(f"dmesg > {dmesg_log}", verbose=True):
                if Shell.check(
                    f"cat {dmesg_log} | grep -a -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' | tee /dev/stderr | grep -q .",
                    verbose=True,
                ):
                    info.append("ERROR: OOM in dmesg")
                    status = Result.Status.ERROR
            else:
                print("WARNING: dmesg not enabled")

    results = []
    if is_failed and status != Result.Status.ERROR:
        # died server - lets fetch failure from log
        fuzzer_log_parser = FuzzerLogParser(
            server_log=str(server_log),
            stderr_log=str(stderr_log),
            fuzzer_log=str(
                WORKSPACE_PATH / "fuzzerout.sql" if buzzhouse else fuzzer_log
            ),
        )
        parsed_name, parsed_info, files = fuzzer_log_parser.parse_failure()

        if parsed_name:
            results.append(
                Result(
                    name=parsed_name,
                    info=parsed_info,
                    status=Result.Status.FAIL,
                    files=files,
                )
            )

    result = Result.create_from(
        results=extra_results + results,
        status=status if not results else None,
        info=info,
    )

    if is_failed:
        # generate fatal log
        Shell.check(f"rg --text '\\s<Fatal>\\s' {server_log} > {fatal_log}")
        result.set_files(ClickHouseService.collect_cores(WORKSPACE_PATH))
        for file in paths:
            if file.exists() and file.stat().st_size > 0:
                result.set_files(file)

    result.complete_job()


if __name__ == "__main__":
    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    run_fuzz_job(check_name)
