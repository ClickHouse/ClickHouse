#!/usr/bin/env python3
import logging
import os
import subprocess
import sys
import traceback
from pathlib import Path

from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.log_parser import FuzzerLogParser
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_NAME = "clickhouse/fuzzer"

# Maximum number of reproduce commands to display inline before writing to file
MAX_INLINE_REPRODUCE_COMMANDS = 20

cwd = Utils.cwd()


def get_run_command(
    workspace_path: Path,
    image: DockerImage,
    buzzhouse: bool,
) -> str:
    envs = [
        f"-e FUZZER_TO_RUN='{'BuzzHouse' if buzzhouse else 'AST Fuzzer'}'",
    ]

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        "--tmpfs /tmp/clickhouse "
        f"--volume={workspace_path}:/workspace "
        f"--volume={cwd}:/repo "
        f"{env_str} "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE --workdir /repo "
        f"{image} "
        "bash -c './ci/jobs/scripts/fuzzer/run-fuzzer.sh' "
    )


def run_fuzz_job(check_name: str):
    logging.basicConfig(level=logging.INFO)
    buzzhouse: bool = check_name.lower().startswith("buzzhouse")

    temp_dir = Path(f"{cwd}/ci/tmp/")
    assert Path(f"{temp_dir}/clickhouse").exists(), "ClickHouse binary not found"

    docker_image = DockerImage.get_docker_image(IMAGE_NAME).pull_image()

    workspace_path = temp_dir / "workspace"
    workspace_path.mkdir(parents=True, exist_ok=True)

    run_command = get_run_command(workspace_path, docker_image, buzzhouse)
    logging.info("Going to run %s", run_command)

    info = Info()
    is_sanitized = "san" in info.job_name

    with open(workspace_path / "ci-changed-files.txt", "w") as f:
        f.write("\n".join(info.get_changed_files()))

    Shell.check(command=run_command, verbose=True)
    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_dir}", shell=True)

    fuzzer_log = workspace_path / "fuzzer.log"
    dmesg_log = workspace_path / "dmesg.log"
    fatal_log = workspace_path / "fatal.log"
    server_log = workspace_path / "server.log"
    paths = [
        workspace_path / "core.zst",
        workspace_path / "dmesg.log",
        fatal_log,
        workspace_path / "stderr.log",
        server_log,
        fuzzer_log,
        dmesg_log,
    ]
    if buzzhouse:
        paths.extend([workspace_path / "fuzzerout.sql", workspace_path / "fuzz.json"])

    server_died = False
    server_exit_code = 0
    fuzzer_exit_code = 0
    try:
        with open(workspace_path / "status.tsv", "r", encoding="utf-8") as status_f:
            server_died, server_exit_code, fuzzer_exit_code = (
                status_f.readline().rstrip("\n").split("\t")
            )
            server_died = bool(int(server_died))
            server_exit_code = int(server_exit_code)
            fuzzer_exit_code = int(fuzzer_exit_code)
    except Exception:
        result.set_status(Result.Status.ERROR)
        result.set_info("Unknown error in fuzzer runner script")
        result.complete_job()
        sys.exit(1)

    # parse runner script exit status
    status = Result.StatusExtended.OK
    result_name = ""
    info = ""
    is_failure_found = False
    if server_died:
        is_failure_found = True
        status = Result.StatusExtended.FAIL
        if is_sanitized:
            sanitizer_oom = Shell.get_output(
                f"rg --text 'Sanitizer:? (out-of-memory|out of memory|failed to allocate)|Child process was terminated by signal 9' {server_log}"
            )
            if sanitizer_oom:
                print("Sanitizer OOM")
                status = Result.StatusExtended.OK
    elif fuzzer_exit_code in (0, 143):
        # Variants of a normal run:
        # 0   -- fuzzing ended earlier than timeout.
        # 143 -- SIGTERM -- the fuzzer was killed by timeout.
        status = Result.StatusExtended.OK
    elif fuzzer_exit_code in (227,):
        # BuzzHouse exception, it means a query oracle failed, or
        # an unwanted exception was found
        status = Result.StatusExtended.ERROR
        result_name = (
            Shell.get_output(
                f"rg --text -o 'DB::Exception: Found disallowed error code.*' {fuzzer_log}"
            )
            or "BuzzHouse fuzzer exception not found, fuzzer issue?"
        )
    else:
        if fuzzer_exit_code == 137:
            # Killed.
            status = Result.StatusExtended.ERROR
            result_name = "Killed"
        else:
            # The server was alive, but the fuzzer returned some error. This might
            # be some client-side error detected by fuzzing, or a problem in the
            # fuzzer itself. Don't grep the server log in this case, because we will
            # find a message about normal server termination (Received signal 15),
            # which is confusing.
            status = Result.StatusExtended.ERROR
            result_name = "Client failure (see logs)"
        info += "---\n\nFuzzer log (last 200 lines):\n"
        info += Shell.get_output(f"tail -n200 {fuzzer_log}", verbose=False) + "\n"

    # parse failre from logs
    results = []
    if is_failure_found and status == Result.StatusExtended.FAIL:
        fuzzer_log_parser = FuzzerLogParser(
            str(server_log),
            str(workspace_path / "fuzzerout.sql" if buzzhouse else fuzzer_log),
        )

        result_name, info = fuzzer_log_parser.parse_failure()

        if result_name and status != Result.StatusExtended.OK:
            results.append(Result(name=result_name, info=info, status=status))

        if Shell.check(f"dmesg > {dmesg_log}"):
            oom_result = Result.from_commands_run(
                name="OOM in dmesg",
                command=f"! cat {dmesg_log} | grep -a -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' | tee /dev/stderr | grep -q .",
            )
            if not oom_result.is_ok():
                # change status: failure -> FAIL
                oom_result.set_status(Result.StatusExtended.FAIL)
                results.append(oom_result)
        else:
            print("WARNING: dmesg not enabled")

    result = Result.create_from(results=results, status=True if not results else None)

    if not result.is_ok():
        for file in paths:
            if file.exists():
                result.set_files(file)

    result.complete_job()


if __name__ == "__main__":
    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    run_fuzz_job(check_name)
