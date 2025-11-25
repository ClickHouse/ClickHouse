#!/usr/bin/env python3
import logging
import os
import subprocess
import sys
import traceback
from pathlib import Path

from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.generate_test import FuzzerTestGenerator
from ci.jobs.scripts.stack_trace_reader import StackTraceReader
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

    with open(workspace_path / "ci-changed-files.txt", "w") as f:
        f.write("\n".join(info.get_changed_files()))

    Shell.check(command=run_command, verbose=True)
    result = Result.from_fs(name=info.job_name)
    result.status = Result.Status.SUCCESS
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

    try:
        with open(workspace_path / "status.txt", "r", encoding="utf-8") as status_f:
            status = status_f.readline().rstrip("\n")

        with open(workspace_path / "description.txt", "r", encoding="utf-8") as desc_f:
            description = desc_f.readline().rstrip("\n")
        result_ = Result(name=description, status=status)
        if not result_.is_ok():
            result.results = [result_]
            result.set_status(Result.Status.FAILED)
    except Exception:
        result.set_status(Result.Status.ERROR)
        result.results = [
            Result(name="Unknown error", status=Result.StatusExtended.ERROR)
        ]

    if not result.is_ok():
        info = ""
        is_assertion = False
        error_output = Shell.get_output(
            f"rg --text -A 10 -o 'Logical error.*|Assertion.*failed|Failed assertion.*|.*runtime error: .*|.*is located.*|(SUMMARY|ERROR|WARNING): [a-zA-Z]+Sanitizer:.*|.*_LIBCPP_ASSERT.*|Received signal.*|.*Child process was terminated by signal 9.*' {server_log} | head -n10"
        )
        if error_output:
            is_assertion = True
        else:
            error_output = Shell.get_output(
                f"rg --text -A 10 -o 'Received signal.*|.*Child process was terminated by signal 9.*' {server_log} | head -n10"
            )
        if error_output:
            error_lines = error_output.splitlines()
            # keep all lines before next log line
            for i, line in enumerate(error_lines):
                if "] {" in line and "} <" in line:
                    error_lines = error_lines[:i]
                    break
            error_output = "\n".join(error_lines)
            info += f"Error:\n{error_output}\n"

        patterns = [
            "BuzzHouse fuzzer exception",
            "Killed",
            "Let op!",
            "Unknown error",
        ]
        if result.results and any(
            pattern in result.results[-1].name for pattern in patterns
        ):
            info += "---\n\nFuzzer log (last 200 lines):\n"
            info += Shell.get_output(f"tail -n200 {fuzzer_log}", verbose=False) + "\n"
        else:
            try:
                fuzzer_test_generator = FuzzerTestGenerator(
                    str(server_log),
                    str(workspace_path / "fuzzerout.sql" if buzzhouse else fuzzer_log),
                )
                if is_assertion:
                    failed_query = fuzzer_test_generator.get_failed_query()
                    if failed_query:
                        info += "---\n\nFailed query:\n"
                        info += failed_query + "\n"
                        reproduce_commands = (
                            fuzzer_test_generator.get_reproduce_commands(failed_query)
                        )
                        if reproduce_commands:
                            info += "---\n\nReproduce commands (auto-generated; may require manual adjustment):\n"
                            if len(reproduce_commands) > MAX_INLINE_REPRODUCE_COMMANDS:
                                reproduce_file_sql = (
                                    workspace_path / "reproduce_commands.sql"
                                )
                                try:
                                    with open(reproduce_file_sql, "w") as f:
                                        f.write("\n".join(reproduce_commands))
                                    paths.append(reproduce_file_sql)
                                    info += f"See file: {reproduce_file_sql}\n"
                                except IOError as write_error:
                                    info += f"Failed to write reproduce commands file: {write_error}\n"
                            else:
                                info += "\n".join(reproduce_commands) + "\n"
                # Signal case: no action needed (query fetch not possible)
            except Exception as e:
                info += (
                    "---\n\nFailed to fetch relevant queries from logs:\n"
                    + traceback.format_exc()
                    + "\n"
                )

        if fatal_log.exists():
            stack_trace = StackTraceReader.get_stack_trace(fatal_log)
            if stack_trace:
                info += "---\n\nStack trace:\n"
                info += stack_trace + "\n"

        if result.results:
            result.results[-1].info = info
        else:
            result.info = info

        if Shell.check(f"dmesg > {dmesg_log}"):
            oom_result = Result.from_commands_run(
                name="OOM in dmesg",
                command=f"! cat {dmesg_log} | grep -a -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' | tee /dev/stderr | grep -q .",
            )
            if not oom_result.is_ok():
                # change status: failure -> FAIL
                oom_result.set_status(Result.StatusExtended.FAIL)
                result.results.append(oom_result)
        else:
            print("WARNING: dmesg not enabled")

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
