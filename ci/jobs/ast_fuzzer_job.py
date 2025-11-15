#!/usr/bin/env python3
import logging
import os
import subprocess
import sys
from pathlib import Path

from ci.jobs.scripts.docker_image import DockerImage
from ci.jobs.scripts.stack_trace_reader import StackTraceReader
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

IMAGE_NAME = "clickhouse/fuzzer"

cwd = Utils.cwd()
temp_dir = Path(f"{cwd}/ci/tmp/")


def get_run_command(
    workspace_path: Path,
    image: DockerImage,
    check_name: str,
) -> str:
    fuzzer_name = (
        "BuzzHouse" if check_name.lower().startswith("buzzhouse") else "AST Fuzzer"
    )
    envs = [
        f"-e FUZZER_TO_RUN='{fuzzer_name}'",
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


def main():
    logging.basicConfig(level=logging.INFO)

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    assert Path(f"{temp_dir}/clickhouse").exists(), "ClickHouse binary not found"

    docker_image = DockerImage.get_docker_image(IMAGE_NAME).pull_image()

    workspace_path = temp_dir / "workspace"
    workspace_path.mkdir(parents=True, exist_ok=True)

    run_command = get_run_command(workspace_path, docker_image, check_name)
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
    paths = [
        workspace_path / "core.zst",
        workspace_path / "dmesg.log",
        fatal_log,
        workspace_path / "stderr.log",
        workspace_path / "server.log",
        workspace_path / "fuzzer_out.sql",
        fuzzer_log,
        dmesg_log,
    ]

    try:
        with open(workspace_path / "status.txt", "r", encoding="utf-8") as status_f:
            status = status_f.readline().rstrip("\n")

        with open(workspace_path / "description.txt", "r", encoding="utf-8") as desc_f:
            description = desc_f.readline().rstrip("\n")
        result_ = Result(name=description, status=status)
        if not result_.is_ok():
            result.results = [result_]
            result.set_status(Result.Status.FAILED)
        else:
            pass
    except:
        result.set_status(Result.Status.ERROR)
        result.results = [
            Result(name="Unknown error", status=Result.StatusExtended.ERROR)
        ]

    info = ""
    if not result.is_ok():
        if fuzzer_log.exists():
            fuzzer_query = StackTraceReader.get_fuzzer_query(fuzzer_log)
            if fuzzer_query:
                info = f"Query: {fuzzer_query}"
            else:
                info = "Fuzzer log:\n ~~~\n" + Shell.get_output(
                    f"tail {fuzzer_log}", verbose=False
                )
            info += "\n---\n"
        if fatal_log.exists():
            stack_trace = StackTraceReader.get_stack_trace(fatal_log)
            if stack_trace:
                info += "Stack trace:\n" + stack_trace
        if result.results:
            result.results[-1].info = info
        else:
            result.info = info

        if Shell.check(f"dmesg > {dmesg_log}"):
            result.results.append(
                Result.from_commands_run(
                    name="OOM in dmesg",
                    command=f"! cat {dmesg_log} | grep -a -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE' | tee /dev/stderr | grep -q .",
                )
            )
        else:
            print("WARNING: dmesg not enabled")

    if not result.is_ok():
        for file in paths:
            if file.exists():
                result.set_files(file)

    result.complete_job()


if __name__ == "__main__":
    main()
