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
    from ci.jobs.ci_utils import is_extended_run

    minutes = 60 if is_extended_run() else 30
    envs = [
        f"-e FUZZER_TO_RUN='{'BuzzHouse' if buzzhouse else 'AST Fuzzer'}'",
        f"-e FUZZ_TIME_LIMIT='{minutes}m'",
    ]

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        "--tmpfs /tmp/clickhouse:mode=1777 "
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

    changed_files_path = workspace_path / "ci-changed-files.txt"
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
    uid = os.getuid()
    gid = os.getgid()
    chown_cmd = f"docker run --rm --user root --volume {cwd}:/repo --workdir=/repo {docker_image} chown -R {uid}:{gid} /repo"
    Shell.check(chown_cmd, verbose=True)

    fuzzer_log = workspace_path / "fuzzer.log"
    dmesg_log = workspace_path / "dmesg.log"
    fatal_log = workspace_path / "fatal.log"
    server_log = workspace_path / "server.log"
    stderr_log = workspace_path / "stderr.log"
    paths = [
        workspace_path / "core.zst",
        workspace_path / "dmesg.log",
        fatal_log,
        stderr_log,
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
        error_info = f"Unknown error in fuzzer runner script. Traceback:\n{traceback.format_exc()}"
        Result.create_from(status=Result.Status.ERROR, info=error_info).complete_job()

    # parse runner script exit status
    status = Result.Status.FAILED
    info = []
    is_failed = True
    if server_died:
        # Server died - status will be determined after OOM checks
        is_failed = True
    elif fuzzer_exit_code in (0, 137, 143):
        # normal exit with timeout or OOM kill
        is_failed = False
        status = Result.Status.SUCCESS
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
                status = Result.Status.SUCCESS
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
                workspace_path / "fuzzerout.sql" if buzzhouse else fuzzer_log
            ),
        )
        parsed_name, parsed_info, files = fuzzer_log_parser.parse_failure()

        if parsed_name:
            results.append(
                Result(
                    name=parsed_name,
                    info=parsed_info,
                    status=Result.StatusExtended.FAIL,
                    files=files,
                )
            )

    result = Result.create_from(
        results=results, status=status if not results else None, info=info
    )

    if is_failed:
        # generate fatal log
        Shell.check(f"rg --text '\s<Fatal>\s' {server_log} > {fatal_log}")
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
