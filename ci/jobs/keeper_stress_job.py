#!/usr/bin/env python3
import os
import subprocess
import time
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils


def main():
    # Ensure weekly selection for nightly runs unless explicitly overridden
    os.environ.setdefault("KEEPER_RUN_WEEKLY", "1")

    stop_watch = Utils.Stopwatch()
    results = []

    # Ensure docker-in-docker is up for nested compose workloads
    os.makedirs("./ci/tmp", exist_ok=True)
    if not Shell.check("docker info > /dev/null", verbose=True):
        with open("./ci/tmp/docker-in-docker.log", "w") as log_file:
            dockerd_proc = subprocess.Popen(
                "./ci/jobs/scripts/docker_in_docker.sh",
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        # wait until docker responds
        for i in range(60):
            if Shell.check("docker info > /dev/null", verbose=True):
                break
            time.sleep(2)

    # Respect optional duration override if provided
    dur = os.environ.get("KEEPER_DURATION")
    dur_arg = f" --duration={int(dur)}" if dur else ""

    # Construct pytest command (Result.from_pytest_run adds 'pytest' itself)
    # - quiet output, show per-test durations, run the keeper stress suite
    cmd = f"-q tests/stress/keeper/tests --durations=0{dur_arg}"

    # Run in repo root
    repo_dir = str(Path(__file__).resolve().parents[2])

    results.append(
        Result.from_pytest_run(
            command=cmd,
            cwd=repo_dir,
            name="Keeper Stress",
            env=os.environ.copy(),
            pytest_report_file=None,
            logfile=None,
        )
    )

    # Publish aggregated job result (with nested pytest results)
    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
