#!/usr/bin/env python3
import os
import subprocess
import time
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils


def main():
    # Ensure weekly selection defaults depend on workflow: PR disables weekly, Nightly enables
    wf = os.environ.get("WORKFLOW_NAME", "")
    if wf == "PR":
        os.environ.setdefault("KEEPER_RUN_WEEKLY", "0")
    else:
        os.environ.setdefault("KEEPER_RUN_WEEKLY", "1")
    # Keep PR job fast/reliable by default; can be overridden via env
    os.environ.setdefault("KEEPER_FAULTS", "off")
    os.environ.setdefault("KEEPER_DURATION", "120")

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

    # Install Python dependencies required by Keeper stress framework (PyYAML, etc.)
    install_cmd = (
        "python3 -m pip install --no-cache-dir -r tests/stress/keeper/requirements.txt "
        "|| python3 -m pip install --no-cache-dir pyyaml requests"
    )
    results.append(
        Result.from_commands_run(name="Install Keeper Python deps", command=install_cmd)
    )
    if not results[-1].is_ok():
        # Publish aggregated job result (deps install failed)
        Result.create_from(results=results, stopwatch=stop_watch).complete_job()
        return

    # Ensure ClickHouse client binary is available for integration helpers
    repo_dir = str(Path(__file__).resolve().parents[2])
    temp_dir = f"{repo_dir}/ci/tmp"
    ch_path = f"{temp_dir}/clickhouse"
    built_path = f"{repo_dir}/ci/tmp/build/programs/self-extracting/clickhouse"
    built_non_self = f"{repo_dir}/ci/tmp/build/programs/clickhouse"
    if Path(built_path).is_file() or Path(built_non_self).is_file():
        # Use locally built artifact from build jobs
        ch_path = built_path if Path(built_non_self).is_file() is False else built_non_self
        results.append(
            Result.from_commands_run(
                name="Use built ClickHouse binary",
                command=[
                    f"chmod +x {ch_path}",
                    f"{ch_path} --version || true",
                    f"ln -sf {ch_path} {temp_dir}/clickhouse",
                    f"ln -sf {ch_path} {temp_dir}/clickhouse-client",
                ],
            )
        )
        if not results[-1].is_ok():
            Result.create_from(results=results, stopwatch=stop_watch).complete_job()
            return
    else:
        if Utils.is_arm():
            ch_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
        else:
            ch_url = "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
        if not Path(ch_path).is_file():
            results.append(
                Result.from_commands_run(
                    name="Download ClickHouse client",
                    command=[
                        f"wget -nv -P {temp_dir} {ch_url}",
                        f"chmod +x {ch_path}",
                        f"{ch_path} --version",
                        f"ln -sf {ch_path} {temp_dir}/clickhouse",
                        f"ln -sf {ch_path} {temp_dir}/clickhouse-client",
                    ],
                )
            )
            if not results[-1].is_ok():
                Result.create_from(results=results, stopwatch=stop_watch).complete_job()
                return

    final_ch = "/usr/local/bin/clickhouse"
    results.append(
        Result.from_commands_run(
            name="Install ClickHouse binary",
            command=[
                f"cp -f {ch_path} {final_ch}",
                f"chmod +x {final_ch}",
                f"{final_ch} --version",
                f"ln -sf {final_ch} /usr/local/bin/clickhouse-client",
            ],
        )
    )
    if not results[-1].is_ok():
        Result.create_from(results=results, stopwatch=stop_watch).complete_job()
        return
    ch_path = final_ch

    # Construct pytest command (Result.from_pytest_run adds 'pytest' itself)
    # - quiet output, show per-test durations, run the keeper stress suite
    cmd = f"-q tests/stress/keeper/tests --durations=0{dur_arg}"

    # Prepare env for pytest
    env = os.environ.copy()
    # IMPORTANT: containers launched by docker-compose will bind-mount this exact host path
    # Mount the installed binary to avoid noexec on repo mounts and ensure parity with host-side tools
    server_bin_for_mount = ch_path
    env["CLICKHOUSE_BINARY"] = ch_path
    env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = ch_path
    env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = server_bin_for_mount
    env["CLICKHOUSE_BINARY"] = ch_path
    env.setdefault("CLICKHOUSE_TESTS_BASE_CONFIG_DIR", f"{repo_dir}/programs/server")
    env["PATH"] = f"/usr/local/bin:{env.get('PATH','')}"

    # Quick preflight to aid debugging in CI artifacts
    results.append(
        Result.from_commands_run(
            name="Binary preflight",
            command=[
                "which clickhouse || true",
                "clickhouse --version || true",
                f"ls -l {server_bin_for_mount} || true",
            ],
        )
    )

    results.append(
        Result.from_pytest_run(
            command=cmd,
            cwd=repo_dir,
            name="Keeper Stress",
            env=env,
            pytest_report_file=None,
            logfile=None,
        )
    )
    # Collect debug artifacts on failure
    files_to_attach = []
    try:
        if not results[-1].is_ok():
            base = Path(repo_dir) / "tests/stress/keeper/tests"
            inst_dirs = sorted(
                base.glob("_instances-*"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if inst_dirs:
                inst = inst_dirs[0]
                maybe = [
                    inst / "docker.log",
                ]
                for i in range(1, 4):
                    maybe.append(inst / f"keeper{i}" / "docker-compose.yml")
                    maybe.append(inst / f"keeper{i}" / "logs" / "clickhouse-server.log")
                    maybe.append(inst / f"keeper{i}" / "logs" / "clickhouse-server.err.log")
                for p in maybe:
                    try:
                        if p.exists():
                            files_to_attach.append(str(p))
                    except Exception:
                        pass
    except Exception:
        pass

    # Also attach docker-in-docker logs if present
    try:
        dind_log = Path("./ci/tmp/docker-in-docker.log")
        if dind_log.exists():
            files_to_attach.append(str(dind_log))
    except Exception:
        pass

    # Publish aggregated job result (with nested pytest results)
    Result.create_from(results=results, stopwatch=stop_watch, files=files_to_attach).complete_job()


if __name__ == "__main__":
    main()
