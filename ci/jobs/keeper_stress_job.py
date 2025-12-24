#!/usr/bin/env python3
import os
import argparse
import subprocess
import time
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils


def main():
    # Parse optional CLI args similar to integration jobs
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--test", nargs="+")
    parser.add_argument("--keeper-include-ids")
    parser.add_argument("--faults")
    parser.add_argument("--duration", type=int)
    parser.add_argument("--param")
    args, _ = parser.parse_known_args()
    # Ensure weekly selection defaults depend on workflow: PR disables weekly, Nightly enables
    wf = os.environ.get("WORKFLOW_NAME", "")
    jn = os.environ.get("JOB_NAME", "")
    is_pr = (wf == "PR") or ("(PR)" in jn)
    if is_pr:
        # Run full suite on CI by default (including @weekly scenarios)
        os.environ.setdefault("KEEPER_RUN_WEEKLY", "1")
        # Do not restrict scenarios on PRs unless explicitly overridden via --keeper-include-ids
        os.environ.setdefault("KEEPER_INCLUDE_IDS", "")
        # Keep PR runs predictable; users can override via --duration/--param
        os.environ.setdefault("KEEPER_READY_TIMEOUT", "60")
        os.environ.setdefault("KEEPER_BENCH_CLIENTS", "32")
        os.environ.setdefault("KEEPER_DISABLE_S3", "1")
    else:
        os.environ.setdefault("KEEPER_RUN_WEEKLY", "1")
    # Always keep containers/logs on fail for local/CI triage unless explicitly disabled
    os.environ.setdefault("KEEPER_KEEP_ON_FAIL", "1")
    # Enable faults by default on CI; can be overridden via env/CLI
    os.environ.setdefault("KEEPER_FAULTS", "on")
    os.environ.setdefault("KEEPER_DURATION", "120")

    # Apply custom KEY=VALUE envs passed via --param to mirror integration jobs UX
    if args.param:
        for pair in str(args.param).split(","):
            if not pair.strip():
                continue
            if "=" in pair:
                k, v = pair.split("=", 1)
                os.environ[str(k).strip()] = str(v)

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

    # Respect optional duration override from CLI first, then env
    dur_cli = args.duration
    dur_env = os.environ.get("KEEPER_DURATION")
    dur_arg = f" --duration={int(dur_cli)}" if dur_cli else (f" --duration={int(dur_env)}" if dur_env else "")

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
    # - show prints (-s), verbose (-vv), show per-test durations; run selected tests (or whole suite)
    tests_target = " ".join(args.test) if args.test else "tests/stress/keeper/tests"
    extra = []
    if args.keeper_include_ids:
        extra.append(f"--keeper-include-ids={args.keeper_include_ids}")
    if args.faults:
        extra.append(f"--faults={args.faults}")
    # Global test timeout ~= bench + readiness + overhead
    try:
        dur_val = int(args.duration or os.environ.get("KEEPER_DURATION", 120))
    except Exception:
        dur_val = 120
    try:
        ready_val = int(os.environ.get("KEEPER_READY_TIMEOUT", 120))
    except Exception:
        ready_val = 120
    timeout_val = max(180, min(1800, dur_val + ready_val + 120))
    extra.append(f"--timeout={timeout_val}")
    if is_pr:
        extra.append("--maxfail=1")
    cmd = f"-s -vv {tests_target} --durations=0{dur_arg} {' '.join(extra)}".rstrip()

    # Prepare env for pytest
    env = os.environ.copy()
    env["KEEPER_PYTEST_TIMEOUT"] = str(timeout_val)
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
                for i in range(1, 6):
                    maybe.append(inst / f"keeper{i}" / "docker-compose.yml")
                    maybe.append(inst / f"keeper{i}" / "logs" / "clickhouse-server.log")
                    maybe.append(inst / f"keeper{i}" / "logs" / "clickhouse-server.err.log")
                # attach emitted keeper config fragments (per node)
                for i in range(1, 6):
                    maybe.append(inst / f"keeper{i}" / "configs" / "config.d" / f"keeper_config_keeper{i}.xml")
                for p in maybe:
                    try:
                        if p.exists():
                            files_to_attach.append(str(p))
                    except Exception:
                        pass
                # Print concise tails directly to job output for quick triage
                tail_cmds = []
                for i in range(1, 4):
                    err = inst / f"keeper{i}" / "logs" / "clickhouse-server.err.log"
                    log = inst / f"keeper{i}" / "logs" / "clickhouse-server.log"
                    conf = inst / f"keeper{i}" / "configs" / "config.d" / f"keeper_config_keeper{i}.xml"
                    if conf:
                        tail_cmds.append(f"echo '==== keeper{i} config ====' && sed -n '1,120p' '{conf}'")
                    tail_cmds.append(f"echo '==== keeper{i} err ====' && tail -n 120 '{err}' || true")
                    tail_cmds.append(f"echo '==== keeper{i} log ====' && tail -n 120 '{log}' || true")
                # docker inventory and service logs (best-effort)
                tail_cmds.append("echo '==== docker ps (keepers) ====' && docker ps -a --format '{{.Names}}\t{{.Status}}\t{{.Image}}' | sed -n '1,200p'")
                tail_cmds.append("for n in $(docker ps --format '{{.Names}}' | grep -E 'keeper[0-9]+' || true); do echo '==== docker logs' $n '===='; docker logs --tail 200 $n || true; done")
                if tail_cmds:
                    results.append(Result.from_commands_run(name="Keeper debug tails", command=tail_cmds))
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
