#!/usr/bin/env python3
"""Keeper stress test CI job runner."""
import argparse
import os
import subprocess
import sys
import time
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path

# Ensure local invocations can import praktika without requiring PYTHONPATH
_repo_dir = str(Path(__file__).resolve().parents[2])
if _repo_dir not in sys.path:
    sys.path.insert(0, _repo_dir)
_ci_dir = os.path.join(_repo_dir, "ci")
if _ci_dir not in sys.path:
    sys.path.insert(0, _ci_dir)

from praktika.info import Info
from praktika.result import Result
from praktika.utils import Shell, Utils

try:
    from praktika._environment import _Environment
except Exception:
    _Environment = None

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
REPO_DIR = _repo_dir
TEMP_DIR = f"{REPO_DIR}/ci/tmp"
DEFAULT_XDIST_WORKERS = "1"
DEFAULT_TIMEOUT = 1200
DEFAULT_READY_TIMEOUT = 1200


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def env_int(name, default=0):
    """Get environment variable as int."""
    try:
        return int(os.environ.get(name) or default)
    except (ValueError, TypeError):
        return default


def set_default_env():
    """Set default environment variables for keeper stress tests."""
    defaults = {
        # Test selection
        "KEEPER_SCENARIO_FILE": "all",
        "KEEPER_INCLUDE_IDS": "CHA-01",
        "KEEPER_FAULTS": "on",
        "KEEPER_DURATION": "1200",
        "KEEPER_MATRIX_BACKENDS": "default,rocks",
        # Adaptive bench tuning
        "KEEPER_BENCH_ADAPTIVE": "1",
        "KEEPER_ADAPT_TARGET_P99_MS": "1200",
        "KEEPER_ADAPT_MAX_ERROR": "0.01",
        "KEEPER_ADAPT_STAGE_S": "15",
        "KEEPER_ADAPT_MIN_CLIENTS": "8",
        "KEEPER_ADAPT_MAX_CLIENTS": "192",
        # Gate defaults
        "KEEPER_DEFAULT_P99_MS": "1000",
        "KEEPER_DEFAULT_ERROR_RATE": "0.01",
        # CI behavior
        "KEEPER_KEEP_ON_FAIL": "1",
        "KEEPER_CLEAN_ARTIFACTS": "1",
        "KEEPER_XDIST_PORT_STEP": "100",
        "CI_HEARTBEAT_SEC": "60",
    }
    for k, v in defaults.items():
        os.environ.setdefault(k, v)


def apply_cli_params(args):
    """Apply KEY=VALUE params from --param CLI arg."""
    if not args.param:
        return
    for pair in str(args.param).split(","):
        pair = pair.strip()
        if "=" in pair:
            k, v = pair.split("=", 1)
            os.environ[k.strip()] = str(v)


def setup_docker():
    """Ensure docker is available, starting dockerd if needed. Returns success."""
    os.makedirs("./ci/tmp", exist_ok=True)
    os.environ["DOCKER_HOST"] = "unix:///var/run/docker.sock"
    os.environ.pop("DOCKER_TLS_VERIFY", None)
    os.environ.pop("DOCKER_CERT_PATH", None)

    if Shell.check("docker info > /dev/null", verbose=True):
        return True

    # Start docker-in-docker
    with open("./ci/tmp/docker-in-docker.log", "w") as log_file:
        subprocess.Popen(
            "./ci/jobs/scripts/docker_in_docker.sh",
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    # Wait for docker to respond
    for _ in range(90):
        if os.path.exists("/var/run/docker.sock") and Shell.check(
            "docker info > /dev/null", verbose=True
        ):
            return True
        time.sleep(2)
    return Shell.check("docker info > /dev/null", verbose=True)


def get_clickhouse_binary():
    """Locate or download ClickHouse binary. Returns (path, results)."""
    results = []
    ch_path = f"{TEMP_DIR}/clickhouse"
    built_self = f"{REPO_DIR}/ci/tmp/build/programs/self-extracting/clickhouse"
    built_non_self = f"{REPO_DIR}/ci/tmp/build/programs/clickhouse"

    # Check for locally built binary
    if Path(built_non_self).is_file():
        ch_path = built_non_self
    elif Path(built_self).is_file():
        ch_path = built_self
    else:
        ch_path = None

    if ch_path:
        results.append(
            Result.from_commands_run(
                name="Use built ClickHouse binary",
                command=[
                    f"chmod +x {ch_path}",
                    f"{ch_path} --version || true",
                    f"ln -sf {ch_path} {TEMP_DIR}/clickhouse",
                    f"ln -sf {ch_path} {TEMP_DIR}/clickhouse-client",
                ],
            )
        )
        return ch_path, results

    # Download from S3
    ch_path = f"{TEMP_DIR}/clickhouse"
    if not Path(ch_path).is_file():
        ch_url = (
            "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/aarch64/clickhouse"
            if Utils.is_arm()
            else "https://clickhouse-builds.s3.us-east-1.amazonaws.com/master/amd64/clickhouse"
        )
        results.append(
            Result.from_commands_run(
                name="Download ClickHouse client",
                command=[
                    f"wget -nv -P {TEMP_DIR} {ch_url}",
                    f"chmod +x {ch_path}",
                    f"{ch_path} --version",
                    f"ln -sf {ch_path} {TEMP_DIR}/clickhouse",
                    f"ln -sf {ch_path} {TEMP_DIR}/clickhouse-client",
                ],
            )
        )
    return ch_path, results


def install_clickhouse_binary(ch_path):
    """Install ClickHouse binary to /usr/local/bin. Returns Result."""
    final = "/usr/local/bin/clickhouse"
    return Result.from_commands_run(
        name="Install ClickHouse binary",
        command=[
            f'[ "$(readlink -f {ch_path})" = "$(readlink -f {final})" ] && echo "Already installed" || cp -f {ch_path} {final}',
            f"chmod +x {final} || true",
            f"{final} --version",
            f"ln -sf {final} /usr/local/bin/clickhouse-client || true",
        ],
    )


def get_commit_sha(env):
    """Resolve commit SHA from various sources."""
    if env.get("COMMIT_SHA") and env["COMMIT_SHA"] not in ("", "local"):
        return env["COMMIT_SHA"]

    # Try Info()
    try:
        sha = Info().sha or ""
        if sha:
            return sha
    except Exception:
        pass

    # Try environment variables
    for key in ("GITHUB_SHA", "GITHUB_HEAD_SHA", "SHA"):
        sha = os.environ.get(key, "")
        if sha:
            return sha

    # Try PRInfo
    try:
        from tests.ci.pr_info import PRInfo

        sha = getattr(PRInfo(), "sha", "")
        if sha:
            return sha
    except Exception:
        pass

    # Try git
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=REPO_DIR,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
        )
        sha = out.strip()
        if sha:
            return sha
    except Exception:
        pass

    return "local"


def build_pytest_command(args):
    """Build pytest command string. Returns (cmd, timeout_val, xdist_workers)."""
    tests_target = " ".join(args.test) if args.test else "tests/stress/keeper/tests"
    extra = []

    include_ids = args.keeper_include_ids or os.environ.get("KEEPER_INCLUDE_IDS", "")
    include_ids = str(include_ids or "").strip()
    if include_ids:
        extra.append(f"--keeper-include-ids={include_ids}")

    matrix_backends = os.environ.get("KEEPER_MATRIX_BACKENDS", "")
    matrix_backends = str(matrix_backends or "").strip()
    if matrix_backends:
        extra.append(f"--matrix-backends={matrix_backends}")
    if args.faults:
        extra.append(f"--faults={args.faults}")

    # Calculate timeout
    dur_val = args.duration or env_int("KEEPER_DURATION", DEFAULT_TIMEOUT)
    ready_val = max(
        DEFAULT_READY_TIMEOUT, env_int("KEEPER_READY_TIMEOUT", DEFAULT_READY_TIMEOUT)
    )
    timeout_val = max(180, min(7200, dur_val + ready_val + 120))
    extra.extend([f"--timeout={timeout_val}", "--timeout-method=signal"])

    # xdist workers
    xdist_workers = DEFAULT_XDIST_WORKERS
    extra.append(f"-n {xdist_workers}")
    is_parallel = xdist_workers not in ("1", "no", "0")

    extra.append(f"--junitxml={TEMP_DIR}/keeper_junit.xml")

    # Duration override
    if dur_val:
        extra.append(f"--duration={dur_val}")

    base = ["-vv", tests_target, "--durations=0"]
    if is_parallel:
        extra.extend(
            [
                "-o",
                "log_cli=false",
                "-o",
                "log_level=WARNING",
                "-p",
                "no:cacheprovider",
                "-p",
                "no:logging",
                "--max-worker-restart=2",
                "--dist",
                "load",
                "--capture=no",
            ]
        )
        cmd = " ".join(base + extra)
    else:
        cmd = " ".join(["-s"] + base + extra)

    return cmd, timeout_val, xdist_workers


def build_pytest_env(ch_path, timeout_val):
    """Build environment dict for pytest."""
    ready_val = max(
        DEFAULT_READY_TIMEOUT, env_int("KEEPER_READY_TIMEOUT", DEFAULT_READY_TIMEOUT)
    )
    env = os.environ.copy()

    # Timeouts
    env["KEEPER_READY_TIMEOUT"] = str(ready_val)
    env["KEEPER_START_TIMEOUT_SEC"] = str(ready_val)

    # ClickHouse binary paths
    env["CLICKHOUSE_BINARY"] = ch_path
    env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = ch_path
    env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = ch_path
    env.setdefault("CLICKHOUSE_TESTS_BASE_CONFIG_DIR", f"{REPO_DIR}/programs/server")
    env["PATH"] = f"/usr/local/bin:{env.get('PATH', '')}"

    # Metrics
    env["KEEPER_METRICS_FILE"] = f"{TEMP_DIR}/keeper_metrics.jsonl"

    # PYTHONPATH - add repo root and tests/stress for keeper imports
    env["PYTHONPATH"] = f"{REPO_DIR}:{REPO_DIR}/tests/stress:{REPO_DIR}/ci"

    # Remove CI DB credentials from test environment
    for k in (
        "CI_DB_USER",
        "CI_DB_PASSWORD",
        "KEEPER_METRICS_CLICKHOUSE_URL",
        "CI_DB_URL",
    ):
        env.pop(k, None)

    # Commit SHA
    env["COMMIT_SHA"] = get_commit_sha(env)

    # PYTEST_ADDOPTS
    fh_to = max(1200, min(timeout_val - 60, 1800))
    env["PYTEST_ADDOPTS"] = (
        f"--ignore-glob=tests/stress/keeper/tests/_instances-* -o faulthandler_timeout={fh_to}"
    )

    return env


def parse_junit_summary(junit_file):
    """Parse JUnit XML and return (tests, passed, failures, errors, skipped)."""
    tests = failures = errors = skipped = 0
    try:
        if not Path(junit_file).exists():
            return 0, 0, 0, 0, 0
        root = ET.parse(junit_file).getroot()
        suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
        for ts in suites:
            tests += int(ts.attrib.get("tests", 0) or 0)
            failures += int(ts.attrib.get("failures", 0) or 0)
            errors += int(ts.attrib.get("errors", 0) or 0)
            skipped += int(ts.attrib.get("skipped", 0) or 0)
    except Exception:
        pass
    passed = max(0, tests - failures - errors - skipped)
    return tests, passed, failures, errors, skipped


def collect_failure_artifacts(pytest_ok):
    """Collect debug artifacts on failure. Returns (files, results)."""
    files = []
    results = []
    if pytest_ok:
        return files, results

    base = Path(REPO_DIR) / "tests/stress/keeper/tests"
    inst_dirs = sorted(
        base.glob("_instances-*"), key=lambda p: p.stat().st_mtime, reverse=True
    )
    if not inst_dirs:
        return files, results

    inst = inst_dirs[0]
    # Collect files
    for i in range(1, 6):
        for p in [
            inst / f"keeper{i}" / "docker-compose.yml",
            inst / f"keeper{i}" / "logs" / "clickhouse-server.log",
            inst / f"keeper{i}" / "logs" / "clickhouse-server.err.log",
            inst
            / f"keeper{i}"
            / "configs"
            / "config.d"
            / f"keeper_config_keeper{i}.xml",
        ]:
            if p.exists():
                files.append(str(p))

    results.append(
        Result.from_commands_run(
            name="Keeper debug: docker ps",
            command=[
                f"echo 'Instances dir: {inst}'",
                "docker ps -a --format '{{.Names}}\t{{.Status}}' | sed -n '1,200p'",
            ],
        )
    )

    return files, results


def start_keepalive():
    """Start keepalive process. Returns process or None."""
    try:
        hb = max(10, env_int("CI_HEARTBEAT_SEC", 60))
        cmd = f"bash -lc 'while true; do echo [keeper-keepalive] $(date -u +%Y-%m-%dT%H:%M:%SZ); sleep {hb}; done'"
        return subprocess.Popen(cmd, shell=True)
    except Exception:
        return None


def stop_keepalive(proc):
    """Stop keepalive process."""
    if not proc or proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--test", nargs="+")
    parser.add_argument("--keeper-include-ids")
    parser.add_argument("--faults")
    parser.add_argument("--duration", type=int)
    parser.add_argument("--param")
    args, _ = parser.parse_known_args()
    set_default_env()
    apply_cli_params(args)

    job_name = os.environ.get("JOB_NAME") or os.environ.get("CHECK_NAME")
    if not job_name and _Environment is not None:
        try:
            job_name = _Environment.get().JOB_NAME
        except Exception:
            job_name = None
    job_name = job_name or "Keeper Stress"

    stop_watch = Utils.Stopwatch()
    results = []
    files_to_attach = []

    # Docker setup
    if not setup_docker():
        results.append(
            Result.from_commands_run(
                name="Docker startup failed",
                command=[
                    "ps ax | grep dockerd | grep -v grep || true",
                    "tail -n 200 ./ci/tmp/docker-in-docker.log || true",
                    "df -h || true",
                ],
            )
        )
        Result.create_from(
            name=job_name,
            results=results,
            status=Result.Status.ERROR,
            stopwatch=stop_watch,
        ).complete_job()
        return

    # Preflight
    results.append(
        Result.from_commands_run(name="Disk space preflight", command=["df -h || true"])
    )
    results.append(
        Result.from_commands_run(
            name="Docker prune",
            command=[
                "docker system prune -af --volumes || true",
                "docker builder prune -af || true",
                "docker network prune -f || true",
            ],
        )
    )

    # Install Python deps
    results.append(
        Result.from_commands_run(
            name="Install Keeper Python deps",
            command="PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --no-cache-dir -r tests/stress/keeper/requirements.txt pytest-reportlog",
        )
    )
    if not results[-1].is_ok():
        Result.create_from(
            name=job_name, results=results, stopwatch=stop_watch
        ).complete_job()
        return

    # ClickHouse binary
    ch_path, ch_results = get_clickhouse_binary()
    results.extend(ch_results)
    if ch_results and not ch_results[-1].is_ok():
        Result.create_from(
            name=job_name, results=results, stopwatch=stop_watch
        ).complete_job()
        return

    results.append(install_clickhouse_binary(ch_path))
    if not results[-1].is_ok():
        Result.create_from(
            name=job_name, results=results, stopwatch=stop_watch
        ).complete_job()
        return
    ch_path = "/usr/local/bin/clickhouse"

    # Build pytest command and env
    cmd, timeout_val, xdist_workers = build_pytest_command(args)
    env = build_pytest_env(ch_path, timeout_val)

    # Add commit SHA to command if available
    if env.get("COMMIT_SHA") and "--commit-sha=" not in cmd:
        cmd = f"{cmd} --commit-sha={env['COMMIT_SHA']}"

    # Metrics sidecar preflight
    sidecar = env["KEEPER_METRICS_FILE"]
    Path(os.path.dirname(sidecar)).mkdir(parents=True, exist_ok=True)
    Path(sidecar).touch()

    # Preflight info
    results.append(
        Result.from_commands_run(
            name="Preflight",
            command=[
                "clickhouse --version || true",
                f"echo 'xdist_workers={xdist_workers}'",
            ],
        )
    )

    # Run pytest
    report_file = f"{TEMP_DIR}/pytest.jsonl"
    junit_file = f"{TEMP_DIR}/keeper_junit.xml"
    pytest_log_file = f"{TEMP_DIR}/keeper_pytest.log"

    keepalive = start_keepalive()
    results.append(
        Result.from_pytest_run(
            command=cmd,
            cwd=REPO_DIR,
            name="Keeper Stress",
            env=env,
            pytest_report_file=report_file,
            logfile=pytest_log_file,
        )
    )
    stop_keepalive(keepalive)
    pytest_ok = results[-1].is_ok()

    # Handle large report files
    max_mb = env_int("KEEPER_ATTACH_PYTEST_JSONL_MAX_MB", 512)
    try:
        if (
            os.path.exists(report_file)
            and os.path.getsize(report_file) > max_mb * 1024 * 1024
        ):
            sz_gb = round(os.path.getsize(report_file) / (1024**3), 2)
            results.append(
                Result.from_commands_run(
                    name=f"Skip large pytest report: {sz_gb} GiB", command=["true"]
                )
            )
    except Exception:
        pass

    # Fix artifact permissions
    results.append(
        Result.from_commands_run(
            name="Fix permissions",
            command=[
                f"chmod -R a+rX {REPO_DIR}/tests/stress/keeper/tests/_instances-* || true"
            ],
        )
    )

    # Attach files
    for p in [junit_file, pytest_log_file]:
        if Path(p).exists():
            files_to_attach.append(str(p))

    # Praktika runner log (contains full stdout/stderr of the job)
    job_log = f"{TEMP_DIR}/job.log"
    if Path(job_log).exists():
        files_to_attach.append(job_log)
    # Skip large report file
    if Path(report_file).exists():
        try:
            if os.path.getsize(report_file) <= max_mb * 1024 * 1024:
                files_to_attach.append(report_file)
        except Exception:
            files_to_attach.append(report_file)

    # Metrics sidecar
    if Path(sidecar).exists():
        files_to_attach.append(sidecar)

    # Test summary
    tests, passed, failures, errors, skipped = parse_junit_summary(junit_file)
    results.append(
        Result.from_commands_run(
            name=f"Summary: {passed} passed, {failures} failed, {skipped} skipped, {errors} errors (total {tests})",
            command=["true"],
        )
    )

    # Failure artifacts
    fail_files, fail_results = collect_failure_artifacts(pytest_ok)
    files_to_attach.extend(fail_files)
    results.extend(fail_results)

    # Docker-in-docker log
    dind_log = Path("./ci/tmp/docker-in-docker.log")
    if dind_log.exists():
        files_to_attach.append(str(dind_log))

    # Post-clean
    results.append(
        Result.from_commands_run(
            name="Docker post-clean",
            command=[
                "docker system prune -af --volumes || true",
                "docker network prune -f || true",
            ],
        )
    )

    Result.create_from(
        name=job_name,
        results=results,
        stopwatch=stop_watch,
        files=files_to_attach,
    ).complete_job()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        job_name = os.environ.get("JOB_NAME") or os.environ.get("CHECK_NAME")
        if not job_name and _Environment is not None:
            try:
                job_name = _Environment.get().JOB_NAME
            except Exception:
                job_name = None
        job_name = job_name or "Keeper Stress"
        err_txt = f"{e}\n{traceback.format_exc()}"
        os.makedirs("./ci/tmp", exist_ok=True)
        err_path = "./ci/tmp/keeper_job_fatal_error.txt"
        try:
            with open(err_path, "w", encoding="utf-8") as f:
                f.write(err_txt)
        except Exception:
            pass
        Result.create_from(
            name=job_name,
            status=Result.Status.ERROR,
            info=err_txt,
            stopwatch=Utils.Stopwatch(),
            files=[err_path] if os.path.exists(err_path) else [],
        ).complete_job()
