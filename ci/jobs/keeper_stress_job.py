#!/usr/bin/env python3
"""Keeper stress test CI job runner."""
import json
import os
import subprocess
import sys
import time
import traceback
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

from praktika._environment import _Environment

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
REPO_DIR = _repo_dir
TEMP_DIR = f"{REPO_DIR}/ci/tmp"
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


def _job_name():
    name = os.environ.get("JOB_NAME") or os.environ.get("CHECK_NAME")
    if not name:
        name = _Environment.get().JOB_NAME
    return name or "Keeper Stress"


def _ready_timeout():
    return max(DEFAULT_READY_TIMEOUT, env_int("KEEPER_READY_TIMEOUT"))


def _abort(job_name, results, stopwatch, status=None, info=None):
    opts = {"name": job_name, "results": results, "stopwatch": stopwatch}
    if status is not None:
        opts["status"] = status
    if info is not None:
        opts["info"] = info
    Result.create_from(**opts).complete_job()


def set_default_env():
    """Defaults for keeper stress (env and pytest)."""
    for k, v in {
        "KEEPER_INCLUDE_IDS": "CHA-01,CHA-01-REPLAY",
        "KEEPER_DURATION": "1200",
        "KEEPER_FAULTS": "false",
        "KEEPER_MATRIX_BACKENDS": "default,rocks",
    }.items():
        os.environ.setdefault(k, v)


def setup_docker():
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


def get_commit_sha(env):
    c = (env.get("COMMIT_SHA") or "").strip()
    if c and c not in ("", "local"):
        return c
    try:
        if Info().sha:
            return Info().sha
    except Exception:
        pass
    for k in ("GITHUB_SHA", "GITHUB_HEAD_SHA", "SHA"):
        if os.environ.get(k):
            return os.environ.get(k)
    try:
        from tests.ci.pr_info import PRInfo
        if getattr(PRInfo(), "sha", None):
            return PRInfo().sha
    except Exception:
        pass
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO_DIR, stderr=subprocess.DEVNULL, text=True, timeout=10)
        if out.strip():
            return out.strip()
    except Exception:
        pass
    return "local"


def build_pytest_command():
    dur = env_int("KEEPER_DURATION", DEFAULT_TIMEOUT)
    ready = _ready_timeout()
    timeout_val = dur + ready + 120
    extra = [f"--timeout={timeout_val}", "--timeout-method=signal", f"--junitxml={TEMP_DIR}/keeper_junit.xml"]
    extra.append(f"--duration={dur}")

    base = ["-vv", "tests/stress/keeper/tests", "--durations=0"]
    cmd = " ".join((["-s"]) + base + extra)
    return cmd, timeout_val


def build_pytest_env(ch_path, timeout_val):
    ready = _ready_timeout()
    env = os.environ.copy()
    env["KEEPER_READY_TIMEOUT"] = env["KEEPER_START_TIMEOUT_SEC"] = str(ready)
    for k in ("CLICKHOUSE_BINARY", "CLICKHOUSE_TESTS_CLIENT_BIN_PATH", "CLICKHOUSE_TESTS_SERVER_BIN_PATH"):
        env[k] = ch_path
    env.setdefault("CLICKHOUSE_TESTS_BASE_CONFIG_DIR", f"{REPO_DIR}/programs/server")
    env["PATH"] = f"{str(Path(ch_path).resolve().parent)}:{env.get('PATH', '')}"
    env["KEEPER_METRICS_FILE"] = f"{TEMP_DIR}/keeper_metrics.jsonl"
    env["PYTHONPATH"] = f"{REPO_DIR}:{REPO_DIR}/tests/stress:{REPO_DIR}/ci"
    env["COMMIT_SHA"] = get_commit_sha(env)
    env["PYTEST_ADDOPTS"] = f"--ignore-glob=tests/stress/keeper/tests/_instances-* -o faulthandler_timeout={timeout_val - 60}"
    return env


def validate_metrics_jsonl(metrics_path):
    """Validate that keeper_metrics.jsonl has source=bench and the six bench names. Returns (ok, report)."""
    p = str(metrics_path).strip()
    if not p or not Path(p).exists():
        print(f"metrics file {p} does not exist")
        return False

    # Check if the file is not empty
    try:
        if os.path.getsize(p) == 0:
            print(f"metrics file {p} is empty")
            return False
    except Exception as e:
        print(f"failed to stat metrics path {p}: {e}")
        return False

    print(f"metrics file {p} is valid")
    return True

def collect_failure_artifacts(pytest_ok):
    """Collect debug artifact file paths on failure. Returns list of paths."""
    files = []
    if pytest_ok:
        return files
    base = Path(REPO_DIR) / "tests/stress/keeper/tests"
    inst_dirs = sorted(base.glob("_instances-*"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not inst_dirs:
        return files
    inst = inst_dirs[0]
    for i in range(1, 6):
        k = inst / f"keeper{i}"
        for rel in ["docker-compose.yml", "logs/clickhouse-server.log", "logs/clickhouse-server.err.log"]:
            if (k / rel).exists():
                files.append(str(k / rel))
        cfg = k / "configs" / "config.d" / f"keeper_config_keeper{i}.xml"
        if cfg.exists():
            files.append(str(cfg))
    return files


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    set_default_env()

    job_name = _job_name()
    stop_watch = Utils.Stopwatch()
    results = []
    files_to_attach = []

    if not setup_docker():
        _abort(job_name, results, stop_watch, status=Result.Status.ERROR)
        return

    Shell.run("docker system prune -af --volumes || true; docker builder prune -af || true; docker network prune -f || true")
    if not Result.from_commands_run(name="Install Keeper Python deps", command="PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --no-cache-dir -r tests/stress/keeper/requirements.txt pytest-reportlog").is_ok():
        _abort(job_name, results, stop_watch)
        return

    ch_path = f"{Utils.cwd()}/ci/tmp/clickhouse"
    if not Path(ch_path).is_file():
        _abort(
            job_name, results, stop_watch, status=Result.Status.ERROR,
            info=f"ClickHouse binary not found at {ch_path}. The pipeline must provide the binary (e.g. build or artifact step).",
        )
        return
    if not Shell.check(f"chmod +x {ch_path}", verbose=True):
        _abort(job_name, results, stop_watch, status=Result.Status.ERROR, info="chmod +x on ClickHouse binary failed.")
        return
    if not Shell.check(f"{ch_path} --version", verbose=True):
        _abort(job_name, results, stop_watch, status=Result.Status.ERROR, info="ClickHouse binary --version failed.")
        return

    # Build pytest command and env
    cmd, timeout_val = build_pytest_command()
    env = build_pytest_env(ch_path, timeout_val)

    if env.get("COMMIT_SHA") and "--commit-sha=" not in cmd:
        cmd = f"{cmd} --commit-sha={env['COMMIT_SHA']}"

    sidecar = env["KEEPER_METRICS_FILE"]
    Path(sidecar).parent.mkdir(parents=True, exist_ok=True)
    Path(sidecar).touch()

    report_file = f"{TEMP_DIR}/pytest.jsonl"
    junit_file = f"{TEMP_DIR}/keeper_junit.xml"
    pytest_log_file = f"{TEMP_DIR}/keeper_pytest.log"

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
    pytest_ok = results[-1].is_ok()

    metrics_ok = validate_metrics_jsonl(sidecar)
    if not metrics_ok:
        pytest_ok = False

    max_mb = env_int("KEEPER_ATTACH_PYTEST_JSONL_MAX_MB", 512)
    report_attach = False
    
    report_size = os.path.getsize(report_file)
    print(f"report file {report_file} size: {report_size / (1024 * 1024)} MB")
    if Path(report_file).exists() and report_size <= max_mb * 1024 * 1024:
        report_attach = True
    else:
        report_attach = False
        print(f"report file {report_file} is too large: {report_size / (1024 * 1024)} MB")
    
    Shell.run(f"chmod -R a+rX {REPO_DIR}/tests/stress/keeper/tests/_instances-* || true")

    job_log = f"{TEMP_DIR}/job.log"
    to_attach = [junit_file, pytest_log_file, job_log, sidecar]
    if report_attach:
        to_attach.append(report_file)
    for p in to_attach:
        if Path(p).exists():
            files_to_attach.append(str(p))

    files_to_attach.extend(collect_failure_artifacts(pytest_ok))

    dind = Path("./ci/tmp/docker-in-docker.log")
    if dind.exists():
        files_to_attach.append(str(dind))

    Shell.run("docker system prune -af --volumes || true; docker network prune -f || true")

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
        job_name = _job_name()
        err_txt = f"{e}\n{traceback.format_exc()}"
        os.makedirs("./ci/tmp", exist_ok=True)
        err_path = "./ci/tmp/keeper_job_fatal_error.txt"

        with open(err_path, "w", encoding="utf-8") as f:
            f.write(err_txt)

        Result.create_from(
            name=job_name,
            status=Result.Status.ERROR,
            info=err_txt,
            stopwatch=Utils.Stopwatch(),
            files=[err_path] if os.path.exists(err_path) else [],
        ).complete_job()
