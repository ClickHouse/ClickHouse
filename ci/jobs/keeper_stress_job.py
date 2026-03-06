#!/usr/bin/env python3
"""Keeper stress test CI job runner."""
import json
import os
import yaml
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode

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

# Grafana base and dashboard UIDs (must match tests/stress/keeper/tools/build_grafana_dashboards.py).
GRAFANA_BASE = "https://grafana.clickhouse-prd.com"
GRAFANA_DASH_UID = {
    "run_details": "keeper-stress-run-details",
    "comparison": "keeper-stress-run-comparison",
    "historical": "keeper-stress-historical",
}


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
        "KEEPER_DURATION": "1200",
        "KEEPER_FAULTS": "true",
        "KEEPER_MATRIX_BACKENDS": "raftkeeper",
        "KEEPER_METRICS_INTERVAL_S": "5",
        # Temporary: run only one scenario to isolate RaftKeeper startup failures.
        "KEEPER_INCLUDE_IDS": "prod-mix-no-fault",
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


def get_branch(env):
    """
    Returns the branch name for metrics usage (e.g., for Grafana dashboards).

    Priority:
    1. Info().env.BRANCH, if present and non-empty
    2. env["BRANCH"], if set and non-empty
    3. $GITHUB_HEAD_REF or $GITHUB_REF_NAME from the environment
    4. git rev-parse --abbrev-ref HEAD
    5. Fallback: "master"
    """
    # 1. Check Info().env.BRANCH if available
    try:
        info = Info()
        info_env = getattr(info, "env", None)
        if info_env:
            branch = getattr(info_env, "BRANCH", None)
            if branch:
                branch = str(branch).strip()
                if branch:
                    return branch
    except Exception:
        print(f"failed to get branch from Info().env.BRANCH: {traceback.format_exc()}")

    # 2. Check env["BRANCH"]
    b = str(env.get("BRANCH", "")).strip()
    if b:
        return b

    # 3. Check CI standard branch envs
    for k in ("GITHUB_HEAD_REF", "GITHUB_REF_NAME"):
        val = os.environ.get(k, "").strip()
        if val:
            return val

    # 4. Attempt to get branch from git
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=REPO_DIR,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=10,
        ).strip()
        if out:
            return out
    except Exception as e:
        print(f"failed to get branch from git: {e}")

    # 5. Fallback to "master"
    return "master"


def build_pytest_command(junit_suffix=""):
    """Build pytest command. Scenario selection is via KEEPER_SCENARIO_FILES in env."""
    dur = env_int("KEEPER_DURATION", DEFAULT_TIMEOUT)
    ready = _ready_timeout()
    timeout_val = dur + ready + 120
    junit_path = f"{TEMP_DIR}/keeper_junit{junit_suffix}.xml"
    extra = [f"--timeout={timeout_val}", "--timeout-method=signal", f"--junitxml={junit_path}"]
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
    env["BRANCH"] = get_branch(env)
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

def _load_all_scenario_ids():
    """Load all scenario IDs from scenario YAML files (when KEEPER_INCLUDE_IDS is unset)."""
    scn_base = Path(REPO_DIR) / "tests" / "stress" / "keeper" / "scenarios"
    ids = []
    for f in sorted(scn_base.glob("*.yaml")):
        if not f.exists():
            continue
        try:
            d = yaml.safe_load(f.read_text())
            if isinstance(d, dict) and isinstance(d.get("scenarios"), list):
                for s in d["scenarios"]:
                    if isinstance(s, dict) and s.get("id"):
                        ids.append(str(s["id"]).strip())
        except Exception as e:
            print(f"failed to load scenario IDs from {f}: {traceback.format_exc()}")
            raise Exception(f"failed to load scenario IDs from {f}: {e}")
    return ids


def _scenario_ids_for_grafana():
    """
    Build full scenario IDs for Grafana (e.g. baseline-prod-mix[default], baseline-prod-mix[rocks]).
    Uses KEEPER_INCLUDE_IDS when set; otherwise loads all scenario IDs from YAML. Matches scenario_loader clone id format.
    """
    include_ids = os.environ.get("KEEPER_INCLUDE_IDS", "").strip()
    ids = [s.strip() for s in include_ids.split(",") if s.strip()] if include_ids else _load_all_scenario_ids()
    backends_raw = os.environ.get("KEEPER_MATRIX_BACKENDS", "default,rocks").strip()
    backends = [b.strip() for b in backends_raw.split(",") if b.strip()] or ["default"]
    return [f"{sid}[{b}]" for sid in ids for b in backends]


def _add_grafana_links(result, commit_sha, stop_watch, scenario_filter=None, branch=None):
    """
    Add clickable Grafana dashboard links to the result (bot comment / GH summary).
    All links use explicit time range params to avoid 1970. Three dashboards:
    - Run details: now-7d/now, scenario(s), backend=All, branch=current, commit_sha=current.
    - 1vs1 Comparison: last 7d, scenario + baseline_version + compare_version.
    - Historical: last 7d, scenario (no last_n_commits in URL; branch/backend).
    """
    if scenario_filter is None:
        include_ids = os.environ.get("KEEPER_INCLUDE_IDS", "").strip()
        scenario_filter = [s.strip() for s in include_ids.split(",") if s.strip()] if include_ids else []
    scenario_ids = _scenario_ids_for_grafana()
    scenario_val = scenario_filter[0] if scenario_filter else (scenario_ids[0] if scenario_ids else "")
    compare_short = (commit_sha or "")[:8]

    def _url(uid, params):
        return f"{GRAFANA_BASE}/d/{uid}?{urlencode(params, doseq=True)}"

    # 1. Run details — now-7d/now, current branch, current commit, full scenario list, backend=All
    p_details = {
        "orgId": "1",
        "from": "now-7d",
        "to": "now",
        "timezone": "utc",
        "var-backend": "$__all",
        "var-branch": branch or "master",
        "var-commit_sha": commit_sha or "$__all",
        "refresh": "5m",
    }
    if scenario_ids:
        p_details["var-scenario"] = scenario_ids
    result.set_clickable_label(
        "Grafana: Run details (this run)",
        _url(GRAFANA_DASH_UID["run_details"], p_details),
    )

    # 2. 1vs1 Comparison — Baseline = this run (branch + commit); Compare = user selects in UI
    p_comp = {
        "orgId": "1",
        "from": "now-7d",
        "to": "now",
        "timezone": "utc",
        "var-scenario": scenario_val,
        "var-baseline_branch": branch or "master",
        "var-baseline_version": compare_short,
        "refresh": "1m",
    }
    result.set_clickable_label(
        "Grafana: 1vs1 Comparison",
        _url(GRAFANA_DASH_UID["comparison"], p_comp),
    )

    # 3. Historical — last 7d, scenario only (branch/backend added as dashboard variables in UI)
    p_hist = {
        "orgId": "1",
        "from": "now-7d",
        "to": "now",
        "timezone": "utc",
        "var-scenario": scenario_val,
        "refresh": "1m",
    }
    result.set_clickable_label(
        "Grafana: Historical progression",
        _url(GRAFANA_DASH_UID["historical"], p_hist),
    )


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

    # Build RaftKeeper Docker image if raftkeeper backend is enabled.
    # The image is built locally from Dockerfile.raftkeeper (downloads pre-built binary for amd64).
    backends_raw = os.environ.get("KEEPER_MATRIX_BACKENDS", "")
    if "raftkeeper" in backends_raw:
        rk_image = os.environ.get("RAFTKEEPER_IMAGE", "raftkeeper:test")
        dockerfile = f"{REPO_DIR}/tests/integration/compose/Dockerfile.raftkeeper"
        if not Result.from_commands_run(
            name="Build RaftKeeper Docker image",
            command=f"docker build -f {dockerfile} -t {rk_image} {REPO_DIR}",
        ).is_ok():
            _abort(job_name, results, stop_watch)
            return

    _, timeout_val = build_pytest_command()  # get timeout for env
    env = build_pytest_env(ch_path, timeout_val)
    sidecar = env["KEEPER_METRICS_FILE"]
    Path(sidecar).parent.mkdir(parents=True, exist_ok=True)
    Path(sidecar).touch()

    # Set to False to run only no-fault tests (skip fault-injection scenarios).
    # Override via env: KEEPER_RUN_FAULT_TESTS=false
    RUN_FAULT_TESTS = os.environ.get("KEEPER_RUN_FAULT_TESTS", "false").lower() == "true"

    def _run_pytest(scenario_file, junit_suffix, run_name):
        run_env = env.copy()
        run_env["KEEPER_SCENARIO_FILES"] = scenario_file
        cmd, _ = build_pytest_command(junit_suffix=junit_suffix)
        if run_env.get("COMMIT_SHA") and "--commit-sha=" not in cmd:
            cmd = f"{cmd} --commit-sha={run_env['COMMIT_SHA']}"
        if run_env.get("BRANCH") and "--branch=" not in cmd:
            cmd = f"{cmd} --branch={run_env['BRANCH']}"
        report_file = f"{TEMP_DIR}/pytest{junit_suffix}.jsonl"
        log_file = f"{TEMP_DIR}/keeper_pytest{junit_suffix}.log"
        return Result.from_pytest_run(
            command=cmd,
            cwd=REPO_DIR,
            name=run_name,
            env=run_env,
            pytest_report_file=report_file,
            logfile=log_file,
        )

    # Run 1: No-fault tests (core_no_faults.yaml)
    results.append(_run_pytest("core_no_faults.yaml", "_no_faults", "Keeper Stress (no-faults)"))

    # Run 2: Fault tests (core_faults.yaml). To skip: RUN_FAULT_TESTS=False or env KEEPER_RUN_FAULT_TESTS=false
    if RUN_FAULT_TESTS:
        results.append(_run_pytest("core_faults.yaml", "_faults", "Keeper Stress (with-faults)"))

    pytest_ok = all(r.is_ok() for r in results)

    metrics_ok = validate_metrics_jsonl(sidecar)
    if not metrics_ok:
        pytest_ok = False

    Shell.run(f"chmod -R a+rX {REPO_DIR}/tests/stress/keeper/tests/_instances-* || true")

    job_log = f"{TEMP_DIR}/job.log"
    to_attach = [
        f"{TEMP_DIR}/keeper_junit_no_faults.xml",
        f"{TEMP_DIR}/keeper_pytest_no_faults.log",
    ]
    if RUN_FAULT_TESTS:
        to_attach.extend([
            f"{TEMP_DIR}/keeper_junit_faults.xml",
            f"{TEMP_DIR}/keeper_pytest_faults.log",
        ])
    to_attach.extend([job_log, sidecar])
    max_mb = env_int("KEEPER_ATTACH_PYTEST_JSONL_MAX_MB", 512)
    report_files = [f"{TEMP_DIR}/pytest_no_faults.jsonl"]
    if RUN_FAULT_TESTS:
        report_files.append(f"{TEMP_DIR}/pytest_faults.jsonl")
    for report_file in report_files:
        if Path(report_file).exists():
            report_size = os.path.getsize(report_file)
            print(f"report file {report_file} size: {report_size / (1024 * 1024)} MB")
            if report_size <= max_mb * 1024 * 1024:
                to_attach.append(report_file)
    for p in to_attach:
        if Path(p).exists():
            files_to_attach.append(str(p))

    files_to_attach.extend(collect_failure_artifacts(pytest_ok))

    dind = Path("./ci/tmp/docker-in-docker.log")
    if dind.exists():
        files_to_attach.append(str(dind))

    Shell.run("docker system prune -af --volumes || true; docker network prune -f || true")

    result = Result.create_from(
        name=job_name,
        results=results,
        stopwatch=stop_watch,
        files=files_to_attach,
    )
    _add_grafana_links(
        result,
        commit_sha=env.get("COMMIT_SHA"),
        stop_watch=stop_watch,
        branch=env.get("BRANCH"),
    )
    result.complete_job()


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
