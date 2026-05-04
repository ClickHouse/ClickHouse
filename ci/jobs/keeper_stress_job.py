#!/usr/bin/env python3
"""Keeper stress test CI job runner."""
import json
import os
import yaml
import subprocess
import sys
import time
import traceback
from pathlib import Path
from urllib.parse import urlencode

# Ensure the repo and ci/ roots are on sys.path so praktika can be imported
# without requiring the caller to set PYTHONPATH.
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
    """Apply default environment variables for the three job modes.

    Mode is determined from Info().pr_number and Info().workflow_name:
      - PR (pr_number > 0): 3 no-fault scenarios, default backend, 15 min each.
      - NightlyKeeperFaults: fault scenarios, default + rocks backends, 20 min each.
      - NightlyKeeperNoFaults: no-fault scenarios, default + rocks backends, 20 min each.
      - Other/local: both fault and no-fault, nightly defaults.

    The legacy KEEPER_PR_MODE env var is still honoured for local runs.
    """
    pr_number = 0
    workflow_name = ""
    try:
        info = Info()
        pr_number = info.pr_number or 0
        workflow_name = info.workflow_name or ""
    except Exception:
        pass

    is_pr = pr_number > 0 or os.environ.get("KEEPER_PR_MODE", "").lower() in ("1", "true")

    if is_pr:
        pr_number_str = str(pr_number or os.environ.get("GITHUB_PR_NUMBER", os.environ.get("PR_NUMBER", "0")))
        for k, v in {
            "KEEPER_DURATION": "900",
            "KEEPER_FAULTS": "false",
            "KEEPER_RUN_FAULT_TESTS": "false",
            "KEEPER_RUN_NO_FAULT_TESTS": "true",
            "KEEPER_MATRIX_BACKENDS": "default",
            "KEEPER_INCLUDE_IDS": "prod-mix-no-fault,read-multi-no-fault,write-multi-no-fault",
            "KEEPER_METRICS_INTERVAL_S": "5",
            "KEEPER_JOB_TYPE": "pr",
            "KEEPER_PR_NUMBER": pr_number_str,
        }.items():
            os.environ.setdefault(k, v)
        return

    if workflow_name == "NightlyKeeperFaults":
        os.environ.setdefault("KEEPER_RUN_FAULT_TESTS", "true")
        os.environ.setdefault("KEEPER_RUN_NO_FAULT_TESTS", "false")
    elif workflow_name == "NightlyKeeperNoFaults":
        os.environ.setdefault("KEEPER_RUN_FAULT_TESTS", "false")
        os.environ.setdefault("KEEPER_RUN_NO_FAULT_TESTS", "true")
    else:
        os.environ.setdefault("KEEPER_RUN_FAULT_TESTS", "true")
        os.environ.setdefault("KEEPER_RUN_NO_FAULT_TESTS", "true")

    for k, v in {
        "KEEPER_DURATION": "1200",
        "KEEPER_FAULTS": "true",
        "KEEPER_MATRIX_BACKENDS": "default,rocks",
        "KEEPER_METRICS_INTERVAL_S": "5",
        "KEEPER_JOB_TYPE": "nightly",
    }.items():
        os.environ.setdefault(k, v)


def setup_docker():
    os.makedirs("./ci/tmp", exist_ok=True)
    os.environ["DOCKER_HOST"] = "unix:///var/run/docker.sock"
    os.environ.pop("DOCKER_TLS_VERIFY", None)
    os.environ.pop("DOCKER_CERT_PATH", None)

    if Shell.check("docker info > /dev/null", verbose=True):
        return True

    # Start docker-in-docker.
    # Keep log_file open for the subprocess's lifetime — closing it (e.g. via
    # a with-block) before the daemon exits would drop the write end of the fd
    # that the process is actively writing to.
    # Keep the Popen handle so we can detect an immediate startup failure
    # inside the poll loop below.
    dind_log = open("./ci/tmp/docker-in-docker.log", "w")  # noqa: WPS515
    dind_proc = subprocess.Popen(
        "./ci/jobs/scripts/docker_in_docker.sh",
        stdout=dind_log,
        stderr=subprocess.STDOUT,
    )
    for _ in range(90):
        if dind_proc.poll() is not None:
            print(f"[setup_docker] docker-in-docker.sh exited early (rc={dind_proc.returncode}); check ./ci/tmp/docker-in-docker.log")
            return False
        if os.path.exists("/var/run/docker.sock") and Shell.check(
            "docker info > /dev/null", verbose=True
        ):
            return True
        time.sleep(2)
    return Shell.check("docker info > /dev/null", verbose=True)


def get_commit_sha(env):
    c = (env.get("COMMIT_SHA") or "").strip()
    if c and c != "local":
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
    """Return the branch name for Grafana metrics.

    Priority:
    1. Info().env.BRANCH
    2. env["BRANCH"]
    3. $GITHUB_HEAD_REF or $GITHUB_REF_NAME
    4. git rev-parse --abbrev-ref HEAD
    5. Fallback: "master"
    """
    try:
        info = Info()
        info_env = getattr(info, "env", None)
        if info_env:
            branch = str(getattr(info_env, "BRANCH", None) or "").strip()
            if branch:
                return branch
    except Exception:
        print(f"failed to get branch from Info().env.BRANCH: {traceback.format_exc()}")

    b = str(env.get("BRANCH", "")).strip()
    if b:
        return b

    for k in ("GITHUB_HEAD_REF", "GITHUB_REF_NAME"):
        val = os.environ.get(k, "").strip()
        if val:
            return val

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

    return "master"


def build_pytest_command(junit_suffix=""):
    """Build the pytest command string. Scenario selection is via KEEPER_SCENARIO_FILES in env."""
    dur = env_int("KEEPER_DURATION", DEFAULT_TIMEOUT)
    ready = _ready_timeout()
    timeout_val = dur + ready + 120
    junit_path = f"{TEMP_DIR}/keeper_junit{junit_suffix}.xml"
    extra = [f"--timeout={timeout_val}", "--timeout-method=signal", f"--junitxml={junit_path}"]
    extra.append(f"--duration={dur}")
    base = ["-vv", "tests/stress/keeper/tests", "--durations=0"]
    cmd = " ".join(["-s"] + base + extra)
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


_REQUIRED_BENCH_METRICS = {
    "rps",
    "read_p99_ms",
    "write_p99_ms",
    "read_p50_ms",
    "write_p50_ms",
    "error_rate",
}


def validate_metrics_jsonl(metrics_path):
    """Validate that keeper_metrics.jsonl has source=bench and the six required bench metric names.

    Returns True on success, False on any failure.
    """
    p = str(metrics_path).strip()
    if not p or not Path(p).exists():
        print(f"[validate_metrics_jsonl] metrics file {p!r} does not exist")
        return False

    try:
        size = os.path.getsize(p)
    except Exception as e:
        print(f"[validate_metrics_jsonl] failed to stat {p!r}: {e}")
        return False

    if size == 0:
        print(f"[validate_metrics_jsonl] metrics file {p!r} is empty")
        return False

    has_bench_source = False
    seen_metric_names = set()
    parse_errors = 0

    try:
        with open(p) as fh:
            for lineno, raw in enumerate(fh, 1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    row = json.loads(raw)
                except json.JSONDecodeError as e:
                    print(f"[validate_metrics_jsonl] line {lineno}: JSON parse error: {e}")
                    parse_errors += 1
                    continue
                if row.get("source") == "bench":
                    has_bench_source = True
                name = row.get("name")
                if name:
                    seen_metric_names.add(name)
    except OSError as e:
        print(f"[validate_metrics_jsonl] failed to read {p!r}: {e}")
        return False

    if parse_errors:
        print(f"[validate_metrics_jsonl] {parse_errors} unparseable line(s) in {p!r}")
        return False

    if not has_bench_source:
        print(f"[validate_metrics_jsonl] no row with source='bench' found in {p!r}")
        return False

    missing = _REQUIRED_BENCH_METRICS - seen_metric_names
    if missing:
        print(
            f"[validate_metrics_jsonl] missing required bench metric names in {p!r}: "
            + ", ".join(sorted(missing))
        )
        return False

    print(
        f"[validate_metrics_jsonl] {p!r} OK — "
        f"bench source present, all required metrics found: {sorted(_REQUIRED_BENCH_METRICS)}"
    )
    return True


def _load_all_scenario_ids(run_faults, run_no_faults):
    """Load scenario IDs from the YAML files that will actually be run."""
    scn_base = Path(REPO_DIR) / "tests" / "stress" / "keeper" / "scenarios"
    yaml_files = []
    if run_no_faults:
        yaml_files.append(scn_base / "core_no_faults.yaml")
    if run_faults:
        yaml_files.append(scn_base / "core_faults.yaml")
    ids = []
    for f in yaml_files:
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


def _scenario_ids_for_grafana(run_faults, run_no_faults):
    """Return fully-qualified scenario IDs for Grafana variables.

    Format: <scenario-id>[<backend>], e.g. baseline-prod-mix[default].
    Uses KEEPER_INCLUDE_IDS when set; otherwise derives IDs from the YAML files.
    """
    include_ids = os.environ.get("KEEPER_INCLUDE_IDS", "").strip()
    ids = [s.strip() for s in include_ids.split(",") if s.strip()] if include_ids else _load_all_scenario_ids(run_faults, run_no_faults)
    backends_raw = os.environ.get("KEEPER_MATRIX_BACKENDS", "default,rocks").strip()
    backends = [b.strip() for b in backends_raw.split(",") if b.strip()] or ["default"]
    return [f"{sid}[{b}]" for sid in ids for b in backends]


def _add_grafana_links(result, commit_sha, stop_watch, run_faults, run_no_faults, scenario_filter=None, branch=None):
    """Attach clickable Grafana dashboard links to the result.

    - Run details: precise job time window, this run's scenarios only.
    - 1vs1 Comparison: last 7d so the user can pick a run to compare against.
    - Historical: last 7d showing progression over recent runs.
    """
    if scenario_filter is None:
        include_ids = os.environ.get("KEEPER_INCLUDE_IDS", "").strip()
        scenario_filter = [s.strip() for s in include_ids.split(",") if s.strip()] if include_ids else []
    scenario_ids = _scenario_ids_for_grafana(run_faults, run_no_faults)
    scenario_val = scenario_filter[0] if scenario_filter else (scenario_ids[0] if scenario_ids else "")
    compare_short = (commit_sha or "")[:8]

    job_start_ms = int(stop_watch.start_time * 1000)
    job_end_ms = int((stop_watch.start_time + stop_watch.duration + 300) * 1000)

    def _url(uid, params):
        return f"{GRAFANA_BASE}/d/{uid}?{urlencode(params, doseq=True)}"

    p_details = {
        "orgId": "1",
        "from": str(job_start_ms),
        "to": str(job_end_ms),
        "timezone": "utc",
        "var-backend": "$__all",
        "var-branch": branch or "master",
        "var-commit_sha": commit_sha or "$__all",
    }
    if scenario_ids:
        p_details["var-scenario"] = scenario_ids
    result.set_clickable_label(
        "Grafana: Run details (this run)",
        _url(GRAFANA_DASH_UID["run_details"], p_details),
    )

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

    # Build RaftKeeper Docker image if the raftkeeper backend is enabled.
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

    # Set by the job command (nightly CI) or by set_default_env (PR mode).
    RUN_FAULT_TESTS = os.environ["KEEPER_RUN_FAULT_TESTS"].lower() == "true"
    RUN_NO_FAULT_TESTS = os.environ["KEEPER_RUN_NO_FAULT_TESTS"].lower() == "true"

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

    if RUN_NO_FAULT_TESTS:
        results.append(_run_pytest("core_no_faults.yaml", "_no_faults", "Keeper Stress (no-faults)"))

    if RUN_FAULT_TESTS:
        results.append(_run_pytest("core_faults.yaml", "_faults", "Keeper Stress (with-faults)"))

    pytest_ok = bool(results) and all(r.is_ok() for r in results)

    metrics_ok = validate_metrics_jsonl(sidecar)
    if not metrics_ok:
        pytest_ok = False

    Shell.run(f"chmod -R a+rX {REPO_DIR}/tests/stress/keeper/tests/_instances-* || true")

    job_log = f"{TEMP_DIR}/job.log"
    to_attach = [job_log, sidecar]
    if RUN_NO_FAULT_TESTS:
        to_attach.extend([
            f"{TEMP_DIR}/keeper_junit_no_faults.xml",
            f"{TEMP_DIR}/keeper_pytest_no_faults.log",
        ])
    if RUN_FAULT_TESTS:
        to_attach.extend([
            f"{TEMP_DIR}/keeper_junit_faults.xml",
            f"{TEMP_DIR}/keeper_pytest_faults.log",
        ])
    max_mb = env_int("KEEPER_ATTACH_PYTEST_JSONL_MAX_MB", 512)
    report_files = []
    if RUN_NO_FAULT_TESTS:
        report_files.append(f"{TEMP_DIR}/pytest_no_faults.jsonl")
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
        run_faults=RUN_FAULT_TESTS,
        run_no_faults=RUN_NO_FAULT_TESTS,
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
