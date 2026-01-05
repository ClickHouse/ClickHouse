#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import time
import json
from pathlib import Path
import xml.etree.ElementTree as ET
import requests

# Ensure local invocations can import praktika without requiring PYTHONPATH
try:
    _repo_dir = str(Path(__file__).resolve().parents[2])
    if _repo_dir not in sys.path:
        sys.path.insert(0, _repo_dir)
    _ci_dir = os.path.join(_repo_dir, "ci")
    if _ci_dir not in sys.path:
        sys.path.insert(0, _ci_dir)
except Exception:
    pass

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
    try:
        _repo = str(Path(__file__).resolve().parents[2])
        _envf = os.path.join(_repo, "ci", "tmp", "praktika_setup_env.sh")
        if os.path.exists(_envf):
            with open(_envf, "r", encoding="utf-8") as _f:
                for _line in _f:
                    _s = _line.strip()
                    if not _s or _s.startswith("#"):
                        continue
                    if _s.startswith("export "):
                        _s = _s[7:]
                    if "=" in _s:
                        _k, _v = _s.split("=", 1)
                        _v = _v.strip().strip("'").strip('"')
                        if _k and _v and _k not in os.environ:
                            os.environ[_k.strip()] = _v
    except Exception:
        pass
    # Ensure weekly selection defaults depend on workflow: PR disables weekly, Nightly enables
    wf = os.environ.get("WORKFLOW_NAME", "")
    jn = os.environ.get("JOB_NAME", "")
    ghe = os.environ.get("GITHUB_EVENT_NAME", "")
    ref = os.environ.get("GITHUB_REF", "")
    is_pr = (wf == "PR") or ("(PR)" in jn) or ghe.startswith("pull_request") or ref.startswith("refs/pull/")
    # Same suite and defaults for PR and nightly
    os.environ.setdefault("KEEPER_INCLUDE_IDS", "")
    os.environ.setdefault("KEEPER_SCENARIO_FILE", "all")
    os.environ.setdefault("KEEPER_BENCH_ADAPTIVE", "1")
    os.environ.setdefault("KEEPER_BENCH_CLIENTS", "96")
    os.environ.setdefault("KEEPER_DEFAULT_P99_MS", "1000")
    os.environ.setdefault("KEEPER_DEFAULT_ERROR_RATE", "0.01")
    os.environ.setdefault("KEEPER_ADAPT_TARGET_P99_MS", "600")
    os.environ.setdefault("KEEPER_ADAPT_MAX_ERROR", "0.01")
    os.environ.setdefault("KEEPER_ADAPT_STAGE_S", "15")
    os.environ.setdefault("KEEPER_ADAPT_MIN_CLIENTS", "8")
    os.environ.setdefault("KEEPER_ADAPT_MAX_CLIENTS", "192")
    # Always keep containers/logs on fail for local/CI triage unless explicitly disabled
    os.environ.setdefault("KEEPER_KEEP_ON_FAIL", "1")
    # Enable faults by default on CI; can be overridden via env/CLI
    os.environ.setdefault("KEEPER_FAULTS", "on")
    os.environ.setdefault("KEEPER_DURATION", "600")
    # Run both default and rocks backends by default to compare behavior
    os.environ.setdefault("KEEPER_MATRIX_BACKENDS", "default,rocks")
    # Default per-worker port step for safe xdist parallelism (only used if -n is enabled)
    os.environ.setdefault("KEEPER_XDIST_PORT_STEP", "100")
    os.environ.setdefault("KEEPER_COMPOSE_DOWN_TIMEOUT", "30")
    os.environ.setdefault("KEEPER_CLEAN_ARTIFACTS", "1")
    os.environ.setdefault("CI_HEARTBEAT_SEC", "60")
    # Default to running tests with pytest-xdist workers (can be overridden)
    os.environ.setdefault("KEEPER_PYTEST_XDIST", "auto")
    # Derive an extended connection window for slow initializations when logs show progress
    try:
        _ready = int(os.environ.get("KEEPER_READY_TIMEOUT", "600") or "600")
        os.environ.setdefault("KEEPER_CONNECT_TIMEOUT_SEC", str(_ready + 600))
    except Exception:
        pass

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
    files_to_attach = []

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
        for i in range(90):
            if Shell.check("docker info > /dev/null", verbose=True):
                break
            time.sleep(2)
        if not Shell.check("docker info > /dev/null", verbose=True):
            results.append(
                Result.from_commands_run(
                    name="Docker startup failed",
                    command=[
                        "ps ax | grep dockerd | grep -v grep || true",
                        "ls -l ./ci/tmp/docker-in-docker.log || true",
                        "tail -n 200 ./ci/tmp/docker-in-docker.log || true",
                        "df -h || true",
                    ],
                )
            )
            Result.create_from(results=results, stopwatch=stop_watch).complete_job()
            return

    results.append(Result.from_commands_run(name="Disk space preflight", command=["df -h || true"]))
    results.append(
        Result.from_commands_run(
            name="Docker aggressive prune",
            command=[
                "docker system prune -af --volumes || true",
                "docker builder prune -af || true",
                "docker image prune -af || true",
            ],
        )
    )
    results.append(
        Result.from_commands_run(
            name="Docker pre-clean",
            command=[
                "docker network prune -f || true",
                "docker container prune -f || true",
            ],
        )
    )

    # Respect optional duration override from CLI first, then env
    dur_cli = args.duration
    dur_env = os.environ.get("KEEPER_DURATION")
    dur_arg = f" --duration={int(dur_cli)}" if dur_cli else (f" --duration={int(dur_env)}" if dur_env else "")

    # Install Python dependencies required by Keeper stress framework (PyYAML, etc.)
    install_cmd = (
        # Ensure deterministic pytest stack compatible with --report-log
        "PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --no-cache-dir -r tests/stress/keeper/requirements.txt "
        "&& PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --no-cache-dir 'pytest<9' pytest-xdist pytest-timeout pytest-reportlog boto3 PyGithub unidiff "
        "|| PIP_BREAK_SYSTEM_PACKAGES=1 python3 -m pip install --no-cache-dir pyyaml requests 'pytest<9' pytest-timeout pytest-xdist pytest-reportlog boto3 PyGithub unidiff"
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
                # Skip cp if source and destination resolve to the same file
                f"[ \"$(readlink -f {ch_path})\" = \"$(readlink -f {final_ch})\" ] && echo 'ClickHouse binary already installed at {final_ch}' || cp -f {ch_path} {final_ch}",
                f"chmod +x {final_ch} || true",
                f"{final_ch} --version",
                f"ln -sf {final_ch} /usr/local/bin/clickhouse-client || true",
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
        ready_val = int(os.environ.get("KEEPER_READY_TIMEOUT", 600))
    except Exception:
        ready_val = 600
    if ready_val < 600:
        ready_val = 600
    timeout_val = max(180, min(1800, dur_val + ready_val + 120))
    extra.append(f"--timeout={timeout_val}")
    extra.append("--timeout-method=signal")
    try:
        cpus = os.cpu_count() or 8
        heuristic_workers = str(min(16, max(6, cpus // 2)))
    except Exception:
        heuristic_workers = "12"
    _xd_env = (os.environ.get("KEEPER_PYTEST_XDIST", "").strip() or "").lower()
    xdist_workers = heuristic_workers if (_xd_env in ("", "auto")) else _xd_env
    extra.append(f"-n {xdist_workers}")
    try:
        is_parallel = xdist_workers not in ("1", "no", "0")
    except Exception:
        is_parallel = True
    report_file = f"{temp_dir}/pytest.jsonl"
    junit_file = f"{temp_dir}/keeper_junit.xml"
    extra.append(f"--junitxml={junit_file}")
    extra.append(f"--report-log={report_file}")
    base = ["-vv", tests_target, f"--durations=0{dur_arg}"]
    if is_parallel:
        extra.extend(["-o", "log_cli=false", "-o", "log_level=WARNING", "-p", "no:cacheprovider", "--max-worker-restart=2", "--dist", "load"])
        cmd = (" ".join(base + extra)).rstrip()
    else:
        cmd = (" ".join(["-s"] + base + extra)).rstrip()

    # Prepare env for pytest
    env = os.environ.copy()
    try:
        if not env.get("CI_DB_URL"):
            try:
                from tests.ci.clickhouse_helper import ClickHouseHelper  # type: ignore
                _h = ClickHouseHelper()
                if getattr(_h, "url", None):
                    env["CI_DB_URL"] = _h.url
            except Exception:
                pass
    except Exception:
        pass
    env["KEEPER_PYTEST_TIMEOUT"] = str(timeout_val)
    try:
        subproc_to = str(max(300, min(timeout_val - 60, 900)))
    except Exception:
        subproc_to = "900"
    env["KEEPER_SUBPROC_TIMEOUT"] = subproc_to
    env["KEEPER_READY_TIMEOUT"] = str(ready_val)
    # IMPORTANT: containers launched by docker-compose will bind-mount this exact host path
    # Mount the installed binary to avoid noexec on repo mounts and ensure parity with host-side tools
    server_bin_for_mount = ch_path
    env["CLICKHOUSE_BINARY"] = ch_path
    env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = ch_path
    env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = server_bin_for_mount
    env["CLICKHOUSE_BINARY"] = ch_path
    env.setdefault("CLICKHOUSE_TESTS_BASE_CONFIG_DIR", f"{repo_dir}/programs/server")
    env["PATH"] = f"/usr/local/bin:{env.get('PATH','')}"
    # Ensure ClickHouseCluster uses the same readiness window when waiting for instances
    env.setdefault("KEEPER_START_TIMEOUT_SEC", str(ready_val))
    env.setdefault("KEEPER_SCENARIO_FILE", "all")
    # Use default DB for Keeper metrics to avoid DB creation permissions issues
    env.setdefault("KEEPER_METRICS_DB", "keeper_stress_tests")
    # Ensure repo root and ci/ are on PYTHONPATH so 'tests' and 'praktika' can be imported
    repo_pythonpath = f"{repo_dir}:{repo_dir}/ci"
    cur_pp = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        repo_pythonpath if not cur_pp else f"{repo_pythonpath}:{cur_pp}"
    )
    # Propagate commit sha for tagging metrics/checks with robust fallbacks
    try:
        need = (not env.get("COMMIT_SHA")) or env.get("COMMIT_SHA") in ("", "local")
        if need:
            sha = (
                os.environ.get("GITHUB_SHA")
                or os.environ.get("GITHUB_HEAD_SHA")
                or os.environ.get("SHA")
                or ""
            )
            # Try to resolve via PRInfo if GH envs are not present
            if not sha:
                try:
                    from tests.ci.pr_info import PRInfo  # type: ignore

                    pr = PRInfo()
                    sha = getattr(pr, "sha", "") or sha
                except Exception:
                    pass
            if not sha:
                try:
                    out = subprocess.check_output(
                        ["git", "rev-parse", "HEAD"],
                        cwd=repo_dir,
                        stderr=subprocess.STDOUT,
                        text=True,
                    )
                    sha = (out or "").strip()
                except Exception:
                    sha = ""
            env["COMMIT_SHA"] = sha if sha else "local"
        # Also pass it explicitly to pytest to ensure run_meta tagging
        try:
            if env.get("COMMIT_SHA") and "--commit-sha=" not in cmd:
                cmd = f"{cmd} --commit-sha={env['COMMIT_SHA']}"
        except Exception:
            pass
    except Exception:
        pass
    # Light preflight to aid debugging: echo has_ci_sink and COMMIT_SHA
    try:
        pre_cmd = (
            "python3 - <<'PY'\n"
            "import os,sys\n"
            f"sys.path.insert(0, '{repo_dir}')\n"
            f"sys.path.insert(0, '{repo_dir}/ci')\n"
            "try:\n"
            "    from tests.stress.keeper.framework.io.sink import has_ci_sink\n"
            "    print('keeper_preflight has_ci_sink=', has_ci_sink())\n"
            "except Exception as e:\n"
            "    print('keeper_preflight has_ci_sink=error', e)\n"
            "print('keeper_preflight COMMIT_SHA=', os.environ.get('COMMIT_SHA'))\n"
            "print('keeper_preflight GITHUB_SHA=', os.environ.get('GITHUB_SHA',''))\n"
            "PY\n"
        )
        results.append(Result.from_commands_run(name="Keeper CIDB preflight", command=pre_cmd))
    except Exception:
        pass
    # Avoid pytest collecting generated _instances-* dirs which may be non-readable
    try:
        addopts = env.get("PYTEST_ADDOPTS", "").strip()
        fh_to = str(max(600, min(timeout_val - 60, 1800)))
        extra_opts = [
            "--ignore-glob=tests/stress/keeper/tests/_instances-*",
            "-o",
            f"faulthandler_timeout={fh_to}",
        ]
        for opt in [" ".join(extra_opts)]:
            if opt not in addopts:
                addopts = (addopts + " " + opt).strip()
        env["PYTEST_ADDOPTS"] = addopts
    except Exception:
        pass

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

    try:
        per_test = timeout_val
        pytest_proc_timeout = max(3600, min(10200, max(9000, per_test * 4)))
    except Exception:
        pytest_proc_timeout = 9000
    pytest_log_file = f"{temp_dir}/pytest_stdout.log"
    results.append(
        Result.from_pytest_run(
            command=cmd,
            cwd=repo_dir,
            name="Keeper Stress",
            env=env,
            pytest_report_file=report_file,
            logfile=pytest_log_file,
            timeout_seconds=pytest_proc_timeout,
        )
    )
    pytest_result_ok = results[-1].is_ok()
    # Best-effort: ensure keeper artifacts are world-readable for attachment/triage
    try:
        results.append(
            Result.from_commands_run(
                name="Fix permissions for keeper artifacts",
                command=[
                    f"chmod -R a+rX {repo_dir}/tests/stress/keeper/tests/_instances-* || true",
                ],
            )
        )
    except Exception:
        pass
    # Post-run: extract keeper metrics from pytest stdout for artifacts only (tests handle CIDB push like other suites)
    try:
        pushed_rows = 0
        extracted_rows = 0
        push_err = ""
        helper = None
        try:
            from tests.ci.clickhouse_helper import ClickHouseHelper  # type: ignore
            helper = ClickHouseHelper()
        except Exception:
            helper = None
        metrics_db = env.get("KEEPER_METRICS_DB", "keeper_stress_tests")
        # No job-level DDL or post-run insert; schema is pre-provisioned and tests push via ClickHouseHelper when creds are present
        if Path(pytest_log_file).exists():
            rows = []
            inside = False
            extracted_path = f"{temp_dir}/keeper_metrics_extracted.jsonl"
            try:
                ef = open(extracted_path, "w", encoding="utf-8")
            except Exception:
                ef = None
            try:
                with open(pytest_log_file, "r", encoding="utf-8", errors="ignore") as lf:
                    for line in lf:
                        s = line.strip()
                        if s == "[keeper][push-metrics] begin":
                            inside = True
                            continue
                        if s == "[keeper][push-metrics] end":
                            inside = False
                            continue
                        if not inside:
                            continue
                        if not s or s[0] != "{" or "value" not in s:
                            continue
                        try:
                            obj = json.loads(s)
                            if all(k in obj for k in ("run_id", "commit_sha", "backend", "scenario", "node", "stage", "source", "name", "value")):
                                rows.append(s)
                                extracted_rows += 1
                                if ef:
                                    try:
                                        ef.write(s + "\n")
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        if len(rows) >= 1000:
                            rows = []
                if rows:
                    rows = []
            finally:
                try:
                    if ef:
                        ef.close()
                except Exception:
                    pass
            try:
                files_to_attach.append(extracted_path)
            except Exception:
                pass
            try:
                results.append(
                    Result.from_commands_run(
                        name="Keeper Metrics Extracted Preview",
                        command=(
                            "bash -lc \"echo '==== Keeper metrics (extracted) preview ===='; "
                            f"sed -n '1,200p' '{extracted_path}'\""
                        ),
                    )
                )
            except Exception:
                pass
        results.append(
            Result.from_commands_run(
                name=f"Keeper Metrics Extract (post-run): extracted={extracted_rows} (tests push to CIDB)",
                command=["true"],
            )
        )
    except Exception:
        pass
    try:
        for p in [pytest_log_file, report_file, junit_file]:
            try:
                if Path(p).exists():
                    files_to_attach.append(str(p))
            except Exception:
                pass
    except Exception:
        pass
    try:
        tests = failures = errors = skipped = 0
        summary_txt = None
        try:
            p = Path(junit_file)
            if p.exists():
                root = ET.parse(str(p)).getroot()
                suites = []
                if root.tag == "testsuite":
                    suites = [root]
                else:
                    suites = list(root.findall("testsuite"))
                for ts in suites:
                    tests += int(ts.attrib.get("tests", 0) or 0)
                    failures += int(ts.attrib.get("failures", 0) or 0)
                    errors += int(ts.attrib.get("errors", 0) or 0)
                    skipped += int(ts.attrib.get("skipped", 0) or 0)
        except Exception:
            pass
        passed = max(0, tests - failures - errors - skipped)
        try:
            summary_txt = f"{temp_dir}/keeper_summary.txt"
            with open(summary_txt, "w", encoding="utf-8") as f:
                f.write(
                    f"Passed: {passed}, Failed: {failures}, Skipped: {skipped}, Errors: {errors}, Total: {tests}\n"
                )
        except Exception:
            summary_txt = None
        results.append(
            Result.from_commands_run(
                name=f"Keeper Test Summary: Passed {passed}, Failed {failures}, Skipped {skipped}, Errors {errors}, Total {tests}",
                command=["true"],
            )
        )
    except Exception:
        pass
    try:
        try:
            files_to_attach
        except NameError:
            files_to_attach = []
        cidb_txt = f"{temp_dir}/keeper_cidb_verify.txt"
        sha = env.get("COMMIT_SHA") or env.get("GITHUB_SHA") or "local"
        backends_counts = {}
        bench_rows = 0
        checks_rows = 0
        # Resolve CIDB endpoint: prefer env CI_DB_URL override for URL, credentials via ClickHouseHelper
        try:
            from tests.ci.clickhouse_helper import ClickHouseHelper  # type: ignore
            _h = None
            try:
                _h = helper if 'helper' in locals() else None
            except Exception:
                _h = None
            if _h is None:
                try:
                    _h = ClickHouseHelper()
                except Exception:
                    _h = None
            cidb_url = env.get("CI_DB_URL") or (getattr(_h, "url", "") if _h is not None else "")
            auth = getattr(_h, "auth", None) if _h is not None else None
        except Exception:
            cidb_url = ""
            auth = None
        have_cidb = bool(cidb_url and auth and auth.get("X-ClickHouse-User") and auth.get("X-ClickHouse-Key"))
        metrics_db = env.get("KEEPER_METRICS_DB", "keeper_stress_tests")
        if have_cidb:
            try:
                if sha and sha != "local":
                    filt_metrics = f"commit_sha = '{sha}' AND ts > now() - INTERVAL 2 DAY"
                else:
                    # Fallback: recent window without sha filter (covers 'local' runs)
                    filt_metrics = "ts > now() - INTERVAL 2 DAY"
                q = (
                    f"SELECT backend, count() FROM {metrics_db}.keeper_metrics_ts "
                    f"WHERE {filt_metrics} "
                    "GROUP BY backend ORDER BY backend FORMAT TabSeparated"
                )
                r = requests.get(cidb_url, params={"query": q}, headers=auth, timeout=30)
                txt = (r.text or "").strip()
                for line in txt.splitlines():
                    parts = line.split("\t")
                    if len(parts) == 2:
                        backends_counts[parts[0]] = int(float(parts[1]))
            except Exception:
                pass
            try:
                if sha and sha != "local":
                    filt_bench = f"commit_sha = '{sha}' AND source = 'bench' AND ts > now() - INTERVAL 2 DAY"
                else:
                    filt_bench = "source = 'bench' AND ts > now() - INTERVAL 2 DAY"
                q = (
                    f"SELECT count() FROM {metrics_db}.keeper_metrics_ts "
                    f"WHERE {filt_bench} FORMAT TabSeparated"
                )
                r = requests.get(cidb_url, params={"query": q}, headers=auth, timeout=30)
                t2 = (r.text or "").strip()
                bench_rows = int(float(t2.splitlines()[0])) if t2 else 0
            except Exception:
                pass
            try:
                # Use case-insensitive matching on check_name to cover 'Keeper Stress' and 'Keeper Stress (PR)'
                name_filter = (
                    "(lowerUTF8(check_name) LIKE 'keeper stress:%' OR lowerUTF8(check_name) LIKE 'keeper stress%' OR "
                    "lowerUTF8(check_name) LIKE 'keeper_stress:%' OR lowerUTF8(check_name) LIKE 'keeper_stress%')"
                )
                if sha and sha != "local":
                    q = (
                        "SELECT count() FROM default.checks "
                        f"WHERE {name_filter} AND commit_sha = '{sha}' "
                        "AND check_start_time > now() - INTERVAL 2 DAY FORMAT TabSeparated"
                    )
                else:
                    q = (
                        "SELECT count() FROM default.checks "
                        f"WHERE {name_filter} "
                        "AND check_start_time > now() - INTERVAL 2 DAY FORMAT TabSeparated"
                    )
                r = requests.get(cidb_url, params={"query": q}, headers=auth, timeout=30)
                t3 = (r.text or "").strip()
                checks_rows = int(float(t3.splitlines()[0])) if t3 else 0
            except Exception:
                pass
            # Best-effort: capture small samples to aid triage when counts are zero
            try:
                cidb_samples = f"{temp_dir}/keeper_cidb_sample.txt"
                lines = []
                # Metrics sample (recent rows)
                try:
                    if sha and sha != "local":
                        filt = f"commit_sha = '{sha}' AND ts > now() - INTERVAL 2 DAY"
                    else:
                        filt = "ts > now() - INTERVAL 2 DAY"
                    q = (
                        "SELECT ts, run_id, backend, scenario, node, stage, source, name, value "
                        f"FROM {metrics_db}.keeper_metrics_ts "
                        f"WHERE {filt} ORDER BY ts DESC LIMIT 30 FORMAT TabSeparated"
                    )
                    r = requests.get(cidb_url, params={"query": q}, headers=auth, timeout=30)
                    mtxt = (r.text or "").strip()
                except Exception:
                    mtxt = ""
                lines.append("[metrics_sample]")
                if mtxt:
                    for l in mtxt.splitlines():
                        lines.append(l)
                else:
                    lines.append("<empty>")
                # Checks sample (recent rows)
                try:
                    if sha and sha != "local":
                        q = (
                            "SELECT check_start_time, check_name, check_status, test_name "
                            "FROM default.checks "
                            f"WHERE {name_filter} AND commit_sha = '{sha}' "
                            "AND check_start_time > now() - INTERVAL 2 DAY ORDER BY check_start_time DESC LIMIT 20 FORMAT TabSeparated"
                        )
                    else:
                        q = (
                            "SELECT check_start_time, check_name, check_status, test_name "
                            "FROM default.checks "
                            f"WHERE {name_filter} "
                            "AND check_start_time > now() - INTERVAL 2 DAY ORDER BY check_start_time DESC LIMIT 20 FORMAT TabSeparated"
                        )
                    r = requests.get(cidb_url, params={"query": q}, headers=auth, timeout=30)
                    ctxt = (r.text or "").strip()
                except Exception:
                    ctxt = ""
                lines.append("")
                lines.append("[checks_sample]")
                if ctxt:
                    for l in ctxt.splitlines():
                        lines.append(l)
                else:
                    lines.append("<empty>")
                # Write and attach
                try:
                    with open(cidb_samples, "w", encoding="utf-8") as sf:
                        sf.write("\n".join(lines) + "\n")
                    files_to_attach.append(cidb_samples)
                    # Also print a short preview to job log for quick triage
                    try:
                        results.append(
                            Result.from_commands_run(
                                name="CIDB Sample Preview",
                                command=(
                                    "bash -lc \"echo '==== CIDB recent metrics/checks sample ===='; "
                                    f"sed -n '1,200p' '{cidb_samples}'\""
                                ),
                            )
                        )
                    except Exception:
                        pass
                except Exception:
                    pass
            except Exception:
                pass
        present = ",".join(sorted(k for k in backends_counts.keys())) if backends_counts else ""
        with open(cidb_txt, "w", encoding="utf-8") as f:
            f.write(f"commit_sha={sha}\n")
            f.write(f"backends_counts={backends_counts}\n")
            f.write(f"bench_rows={bench_rows}\n")
            f.write(f"checks_rows={checks_rows}\n")
        results.append(
            Result.from_commands_run(
                name=f"CIDB Verify: backends [{present}] bench_rows={bench_rows} checks_rows={checks_rows}",
                command=["true"],
            )
        )
        try:
            files_to_attach.append(cidb_txt)
        except Exception:
            pass
        # Do not fail or gate on CIDB availability; align with other tests (best-effort reporting only)
        try:
            results.append(
                Result.from_commands_run(
                    name=f"CIDB Note: sha={sha} have_cidb={(1 if have_cidb else 0)} bench_rows={bench_rows} checks_rows={checks_rows}",
                    command=["true"],
                )
            )
        except Exception:
            pass
    except Exception:
        pass
    # Collect debug artifacts on failure
    try:
        files_to_attach
    except NameError:
        files_to_attach = []
    try:
        if 'summary_txt' in locals() and summary_txt:
            sp = Path(summary_txt)
            if sp.exists():
                files_to_attach.append(str(sp))
    except Exception:
        pass
    try:
        if not pytest_result_ok:
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
                    tail_cmds.append(f"echo '==== keeper{i} err ====' && tail -n 400 '{err}' || true")
                    tail_cmds.append(f"echo '==== keeper{i} log ====' && tail -n 400 '{log}' || true")
                # docker inventory and service logs (best-effort)
                tail_cmds.append("echo '==== docker ps (keepers) ====' && docker ps -a --format '{{.Names}}\t{{.Status}}\t{{.Image}}' | sed -n '1,200p'")
                tail_cmds.append("for n in $(docker ps --format '{{.Names}}' | grep -E 'keeper[0-9]+' || true); do echo '==== docker logs' $n '===='; docker logs --tail 400 $n || true; done")
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

    try:
        if 'cidb_txt' in locals():
            p = Path(cidb_txt)
            if p.exists() and str(p) not in files_to_attach:
                files_to_attach.append(str(p))
    except Exception:
        pass
    # Post-run docker prune to free space for subsequent jobs
    results.append(
        Result.from_commands_run(
            name="Docker post-clean",
            command=[
                "docker system prune -af --volumes || true",
                "docker network prune -f || true",
                "docker container prune -f || true",
            ],
        )
    )
    # Publish aggregated job result (with nested pytest results)
    Result.create_from(results=results, stopwatch=stop_watch, files=files_to_attach).complete_job()


if __name__ == "__main__":
    main()
