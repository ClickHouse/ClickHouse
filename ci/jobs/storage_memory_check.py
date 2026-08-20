#!/usr/bin/env python3
"""Storage scenario memory profiler CI check.

Runs a stateful sequence of SQL scenarios with the PR and master
`clickhouse-examples` binaries, then compares each checkpoint-to-checkpoint
live-allocation delta.
"""

import glob
import os
import shutil
import subprocess
from pathlib import Path

from ci.jobs.parser_memory_check import (
    CHANGE_THRESHOLD_BYTES,
    CHANGE_THRESHOLD_PCT,
    analyze_heap_profiles,
    batch_symbolize,
    build_cross_version_diff_flamegraph_inputs,
    cleanup_heap_profiles,
    compute_cross_version_diff,
    download_master_binary,
    generate_html_report,
    get_merge_base_profiler_url,
)
from ci.praktika.result import Result
from ci.praktika.utils import Utils

TEMP_DIR = f"{Utils.cwd()}/ci/tmp"
SCENARIOS_DIR = Path(Utils.cwd()) / "utils/storage-memory-profiler/scenarios"


def find_heap_profile(profiles_dir: Path, prefix: str, checkpoint: str) -> str:
    matches = glob.glob(str(profiles_dir / f"{prefix}{checkpoint}.*.heap"))
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected one heap profile for {checkpoint}, found {len(matches)}"
        )
    return matches[0]


def run_scenarios(binary_path: str, version: str, scenarios: list[Path]) -> dict:
    profiles_dir = Path(TEMP_DIR) / f"storage-memory-{version}-profiles"
    data_dir = Path(TEMP_DIR) / f"storage-memory-{version}-data"
    for path in (profiles_dir, data_dir):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True)

    prefix = f"{version}_"
    args = [
        binary_path,
        "storage_memory_profiler",
        "--output-dir",
        str(profiles_dir),
        "--path",
        str(data_dir),
        "--prefix",
        prefix,
    ]
    for scenario in scenarios:
        args.extend(["--file", str(scenario)])

    env = os.environ.copy()
    malloc_conf = (
        "prof:true,prof_active:true,prof_thread_active_init:true,lg_prof_sample:0"
    )
    env["JE_MALLOC_CONF"] = malloc_conf
    env["MALLOC_CONF"] = malloc_conf

    try:
        completed = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=1800,
            env=env,
        )
    except subprocess.TimeoutExpired:
        return {"error": "scenario profiler timed out"}

    if completed.returncode != 0:
        return {
            "error": (
                f"scenario profiler exited with code {completed.returncode}\n"
                f"{completed.stderr[-4000:]}"
            )
        }

    checkpoints = {}
    for line in completed.stdout.splitlines():
        fields = line.split("\t")
        if len(fields) != 4 or fields[0] in ("checkpoint", "start"):
            continue
        try:
            checkpoints[fields[0]] = {
                "allocated_bytes": int(fields[1]),
                "diff_from_start": int(fields[2]),
                "diff_from_previous": int(fields[3]),
            }
        except ValueError:
            return {"error": f"Malformed checkpoint line: {line}"}

    expected_labels = [
        f"after_{index:02d}_{scenario.stem}"
        for index, scenario in enumerate(scenarios, start=1)
    ]
    missing = [label for label in expected_labels if label not in checkpoints]
    if missing:
        return {"error": f"Missing checkpoints: {', '.join(missing)}"}

    try:
        heap_files = [find_heap_profile(profiles_dir, prefix, "start")]
        heap_files.extend(
            find_heap_profile(profiles_dir, prefix, label) for label in expected_labels
        )
    except RuntimeError as ex:
        return {"error": str(ex)}

    return {
        "error": None,
        "checkpoints": checkpoints,
        "heap_files": heap_files,
        "profiles_dir": str(profiles_dir),
        "data_dir": str(data_dir),
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def make_error_result(name: str, error: str) -> Result:
    return Result(name=name, status=Result.Status.ERROR, info=error)


def main():
    stopwatch = Utils.Stopwatch()
    setup_results = []
    pr_binary = f"{TEMP_DIR}/clickhouse-examples"
    master_binary = f"{TEMP_DIR}/clickhouse-examples-master"
    scenarios = sorted(SCENARIOS_DIR.glob("*.sql"))

    if not Path(pr_binary).exists():
        setup_results.append(
            Result(
                name="Check PR binary",
                status=Result.Status.FAIL,
                info=f"PR binary not found at {pr_binary}",
            )
        )
        Result.create_from(results=setup_results, stopwatch=stopwatch).complete_job()
        return

    if not scenarios:
        setup_results.append(
            Result(
                name="Load scenarios",
                status=Result.Status.FAIL,
                info=f"No SQL scenarios found in {SCENARIOS_DIR}",
            )
        )
        Result.create_from(results=setup_results, stopwatch=stopwatch).complete_job()
        return

    os.chmod(pr_binary, 0o755)
    setup_results.append(Result(name="Check PR binary", status=Result.Status.OK))

    master_url = get_merge_base_profiler_url()
    if not master_url:
        setup_results.append(
            make_error_result(
                "Resolve master binary",
                "No master `clickhouse-examples` artifact was found",
            )
        )
        Result.create_from(results=setup_results, stopwatch=stopwatch).complete_job()
        return

    download_error = download_master_binary(master_url, master_binary)
    if download_error:
        setup_results.append(
            Result(
                name="Download master binary",
                status=Result.Status.FAIL,
                info=download_error,
            )
        )
        Result.create_from(results=setup_results, stopwatch=stopwatch).complete_job()
        return
    setup_results.append(Result(name="Download master binary", status=Result.Status.OK))

    master_run = run_scenarios(master_binary, "master", scenarios)
    pr_run = run_scenarios(pr_binary, "pr", scenarios)
    if master_run["error"] or pr_run["error"]:
        setup_results.append(
            make_error_result(
                "Run storage scenarios",
                f"master: {master_run['error'] or 'ok'}\nPR: {pr_run['error'] or 'ok'}",
            )
        )
        Result.create_from(results=setup_results, stopwatch=stopwatch).complete_job()
        return
    setup_results.append(Result(name="Run storage scenarios", status=Result.Status.OK))

    if not batch_symbolize(master_binary, master_run["heap_files"]):
        setup_results.append(
            Result(
                name="Symbolize master profiles",
                status=Result.Status.FAIL,
                info="See job log",
            )
        )
        Result.create_from(results=setup_results, stopwatch=stopwatch).complete_job()
        return
    if not batch_symbolize(pr_binary, pr_run["heap_files"]):
        setup_results.append(
            Result(
                name="Symbolize PR profiles",
                status=Result.Status.FAIL,
                info="See job log",
            )
        )
        Result.create_from(results=setup_results, stopwatch=stopwatch).complete_job()
        return
    setup_results.append(Result(name="Batch symbolization", status=Result.Status.OK))

    scenario_results = []
    html_data = []
    total_master = 0
    total_pr = 0
    regressions = 0
    improvements = 0
    errors = 0

    for index, scenario in enumerate(scenarios, start=1):
        master_analysis = analyze_heap_profiles(
            master_run["heap_files"][index - 1],
            master_run["heap_files"][index],
        )
        pr_analysis = analyze_heap_profiles(
            pr_run["heap_files"][index - 1],
            pr_run["heap_files"][index],
        )
        master_bytes = master_analysis["heap_diff"]
        pr_bytes = pr_analysis["heap_diff"]
        change = pr_bytes - master_bytes
        total_master += master_bytes
        total_pr += pr_bytes

        absolute_change = abs(change)
        baseline = abs(master_bytes)
        percent_change = (
            absolute_change / baseline * 100
            if baseline
            else (100.0 if absolute_change else 0.0)
        )
        significant = (
            absolute_change > CHANGE_THRESHOLD_BYTES
            and percent_change > CHANGE_THRESHOLD_PCT
        )
        if significant and change > 0:
            status = Result.Status.FAIL
            regressions += 1
        else:
            status = Result.Status.OK
            if significant and change < 0:
                improvements += 1

        master_stacks = master_analysis["stack_diffs"]
        pr_stacks = pr_analysis["stack_diffs"]
        cross_diff = compute_cross_version_diff(master_stacks, pr_stacks)
        cross_master, cross_pr = build_cross_version_diff_flamegraph_inputs(
            master_stacks, pr_stacks
        )
        info = (
            f"Scenario: {scenario.name}\n"
            f"Live allocation delta: master={master_bytes:+,} bytes, "
            f"PR={pr_bytes:+,} bytes, change={change:+,} bytes "
            f"({percent_change:.1f}%)"
        )
        scenario_results.append(Result(name=scenario.name, status=status, info=info))
        html_data.append(
            {
                "num": index,
                "query": scenario.name,
                "query_display": scenario.name,
                "master_bytes": master_bytes,
                "pr_bytes": pr_bytes,
                "change": change,
                "status": status,
                "master_stacks": master_stacks,
                "pr_stacks": pr_stacks,
                "cross_diff": cross_diff,
                "cross_diff_collapsed_master": cross_master,
                "cross_diff_collapsed_pr": cross_pr,
                "collapsed_pr": pr_analysis["collapsed"],
                "collapsed_master": master_analysis["collapsed"],
            }
        )

    report_path = f"{TEMP_DIR}/storage_memory_report.html"
    generate_html_report(
        html_data,
        total_master,
        total_pr,
        regressions,
        improvements,
        errors,
        stopwatch.duration,
        report_path,
        report_title="Storage Memory Scenario Check Report",
        report_subtitle=(
            "Measuring live-memory changes across stateful storage scenarios."
        ),
        item_label="Scenario",
    )

    cleanup_heap_profiles(master_run["profiles_dir"])
    cleanup_heap_profiles(pr_run["profiles_dir"])
    shutil.rmtree(master_run["data_dir"])
    shutil.rmtree(pr_run["data_dir"])

    test_status = Result.Status.FAIL if regressions or errors else Result.Status.OK
    tests = Result.create_from(
        name="Tests",
        results=scenario_results,
        status=test_status,
        info=(
            f"Scenarios: {len(scenarios)}, master total: {total_master:+,} bytes, "
            f"PR total: {total_pr:+,} bytes, change: {total_pr - total_master:+,} bytes, "
            f"regressions: {regressions}, improvements: {improvements}"
        ),
    )
    setup_results.append(tests)
    Result.create_from(
        results=setup_results,
        stopwatch=stopwatch,
        files=[report_path],
    ).complete_job()


if __name__ == "__main__":
    main()
