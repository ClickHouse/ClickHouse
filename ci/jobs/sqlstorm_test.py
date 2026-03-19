#!/usr/bin/env python3

import html
import json
import os
import sys
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"

# Thresholds based on baseline run with ClickHouse 26.3 and ClickHouse-dialect queries.
# The DBA StackOverflow dataset with ~18K SQLStorm queries.
# With the PostgreSQL -> ClickHouse query rewriter, ~60% of queries succeed.
MIN_TOTAL_QUERIES = 18_000
MIN_SUCCESS_RATE = 0.50  # at least 50% queries should succeed


class ClickHouseBinary:
    def __init__(self):
        self.path = temp_dir
        self.config_path = f"{temp_dir}/config"
        self.start_cmd = (
            f"{self.path}/clickhouse-server --config-file={self.config_path}/config.xml"
        )
        self.log_file = f"{temp_dir}/server.log"
        self.port = 9000

    def install(self):
        Utils.add_to_PATH(self.path)
        commands = [
            f"mkdir -p {self.config_path}/users.d",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {self.config_path}",
            f"cp -r --dereference ./programs/server/config.d {self.config_path}",
            f"chmod +x {self.path}/clickhouse",
            f"ln -sf {self.path}/clickhouse {self.path}/clickhouse-server",
            f"ln -sf {self.path}/clickhouse {self.path}/clickhouse-client",
        ]
        res = True
        for command in commands:
            res = res and Shell.check(command, verbose=True)
        return res

    def start(self):
        import subprocess
        import time

        print("Starting ClickHouse server")
        print("Command: ", self.start_cmd)
        self.log_fd = open(self.log_file, "w")
        self.proc = subprocess.Popen(
            self.start_cmd, stderr=subprocess.STDOUT, stdout=self.log_fd, shell=True
        )
        time.sleep(2)
        retcode = self.proc.poll()
        if retcode is not None:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False
        print("ClickHouse server process started -> wait ready")
        res = self.wait_ready()
        if res:
            print("ClickHouse server ready")
        else:
            print("ClickHouse server NOT ready")
        return res

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                f'clickhouse-client --port {self.port} --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print("Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        return True


def check_thresholds(stats):
    """Check that the benchmark results meet minimum thresholds."""
    lines = []
    passed = True

    total = stats.get("total", 0)
    success = stats.get("success", 0)
    success_rate = stats.get("success_rate", 0)

    if total < MIN_TOTAL_QUERIES:
        lines.append(
            f"FAILED: total queries {total:,} < minimum {MIN_TOTAL_QUERIES:,}"
        )
        passed = False
    else:
        lines.append(f"OK: total queries {total:,} >= minimum {MIN_TOTAL_QUERIES:,}")

    if success_rate < MIN_SUCCESS_RATE:
        lines.append(
            f"FAILED: success rate {success_rate:.1%} < minimum {MIN_SUCCESS_RATE:.1%}"
        )
        passed = False
    else:
        lines.append(
            f"OK: success rate {success_rate:.1%} >= minimum {MIN_SUCCESS_RATE:.1%}"
        )

    info = "; ".join(lines)
    return passed, info


def generate_html_report(stats, results, report_path, threshold_passed, threshold_info):
    """Generate an HTML report with benchmark results."""
    reset_color = "</b>"

    def color_tag(ratio):
        if ratio == 0:
            return "<b style='color: red;'>"
        elif ratio < 0.5:
            return "<b style='color: orange;'>"
        elif ratio < 1:
            return "<b style='color: gray;'>"
        else:
            return "<b style='color: green;'>"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(
            "<html><body><pre style='font-size: 16pt; padding: 1em; line-height: 1.25;'>\n"
        )

        # Threshold status
        if threshold_passed:
            f.write(f"<b style='color: green;'>Thresholds: PASSED{reset_color}\n")
        else:
            f.write(f"<b style='color: red;'>Thresholds: FAILED{reset_color}\n")
        f.write(f"  {html.escape(threshold_info)}\n\n")

        # Summary
        total = stats["total"]
        success = stats["success"]
        timeout = stats["timeout"]
        oom = stats["oom"]
        errors = stats["errors"]
        success_rate = stats["success_rate"]

        f.write("<b>Summary</b>\n")
        f.write(
            f"  Queries: {color_tag(success_rate)}"
            f"{success:,} / {total:,} ({success_rate:.1%}){reset_color}\n"
        )
        f.write(f"  Timeouts: {timeout:,}\n")
        f.write(f"  OOM: {oom:,}\n")
        f.write(f"  Errors: {errors:,}\n")
        f.write(f"  Median query time: {stats['median_ms']:.1f}ms\n")
        f.write(f"  Total query time: {stats['sum_ms']:.0f}ms\n\n")

        # Error categories breakdown
        error_categories = stats.get("error_categories", {})
        if error_categories:
            f.write("<b>Failure categories</b>\n")
            for cat, count in sorted(error_categories.items(), key=lambda x: -x[1]):
                pct = 100.0 * count / (total - success) if (total - success) > 0 else 0
                f.write(
                    f"  <b style='color: red;'>{count:>6,}</b>"
                    f"  {pct:5.1f}%  {html.escape(cat)}\n"
                )
            f.write("\n")

        # Slowest successful queries
        success_results = [r for r in results if r["state"] == "success"]
        if success_results:
            slowest = sorted(success_results, key=lambda r: -r["client_total_ms"])[:20]
            f.write("<b>Slowest successful queries (top 20)</b>\n")
            for r in slowest:
                f.write(
                    f"  {r['client_total_ms']:>10.1f}ms  query {html.escape(r['query'])}\n"
                )
            f.write("\n")

        # Sample errors (up to 5 per category)
        if error_categories:
            f.write("<b>Sample errors</b>\n")
            shown_per_cat = {}
            for r in results:
                if r["state"] == "success":
                    continue
                cat = r["state"]
                shown_per_cat.setdefault(cat, 0)
                if shown_per_cat[cat] >= 5:
                    continue
                shown_per_cat[cat] += 1
                msg = r["message"][:200].replace("\n", " ")
                if len(r["message"]) > 200:
                    msg += "..."
                f.write(
                    f"  [{html.escape(cat)}] query {html.escape(r['query'])}: "
                    f"<span style='color: gray;'>{html.escape(msg)}</span>\n"
                )
            f.write("\n")

        f.write("</pre></body></html>\n")

    print(f"HTML report written to {report_path}")


def main():
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseBinary()

    sqlstorm_dir = os.path.join(Utils.cwd(), "tests", "sqlstorm")
    sqlstorm_repo = os.path.join(temp_dir, "sqlstorm")
    olapbench_repo = os.path.join(temp_dir, "olapbench")
    data_dir = os.path.join(temp_dir, "sqlstorm_data")
    out_dir = os.path.join(temp_dir, "sqlstorm_output")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    # Step 1: Start ClickHouse
    print("Start ClickHouse")

    def start():
        return ch.install() and ch.start()

    results.append(
        Result.from_commands_run(
            name="Start ClickHouse",
            command=start,
        )
    )

    # Step 2: Clone SQLStorm repo (for queries)
    if results[-1].is_ok():
        print("Clone SQLStorm repo")

        # Use the ClickHouse fork with queries rewritten for ClickHouse dialect
        sqlstorm_commit = "clickhouse-compat"

        def clone_sqlstorm():
            if not Path(sqlstorm_repo).is_dir():
                if not Shell.check(
                    f"git clone --branch {sqlstorm_commit}"
                    f" https://github.com/ClickHouse/SQLStorm.git {sqlstorm_repo}",
                    verbose=True,
                ):
                    return False
            else:
                if not Shell.check(
                    f"git -C {sqlstorm_repo} fetch origin",
                    verbose=True,
                ):
                    return False
                if not Shell.check(
                    f"git -C {sqlstorm_repo} checkout {sqlstorm_commit}",
                    verbose=True,
                ):
                    return False
            return True

        results.append(
            Result.from_commands_run(
                name="Clone SQLStorm repo",
                command=clone_sqlstorm,
            )
        )

    # Step 3: Clone OLAPBench repo (for schema definition)
    if results[-1].is_ok():
        print("Clone OLAPBench repo")

        olapbench_commit = "0e23df49491e17b1f38b95da6bc803812d5b5a97"

        def clone_olapbench():
            if not Path(olapbench_repo).is_dir():
                if not Shell.check(
                    f"git clone https://github.com/SQL-Storm/OLAPBench.git {olapbench_repo}",
                    verbose=True,
                ):
                    return False
            else:
                if not Shell.check(
                    f"git -C {olapbench_repo} fetch origin",
                    verbose=True,
                ):
                    return False
            return Shell.check(
                f"git -C {olapbench_repo} checkout {olapbench_commit}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Clone OLAPBench repo",
                command=clone_olapbench,
            )
        )

    # Step 4: Download StackOverflow DBA dataset
    if results[-1].is_ok():
        print("Download StackOverflow DBA dataset")

        data_url = "http://db.in.tum.de/~schmidt/data/stackoverflow_dba.tar.gz"
        data_archive = os.path.join(data_dir, "stackoverflow_dba.tar.gz")
        extracted_dir = os.path.join(data_dir, "stackoverflow_dba")

        def download_data():
            if os.path.isdir(extracted_dir) and os.listdir(extracted_dir):
                print("Data already downloaded, skipping")
                return True
            if not Shell.check(
                f"wget -q -O {data_archive} {data_url}",
                verbose=True,
            ):
                return False
            if not Shell.check(
                f"tar -xzf {data_archive} -C {data_dir}",
                verbose=True,
            ):
                return False
            # Clean up archive to save disk space
            os.remove(data_archive)
            return True

        results.append(
            Result.from_commands_run(
                name="Download dataset",
                command=download_data,
            )
        )

    # Step 5: Run benchmark
    query_results = []
    stats = {}
    if results[-1].is_ok():
        print("Run SQLStorm benchmark")

        schema_path = os.path.join(
            olapbench_repo, "benchmarks", "stackoverflow", "stackoverflow.dbschema.json"
        )
        query_dir = os.path.join(sqlstorm_repo, "v1.0", "stackoverflow", "queries")

        def run_benchmark():
            nonlocal query_results, stats
            ok = Shell.check(
                f"python3 {sqlstorm_dir}/runner.py"
                f" --query-dir {query_dir}"
                f" --data-dir {extracted_dir}"
                f" --schema {schema_path}"
                f" --out-dir {out_dir}"
                f" --timeout 10"
                f" --port {ch.port}",
                verbose=True,
            )
            # Load results regardless of exit code
            results_path = os.path.join(out_dir, "results.json")
            stats_path = os.path.join(out_dir, "stats.json")
            if os.path.isfile(results_path):
                with open(results_path) as f:
                    query_results = json.load(f)
            if os.path.isfile(stats_path):
                with open(stats_path) as f:
                    stats = json.load(f)
            return True  # always succeed - thresholds are checked separately

        results.append(
            Result.from_commands_run(
                name="Run benchmark",
                command=run_benchmark,
            )
        )

    # Step 6: Generate report and check thresholds
    report_html_path = os.path.join(out_dir, "report.html")
    threshold_info = ""

    if results[-1].is_ok():
        print("Generate report and check thresholds")

        def report_and_thresholds():
            nonlocal threshold_info

            effective_stats = stats if stats else {
                "total": 0, "success": 0, "timeout": 0, "oom": 0,
                "errors": 0, "success_rate": 0, "median_ms": 0, "sum_ms": 0,
                "error_categories": {},
            }
            threshold_ok, threshold_info = check_thresholds(effective_stats)
            generate_html_report(
                effective_stats, query_results, report_html_path,
                threshold_ok, threshold_info,
            )
            return threshold_ok

        results.append(
            Result.from_commands_run(
                name="Report & Thresholds",
                command=report_and_thresholds,
            )
        )

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=[report_html_path] if os.path.isfile(report_html_path) else [],
        info=threshold_info,
    ).complete_job()


if __name__ == "__main__":
    main()
