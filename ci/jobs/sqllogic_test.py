#!/usr/bin/env python3

import html
import json
import os
import re
import sys
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp/"

MIN_TOTAL_TESTS = 5_939_581
MAX_FAILED_TESTS = 174_004


# Reuse the same ClickHouseBinary helper from sqltest_job.py
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


def load_stage_reports(out_dir, mode_name):
    """Load report.json files from each stage under a mode's output directory."""
    stages_dir = os.path.join(out_dir, mode_name, f"{mode_name}-stages")
    reports = {}
    if not os.path.isdir(stages_dir):
        print(f"Stages directory not found: {stages_dir}")
        return reports
    for entry in sorted(os.listdir(stages_dir)):
        report_path = os.path.join(stages_dir, entry, "report.json")
        if os.path.isfile(report_path):
            with open(report_path, "r", encoding="utf-8") as f:
                reports[entry] = json.load(f)
            print(f"Loaded report: {report_path}")
    return reports


MAX_EXAMPLES_PER_CATEGORY = 5


def classify_failures(report):
    """Classify failed requests from a stage report by reason category.
    Returns sorted list of (count, category, examples) tuples, descending by count.
    Each example is a query string (up to MAX_EXAMPLES_PER_CATEGORY per category)."""
    categories = {}
    examples = {}
    code_re = re.compile(r"Code: (\d+)")

    for test_data in report.get("tests", {}).values():
        for req in test_data.get("requests", {}).values():
            if req.get("status") != "error":
                continue
            reason = req.get("reason", "") or ""

            if "different hashes" in reason:
                cat = "Hash mismatch"
            elif "different value count" in reason:
                cat = "Row count mismatch"
            elif "different values" in reason:
                cat = "Value mismatch"
            elif "different exceptions" in reason:
                cat = "Exception mismatch"
            elif "canonic result has exception" in reason:
                cat = "Missing exception"
            elif "actual result has exception" in reason:
                cat = "Unexpected exception"
            elif "columns count differ" in reason:
                cat = "Column count mismatch"
            else:
                m = code_re.search(reason)
                if m:
                    cat = f"Code {m.group(1)}"
                else:
                    cat = "Other"

            categories[cat] = categories.get(cat, 0) + 1

            if cat not in examples:
                examples[cat] = []
            if len(examples[cat]) < MAX_EXAMPLES_PER_CATEGORY:
                query = req.get("request", "")
                if query:
                    examples[cat].append(query)

    return sorted(
        [(count, cat, examples.get(cat, [])) for cat, count in categories.items()],
        key=lambda x: -x[0],
    )


def check_thresholds(complete_test_reports):
    """Check that the complete-clickhouse stage meets success thresholds.
    Returns (passed, info_string)."""
    report = complete_test_reports.get("complete-clickhouse")
    if report is None:
        return False, "FAILED: complete-clickhouse report not found"

    stats = report.get("stats", {})
    total_stats = stats.get("total", {})
    total_success = total_stats.get("success", 0)
    total_fail = total_stats.get("fail", 0)
    total_tests = total_success + total_fail

    lines = []
    passed = True

    if total_tests < MIN_TOTAL_TESTS:
        lines.append(
            f"FAILED: total tests {total_tests:,} < minimum {MIN_TOTAL_TESTS:,}"
        )
        passed = False
    else:
        lines.append(f"OK: total tests {total_tests:,} >= minimum {MIN_TOTAL_TESTS:,}")

    if total_fail > MAX_FAILED_TESTS:
        lines.append(
            f"FAILED: failed tests {total_fail:,} > maximum {MAX_FAILED_TESTS:,}"
        )
        passed = False
    else:
        lines.append(
            f"OK: failed tests {total_fail:,} <= maximum {MAX_FAILED_TESTS:,}"
        )

    info = "; ".join(lines)
    return passed, info


def generate_html_report(all_reports, report_path, threshold_passed, threshold_info):
    """Generate an HTML report following the sqltest_job.py pattern."""
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
            f.write(
                f"<b style='color: green;'>Thresholds: PASSED{reset_color}\n"
            )
        else:
            f.write(
                f"<b style='color: red;'>Thresholds: FAILED{reset_color}\n"
            )
        f.write(f"  {html.escape(threshold_info)}\n\n")

        # Per-mode breakdown
        for mode_name, stage_reports in sorted(all_reports.items()):
            f.write(f"<b>{html.escape(mode_name)}</b>\n")

            for stage_name, report in sorted(stage_reports.items()):
                stats = report.get("stats", {})
                total = stats.get("total", {})
                success = total.get("success", 0)
                fail = total.get("fail", 0)
                total_count = success + fail

                if total_count == 0:
                    f.write(f"  {html.escape(stage_name)}: no tests\n")
                    continue

                ratio = success / total_count
                f.write(
                    f"  {html.escape(stage_name)}: {color_tag(ratio)}"
                    f"{success:,} / {total_count:,} ({ratio:.1%}){reset_color}\n"
                )

                # Statements/queries sub-breakdown
                for sub_key in ("statements", "queries"):
                    sub = stats.get(sub_key, {})
                    sub_success = sub.get("success", 0)
                    sub_fail = sub.get("fail", 0)
                    sub_total = sub_success + sub_fail
                    if sub_total == 0:
                        continue
                    sub_ratio = sub_success / sub_total
                    f.write(
                        f"    {sub_key}: {color_tag(sub_ratio)}"
                        f"{sub_success:,} / {sub_total:,} ({sub_ratio:.1%}){reset_color}\n"
                    )

                # Failure classification by reason
                classified = classify_failures(report)
                if classified:
                    f.write(f"    Failure categories:\n")
                    for count, cat, cat_examples in classified:
                        pct = 100.0 * count / fail if fail else 0
                        f.write(
                            f"      <b style='color: red;'>{count:>9,}</b>"
                            f"  {pct:5.1f}%  {html.escape(cat)}\n"
                        )
                        for example in cat_examples:
                            truncated = example[:200]
                            if len(example) > 200:
                                truncated += "..."
                            # Collapse multi-line queries to single line
                            truncated = truncated.replace("\n", " ")
                            f.write(
                                f"               <span style='color: gray;'>"
                                f"{html.escape(truncated)}</span>\n"
                            )

            f.write("\n")

        f.write("</pre></body></html>\n")

    print(f"HTML report written to {report_path}")


def main():
    results = []
    stop_watch = Utils.Stopwatch()
    ch = ClickHouseBinary()

    sqllogic_dir = os.path.join(Utils.cwd(), "tests", "sqllogic")
    sqllogictest_repo = os.path.join(temp_dir, "sqllogictest")
    out_dir = os.path.join(temp_dir, "sqllogic_output")
    os.makedirs(out_dir, exist_ok=True)

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

    # Step 2: Clone sqllogictest repo
    if results[-1].is_ok():
        print("Clone sqllogictest repo")

        # Pin to a specific commit to avoid non-deterministic test results
        # when the upstream repo updates (e.g., SQLite version bumps).
        sqllogictest_commit = "634e46492bff5c206a6eb9ba934f2cd411c74bf0"

        def clone():
            if not Path(sqllogictest_repo).is_dir():
                if not Shell.check(
                    f"git clone https://github.com/gregrahn/sqllogictest.git {sqllogictest_repo}",
                    verbose=True,
                ):
                    return False
            else:
                # On reused workspaces, fetch latest refs so the pinned
                # commit is available even if it was not in the original clone.
                if not Shell.check(
                    f"git -C {sqllogictest_repo} fetch origin",
                    verbose=True,
                ):
                    return False
            return Shell.check(
                f"git -C {sqllogictest_repo} checkout {sqllogictest_commit}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Clone sqllogictest repo",
                command=clone,
            )
        )

    # Step 3: Run self-test
    if results[-1].is_ok():
        print("Run self-test")
        self_test_dir = os.path.join(sqllogic_dir, "self-test")
        self_test_out = os.path.join(out_dir, "self-test")
        os.makedirs(self_test_out, exist_ok=True)

        def self_test():
            return Shell.check(
                f"python3 {sqllogic_dir}/runner.py --log-file {out_dir}/self-test.log"
                f" self-test"
                f" --self-test-dir {self_test_dir}"
                f" --out-dir {self_test_out}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Self-test",
                command=self_test,
            )
        )

    # Step 4: Run statements-test
    if results[-1].is_ok():
        print("Run statements-test")
        statements_input = os.path.join(sqllogictest_repo, "test")
        statements_out = os.path.join(out_dir, "statements-test")
        os.makedirs(statements_out, exist_ok=True)

        def statements_test():
            return Shell.check(
                f"python3 {sqllogic_dir}/runner.py --log-file {out_dir}/statements-test.log"
                f" statements-test"
                f" --input-dir {statements_input}"
                f" --out-dir {statements_out}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Statements test",
                command=statements_test,
            )
        )

    # Step 5: Run complete-test
    if results[-1].is_ok():
        print("Run complete-test")
        complete_input = os.path.join(sqllogictest_repo, "test")
        complete_out = os.path.join(out_dir, "complete-test")
        os.makedirs(complete_out, exist_ok=True)

        def complete_test():
            return Shell.check(
                f"python3 {sqllogic_dir}/runner.py --log-file {out_dir}/complete-test.log"
                f" complete-test"
                f" --input-dir {complete_input}"
                f" --out-dir {complete_out}",
                verbose=True,
            )

        results.append(
            Result.from_commands_run(
                name="Complete test",
                command=complete_test,
            )
        )

    # Step 6: Generate report and check thresholds
    report_html_path = os.path.join(out_dir, "report.html")
    threshold_info = ""

    if results[-1].is_ok():
        print("Generate report and check thresholds")

        def report_and_thresholds():
            nonlocal threshold_info

            all_reports = {}
            for mode_name in ("statements-test", "complete-test"):
                stage_reports = load_stage_reports(out_dir, mode_name)
                if stage_reports:
                    all_reports[mode_name] = stage_reports

            complete_reports = all_reports.get("complete-test", {})
            threshold_ok, threshold_info = check_thresholds(complete_reports)

            generate_html_report(
                all_reports, report_html_path, threshold_ok, threshold_info
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
