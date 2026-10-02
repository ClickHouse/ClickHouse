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

MAX_EXAMPLES_PER_CATEGORY = 5
MAX_NEW_FAILURES_DISPLAY = 200


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


def load_known_failures(known_failures_path):
    """Load the set of known failure IDs from a text file.
    Each line is 'test_name:position'. Lines starting with # and empty lines are skipped."""
    known = set()
    if not os.path.isfile(known_failures_path):
        return known
    with open(known_failures_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                known.add(line)
    return known


def extract_failures_with_details(report):
    """Extract all failures from a report as a dict mapping failure_id to details.
    Returns {failure_id: {"test_name": ..., "position": ..., "request": ...,
                          "request_type": ..., "reason": ...}}."""
    failures = {}
    for test_name, test_data in report.get("tests", {}).items():
        for position, req in test_data.get("requests", {}).items():
            if req.get("status") == "error":
                failure_id = f"{test_name}:{position}"
                failures[failure_id] = {
                    "test_name": test_name,
                    "position": position,
                    "request": req.get("request", ""),
                    "request_type": req.get("request_type", ""),
                    "reason": req.get("reason", ""),
                }
    return failures


def save_current_failures(failures_dict, output_path):
    """Save current failure IDs to a text file (sorted, one per line).
    This file can be copied to tests/sqllogic/known_failures.txt to update the baseline."""
    with open(output_path, "w", encoding="utf-8") as f:
        for fid in sorted(failures_dict.keys()):
            f.write(fid + "\n")
    print(f"Saved {len(failures_dict)} current failures to {output_path}")


def generate_html_report(
    all_reports,
    report_path,
    threshold_passed,
    threshold_info,
    new_failures=None,
    fixed_tests=None,
    known_failures_count=0,
):
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

        # New failures section (shown first, as it's the most actionable)
        if new_failures is not None and known_failures_count > 0:
            num_new = len(new_failures)
            if num_new > 0:
                f.write(
                    f"<b style='color: red;'>NEW FAILURES: {num_new:,} "
                    f"(not in known_failures.txt with {known_failures_count:,} entries)"
                    f"{reset_color}\n"
                )
                f.write(
                    f"  These tests were not failing before. "
                    f"Each entry shows the test file, position, query, and failure reason.\n"
                )
                # Collect unique test files and show per-file reproduction commands
                new_test_files = sorted(
                    set(d["test_name"] for d in new_failures.values())
                )
                f.write(
                    f"\n  To reproduce via sqllogictest (ClickHouse must be running):\n"
                    f"  <span style='color: blue;'>"
                    f"git clone https://github.com/gregrahn/sqllogictest.git"
                    f" /tmp/sqllogictest</span>\n\n"
                )
                for tf in new_test_files:
                    subdir = os.path.dirname(tf)
                    escaped_tf = html.escape(tf)
                    escaped_subdir = html.escape(subdir)
                    f.write(
                        f"  <span style='color: blue;'>"
                        f"mkdir -p /tmp/sqllogic_input/{escaped_subdir}"
                        f" &amp;&amp; cp /tmp/sqllogictest/test/{escaped_tf}"
                        f" /tmp/sqllogic_input/{escaped_subdir}/\n"
                        f"  python3 tests/sqllogic/runner.py complete-test"
                        f" --input-dir /tmp/sqllogic_input"
                        f" --out-dir /tmp/sqllogic_output</span>\n"
                    )
                f.write("\n")

                displayed = 0
                for fid in sorted(new_failures.keys()):
                    if displayed >= MAX_NEW_FAILURES_DISPLAY:
                        remaining = num_new - displayed
                        f.write(
                            f"  ... and {remaining:,} more new failures "
                            f"(see current_failures.txt artifact for the full list)\n"
                        )
                        break
                    details = new_failures[fid]
                    test_name = details["test_name"]
                    position = details["position"]
                    request = details["request"]
                    reason = details["reason"]

                    f.write(
                        f"  <b style='color: red;'>{html.escape(test_name)}:{html.escape(position)}</b>\n"
                    )

                    # Show the query (truncated to one line)
                    if request:
                        truncated = request[:300].replace("\n", " ")
                        if len(request) > 300:
                            truncated += "..."
                        f.write(
                            f"    Query:  <span style='color: gray;'>{html.escape(truncated)}</span>\n"
                        )

                    if reason:
                        truncated_reason = reason[:300].replace("\n", " ")
                        if len(reason) > 300:
                            truncated_reason += "..."
                        f.write(
                            f"    Reason: <span style='color: orange;'>{html.escape(truncated_reason)}</span>\n"
                        )

                    f.write("\n")
                    displayed += 1
            else:
                f.write(
                    f"<b style='color: green;'>No new failures{reset_color} "
                    f"(compared to {known_failures_count:,} known failures)\n"
                )

            f.write("\n")

        # Fixed tests section
        if fixed_tests and known_failures_count > 0:
            num_fixed = len(fixed_tests)
            if num_fixed > 0:
                f.write(
                    f"<b style='color: green;'>FIXED TESTS: {num_fixed:,} "
                    f"(were in known_failures.txt but now pass){reset_color}\n"
                )
                max_show = 50
                displayed = 0
                for fid in sorted(fixed_tests):
                    if displayed >= max_show:
                        remaining = num_fixed - displayed
                        f.write(f"  ... and {remaining:,} more\n")
                        break
                    f.write(f"  {html.escape(fid)}\n")
                    displayed += 1
                f.write("\n")

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

    known_failures_path = os.path.join(sqllogic_dir, "known_failures.txt")
    current_failures_path = os.path.join(out_dir, "current_failures.txt")

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

    # Step 6: Generate report, check thresholds, and detect new failures
    report_html_path = os.path.join(out_dir, "report.html")
    threshold_info = ""

    if results[-1].is_ok():
        print("Generate report, check thresholds, and detect new failures")

        def report_and_thresholds():
            nonlocal threshold_info

            all_reports = {}
            for mode_name in ("statements-test", "complete-test"):
                stage_reports = load_stage_reports(out_dir, mode_name)
                if stage_reports:
                    all_reports[mode_name] = stage_reports

            complete_reports = all_reports.get("complete-test", {})
            threshold_ok, threshold_info = check_thresholds(complete_reports)

            # Known failures analysis on the complete-clickhouse stage
            new_failures = {}
            fixed_tests = set()
            known = load_known_failures(known_failures_path)
            known_count = len(known)

            ch_report = complete_reports.get("complete-clickhouse")
            if ch_report is not None:
                current = extract_failures_with_details(ch_report)

                # Save current failures list as artifact for baseline updates
                save_current_failures(current, current_failures_path)

                if known_count > 0:
                    current_ids = set(current.keys())
                    new_ids = current_ids - known
                    fixed_tests = known - current_ids

                    new_failures = {
                        fid: current[fid] for fid in new_ids
                    }

                    print(
                        f"Known failures: {known_count:,}, "
                        f"current failures: {len(current):,}, "
                        f"new: {len(new_failures):,}, "
                        f"fixed: {len(fixed_tests):,}"
                    )

                    if new_failures:
                        new_test_files = sorted(
                            set(d["test_name"] for d in new_failures.values())
                        )
                        print("\nNEW FAILURES (with reproduction commands):")
                        print(
                            "\nTo reproduce via sqllogictest"
                            " (ClickHouse must be running):"
                        )
                        print(
                            "  git clone"
                            " https://github.com/gregrahn/sqllogictest.git"
                            " /tmp/sqllogictest"
                        )
                        for tf in new_test_files:
                            subdir = os.path.dirname(tf)
                            print(
                                f"\n  mkdir -p"
                                f" /tmp/sqllogic_input/{subdir}"
                                f" && cp"
                                f" /tmp/sqllogictest/test/{tf}"
                                f" /tmp/sqllogic_input/{subdir}/"
                            )
                            print(
                                f"  python3 tests/sqllogic/runner.py"
                                f" complete-test"
                                f" --input-dir /tmp/sqllogic_input"
                                f" --out-dir /tmp/sqllogic_output"
                            )
                        displayed = 0
                        for fid in sorted(new_failures.keys()):
                            if displayed >= MAX_NEW_FAILURES_DISPLAY:
                                remaining = len(new_failures) - displayed
                                print(
                                    f"\n  ... and {remaining:,} more new failures"
                                    f" (see current_failures.txt artifact for the full list)"
                                )
                                break
                            details = new_failures[fid]
                            reason = details.get("reason", "")
                            print(f"\n  {fid}")
                            if reason:
                                print(f"    Reason: {reason[:300]}")
                            displayed += 1

            # Finalize pass/fail verdict before rendering HTML report
            final_ok = threshold_ok
            if known_count > 0 and len(new_failures) > 0:
                threshold_info += (
                    f"; FAILED: {len(new_failures):,} new failures detected"
                )
                final_ok = False

            generate_html_report(
                all_reports,
                report_html_path,
                final_ok,
                threshold_info,
                new_failures=new_failures,
                fixed_tests=fixed_tests,
                known_failures_count=known_count,
            )

            return final_ok

        results.append(
            Result.from_commands_run(
                name="Report & Thresholds",
                command=report_and_thresholds,
            )
        )

    report_files = []
    if os.path.isfile(report_html_path):
        report_files.append(report_html_path)
    if os.path.isfile(current_failures_path):
        report_files.append(current_failures_path)

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=report_files,
        info=threshold_info,
    ).complete_job()


if __name__ == "__main__":
    main()
