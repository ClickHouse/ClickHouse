#!/usr/bin/env python3

import html
import json
import os
import re
from pathlib import Path

from praktika.result import Result
from praktika.utils import Shell, Utils

from ci.jobs.scripts.server_cleanup import kill_leftover_server_processes

temp_dir = f"{Utils.cwd()}/ci/tmp/"

EXPECTED_TOTAL_TESTS = 5_939_581
SQLLOGICTEST_COMMIT = "634e46492bff5c206a6eb9ba934f2cd411c74bf0"

MAX_EXAMPLES_PER_CATEGORY = 5
MAX_NEW_FAILURES_DISPLAY = 200
FAILURE_ID_RE = re.compile(r"^[^:\s]+:\d+$")


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
        kill_leftover_server_processes()
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
                f'clickhouse-client --port {self.port} --receive_timeout=5 --query "select 1"', verbose=True
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

    def died_result(self):
        retcode = self.proc.poll()
        if retcode is None:
            return None
        with open(self.log_file, "rb") as f:
            f.seek(max(0, os.path.getsize(self.log_file) - 64 * 1024 * 1024))
            lines = f.read().decode(errors="replace").splitlines()
        fatal = [line for line in lines if "<Fatal>" in line]
        return Result(
            name="Server died",
            status=Result.Status.FAIL,
            info="\n".join([f"Server exited with code {retcode}", *(fatal or lines)[-200:]]),
        )


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


def check_corpus_size(complete_test_reports):
    """Check that the complete-clickhouse stage ran the pinned corpus.
    Returns (passed, info_string)."""
    report = complete_test_reports.get("complete-clickhouse")
    if report is None:
        return False, "FAILED: complete-clickhouse report not found"

    stats = report.get("stats", {})
    total_stats = stats.get("total", {})
    total_success = total_stats.get("success", 0)
    total_tests = total_success + total_stats.get("fail", 0)

    lines = []
    passed = True

    if total_tests != EXPECTED_TOTAL_TESTS:
        lines.append(
            f"FAILED: total tests {total_tests:,} != expected "
            f"{EXPECTED_TOTAL_TESTS:,}"
        )
        passed = False
    else:
        lines.append(f"OK: total tests {total_tests:,} == expected {EXPECTED_TOTAL_TESTS:,}")

    info = "; ".join(lines)
    return passed, info


def load_exclusions(exclusions_dir, catalog_path):
    """Load and validate the allowed-failure manifests.

    The catalog describes every list file. Each non-comment line in a list is a
    stable SQLLogicTest request ID in the form ``test_name:position``.
    """
    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    if catalog.get("schema_version") != 1:
        raise ValueError("exclusion catalog must use schema_version 1")
    corpus = catalog.get("corpus", {})
    if corpus.get("commit") != SQLLOGICTEST_COMMIT:
        raise ValueError(
            "exclusion catalog commit does not match SQLLOGICTEST_COMMIT"
        )

    groups = catalog.get("groups")
    if not isinstance(groups, list) or not groups:
        raise ValueError("exclusion catalog must contain a non-empty groups list")

    exclusions = {}
    files = set()
    keys = set()
    for group in groups:
        key = group.get("key")
        file_name = group.get("file")
        description = group.get("description")
        if not key or not file_name or not description:
            raise ValueError(
                "each exclusion group needs key, file, and description"
            )
        if key in keys:
            raise ValueError(f"duplicate exclusion group key: {key}")
        if file_name in files:
            raise ValueError(f"duplicate exclusion group file: {file_name}")
        if os.path.basename(file_name) != file_name or not file_name.endswith(".txt"):
            raise ValueError(f"invalid exclusion file name: {file_name}")
        keys.add(key)
        files.add(file_name)

        path = os.path.join(exclusions_dir, file_name)
        if not os.path.isfile(path):
            raise ValueError(f"exclusion file does not exist: {path}")

        group_ids = set()
        with open(path, "r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, 1):
                failure_id = line.strip()
                if not failure_id or failure_id.startswith("#"):
                    continue
                if not FAILURE_ID_RE.fullmatch(failure_id):
                    raise ValueError(
                        f"invalid failure ID in {file_name}:{line_number}: {failure_id}"
                    )
                if failure_id in group_ids:
                    raise ValueError(
                        f"duplicate failure ID in {file_name}: {failure_id}"
                    )
                if failure_id in exclusions:
                    other_group = exclusions[failure_id]
                    raise ValueError(
                        f"failure ID appears in both {other_group} and {key}: {failure_id}"
                    )
                group_ids.add(failure_id)
                exclusions[failure_id] = key
        group["count"] = len(group_ids)

    unmanaged_files = sorted(
        path.name
        for path in Path(exclusions_dir).glob("*.txt")
        if path.name not in files
    )
    if unmanaged_files:
        raise ValueError(
            "exclusion list files missing from catalog: " + ", ".join(unmanaged_files)
        )

    return exclusions, groups


def load_compatibility_status(status_path):
    with open(status_path, "r", encoding="utf-8") as f:
        status = json.load(f)
    if status.get("schema_version") != 1:
        raise ValueError("compatibility status must use schema_version 1")
    if not isinstance(status.get("findings"), list):
        raise ValueError("compatibility status must contain a findings list")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", status.get("verified_at", "")):
        raise ValueError("compatibility status needs an ISO verified_at date")
    if not isinstance(status.get("count_scope"), str):
        raise ValueError("compatibility status needs a count_scope string")
    for index, finding in enumerate(status["findings"]):
        if not finding.get("title"):
            raise ValueError(f"compatibility finding {index} needs a title")
        for field, allowed_states in (
            ("issue", {"open", "closed"}),
            ("pull_request", {"open", "closed", "merged"}),
        ):
            reference = finding.get(field)
            if reference is None:
                continue
            if reference.get("state") not in allowed_states:
                raise ValueError(
                    f"compatibility finding {index} has invalid {field} state"
                )
            if not reference.get("url", "").startswith(
                "https://github.com/ClickHouse/ClickHouse/"
            ):
                raise ValueError(
                    f"compatibility finding {index} has invalid {field} URL"
                )
    return status


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
    This file can be passed to generate_known_failures.py to update the baseline."""
    with open(output_path, "w", encoding="utf-8") as f:
        for fid in sorted(failures_dict.keys()):
            f.write(fid + "\n")
    print(f"Saved {len(failures_dict)} current failures to {output_path}")


def check_compatibility_baseline(new_failures, fixed_tests):
    failures = []
    if new_failures:
        failures.append(f"FAILED: {len(new_failures):,} new failures detected")
    if fixed_tests:
        failures.append(f"FAILED: {len(fixed_tests):,} stale exclusions now pass")
    return not failures, "; ".join(failures)


def generate_html_report(
    all_reports,
    report_path,
    threshold_passed,
    threshold_info,
    new_failures=None,
    fixed_tests=None,
    known_failures_count=0,
    exclusion_groups=None,
    compatibility_status=None,
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

        if exclusion_groups:
            f.write("<b>Compatibility exclusions</b>\n")
            f.write(
                "  Every excluded request is still executed. New failures and "
                "exclusions that start passing fail this check.\n"
            )
            for group in exclusion_groups:
                count = group.get("count", 0)
                current_count = group.get("current_count", 0)
                fixed_count = count - current_count
                f.write(
                    f"  {html.escape(group['key'])}: {current_count:,} failing / "
                    f"{count:,} excluded"
                )
                if fixed_count:
                    f.write(
                        f" <b style='color: green;'>({fixed_count:,} now pass){reset_color}"
                    )
                f.write("\n")
                f.write(f"    {html.escape(group['description'])}\n")
            f.write("\n")

        if compatibility_status:
            verified_at = compatibility_status.get("verified_at", "unknown")
            f.write(
                f"<b>Tracked ClickHouse compatibility work</b> "
                f"(status verified {html.escape(verified_at)})\n"
            )
            count_scope = compatibility_status.get("count_scope")
            if count_scope:
                f.write(f"  {html.escape(count_scope)}\n")
            for finding in compatibility_status.get("findings", []):
                title = html.escape(finding["title"])
                observed = finding.get("observed_cases")
                observed_text = ""
                if observed is not None:
                    observed_text = f"; {observed:,} observed cases"
                f.write(f"  {title}{observed_text}\n")
                issue = finding.get("issue")
                if issue:
                    issue_url = html.escape(issue["url"], quote=True)
                    issue_state = html.escape(issue["state"])
                    f.write(
                        f"    Issue: <a href='{issue_url}'>{issue_url}</a> "
                        f"[{issue_state}]\n"
                    )
                pull_request = finding.get("pull_request")
                if pull_request:
                    pr_url = html.escape(pull_request["url"], quote=True)
                    pr_state = html.escape(pull_request["state"])
                    f.write(
                        f"    PR:    <a href='{pr_url}'>{pr_url}</a> "
                        f"[{pr_state}]\n"
                    )
                note = finding.get("note")
                if note:
                    f.write(f"    {html.escape(note)}\n")
            f.write("\n")

        # New failures section (shown first, as it's the most actionable)
        if new_failures is not None:
            num_new = len(new_failures)
            if num_new > 0:
                f.write(
                    f"<b style='color: red;'>NEW FAILURES: {num_new:,} "
                    f"(not in the {known_failures_count:,}-entry exclusion baseline)"
                    f"{reset_color}\n"
                )
                f.write(
                    "  These tests were not failing before. "
                    "Each entry shows the test file, position, query, and failure reason.\n"
                )
                # Collect unique test files and show per-file reproduction commands
                new_test_files = sorted(
                    set(d["test_name"] for d in new_failures.values())
                )
                f.write(
                    "\n  To reproduce via sqllogictest (ClickHouse must be running):\n"
                    "  <span style='color: blue;'>"
                    "git clone https://github.com/gregrahn/sqllogictest"
                    " /tmp/sqllogictest</span>\n\n"
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
        if fixed_tests:
            num_fixed = len(fixed_tests)
            if num_fixed > 0:
                f.write(
                    f"<b style='color: green;'>FIXED TESTS: {num_fixed:,} "
                    f"(excluded requests that now pass){reset_color}\n"
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
                    f.write("    Failure categories:\n")
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

    exclusions_dir = os.path.join(sqllogic_dir, "exclusions")
    exclusion_catalog_path = os.path.join(exclusions_dir, "catalog.json")
    compatibility_status_path = os.path.join(
        sqllogic_dir, "compatibility_status.json"
    )
    current_failures_path = os.path.join(out_dir, "current_failures.txt")
    fixed_exclusions_path = os.path.join(out_dir, "fixed_exclusions.txt")

    exclusions = {}
    exclusion_groups = []
    compatibility_status = {}
    server_started = False

    def validate_manifests():
        nonlocal exclusions, exclusion_groups, compatibility_status
        exclusions, exclusion_groups = load_exclusions(
            exclusions_dir, exclusion_catalog_path
        )
        compatibility_status = load_compatibility_status(
            compatibility_status_path
        )
        print(
            f"Loaded {len(exclusions):,} exclusions from "
            f"{len(exclusion_groups)} groups"
        )
        return True

    results.append(
        Result.from_commands_run(
            name="Validate compatibility manifests",
            command=validate_manifests,
        )
    )

    # Step 1: Start ClickHouse
    if results[-1].is_ok():
        print("Start ClickHouse")

        def start():
            return ch.install() and ch.start()

        results.append(
            Result.from_commands_run(
                name="Start ClickHouse",
                command=start,
            )
        )
        server_started = results[-1].is_ok()

    # Step 2: Clone sqllogictest repo
    if results[-1].is_ok():
        print("Clone sqllogictest repo")

        # Pin to a specific commit to avoid non-deterministic test results
        # when the upstream repo updates (e.g., SQLite version bumps).
        def clone():
            if not Path(sqllogictest_repo).is_dir():
                if not Shell.check(
                    f"git clone https://github.com/gregrahn/sqllogictest {sqllogictest_repo}",
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
                f"git -C {sqllogictest_repo} checkout {SQLLOGICTEST_COMMIT}",
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
            threshold_ok, threshold_info = check_corpus_size(complete_reports)

            # Compatibility exclusions analysis on the complete-clickhouse stage
            new_failures = {}
            fixed_tests = set()
            known_count = len(exclusions)

            ch_report = complete_reports.get("complete-clickhouse")
            if ch_report is not None:
                current = extract_failures_with_details(ch_report)

                # Save current failures list as artifact for baseline updates
                save_current_failures(current, current_failures_path)

                current_ids = set(current.keys())
                excluded_ids = set(exclusions)
                new_ids = current_ids - excluded_ids
                fixed_tests = excluded_ids - current_ids

                new_failures = {fid: current[fid] for fid in new_ids}

                group_current_counts = {
                    group["key"]: 0 for group in exclusion_groups
                }
                for failure_id in current_ids & excluded_ids:
                    group_current_counts[exclusions[failure_id]] += 1
                for group in exclusion_groups:
                    group["current_count"] = group_current_counts[group["key"]]

                with open(fixed_exclusions_path, "w", encoding="utf-8") as f:
                    for failure_id in sorted(fixed_tests):
                        f.write(failure_id + "\n")

                print(
                    f"Excluded failures: {known_count:,}, "
                    f"current failures: {len(current):,}, "
                    f"new: {len(new_failures):,}, "
                    f"fixed exclusions: {len(fixed_tests):,}"
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
                        " https://github.com/gregrahn/sqllogictest"
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
                            "  python3 tests/sqllogic/runner.py"
                            " complete-test"
                            " --input-dir /tmp/sqllogic_input"
                            " --out-dir /tmp/sqllogic_output"
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
            baseline_ok, baseline_info = check_compatibility_baseline(
                new_failures, fixed_tests
            )
            final_ok = threshold_ok and baseline_ok
            if baseline_info:
                threshold_info += f"; {baseline_info}"

            generate_html_report(
                all_reports,
                report_html_path,
                final_ok,
                threshold_info,
                new_failures=new_failures,
                fixed_tests=fixed_tests,
                known_failures_count=known_count,
                exclusion_groups=exclusion_groups,
                compatibility_status=compatibility_status,
            )

            return final_ok

        results.append(
            Result.from_commands_run(
                name="Report & Thresholds",
                command=report_and_thresholds,
            )
        )

    # A server that died mid-run otherwise shows only as a short test count.
    if server_started:
        died = ch.died_result()
        if died:
            results.append(died)

    report_files = []
    if os.path.isfile(report_html_path):
        report_files.append(report_html_path)
    if os.path.isfile(current_failures_path):
        report_files.append(current_failures_path)
    if os.path.isfile(fixed_exclusions_path):
        report_files.append(fixed_exclusions_path)

    Result.create_from(
        results=results,
        stopwatch=stop_watch,
        files=report_files,
        info=threshold_info,
    ).complete_job()


if __name__ == "__main__":
    main()
