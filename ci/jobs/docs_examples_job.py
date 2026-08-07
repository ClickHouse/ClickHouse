#!/usr/bin/env python3
"""Run the SQL examples embedded in the documentation against a server started for the purpose.

The runner itself is `tests/docs_examples/runner.py`, which is plain Python and can be pointed at
any server. This job only provides that server: the shipped configuration plus the fragments in
`programs/server/config.d` (macros, the legacy geobase, the natural language processing data,
Keeper, the test clusters) and `programs/server/users.d` (the localhost-only network of the default
user, access management, query logging), and the fragments in `tests/docs_examples/config.d` and
`tests/docs_examples/users.d`, so that the features the examples demonstrate are actually
configured, and so that they are configured the way the shipped server configures them."""

import os
import subprocess

from praktika.result import Result
from praktika.utils import Shell, Utils

from ci.jobs.scripts.server_cleanup import kill_leftover_server_processes

TEMP_DIR = f"{Utils.cwd()}/ci/tmp"
# The server runs with this directory as its working directory: the paths in `config.d/path.xml`
# and the paths of the geobase and the lemmatizer data are relative to it.
SERVER_DIR = f"{TEMP_DIR}/docs_examples_server"
REPORT = f"{TEMP_DIR}/docs_examples_report.json"
REPORT_HTML = f"{TEMP_DIR}/docs_examples_report.html"
RUNNER_LOG = f"{TEMP_DIR}/docs_examples_runner.log"


class Server:
    def __init__(self):
        self.log_file = f"{SERVER_DIR}/server.log"
        self.process = None

    def install(self):
        commands = [
            f"rm -rf {SERVER_DIR} && mkdir -p {SERVER_DIR}",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {SERVER_DIR}/",
            f"cp -r --dereference ./programs/server/config.d ./programs/server/users.d {SERVER_DIR}/",
            f"cp ./tests/docs_examples/config.d/*.xml {SERVER_DIR}/config.d/",
            f"cp ./tests/docs_examples/users.d/*.xml {SERVER_DIR}/users.d/",
            f"cp -r ./tests/config/top_level_domains {SERVER_DIR}/",
            f"chmod +x {TEMP_DIR}/clickhouse",
            f"ln -sf {TEMP_DIR}/clickhouse {TEMP_DIR}/clickhouse-server",
            f"ln -sf {TEMP_DIR}/clickhouse {TEMP_DIR}/clickhouse-client",
        ]
        Utils.add_to_PATH(TEMP_DIR)
        return all(Shell.check(command, verbose=True) for command in commands)

    def start(self):
        kill_leftover_server_processes()
        print("Starting ClickHouse server")
        self.process = subprocess.Popen(
            f"{TEMP_DIR}/clickhouse-server --config-file=./config.xml",
            cwd=SERVER_DIR,
            shell=True,
            stdout=open(self.log_file, "w"),
            stderr=subprocess.STDOUT,
        )
        for _ in range(60):
            if self.process.poll() is not None:
                Utils.print_formatted_error("The server exited during startup", self.tail_log(), "")
                return False
            if Shell.check('clickhouse-client --query "SELECT 1"'):
                print("ClickHouse server ready")
                return True
            Utils.sleep(2)
        Utils.print_formatted_error("The server did not become ready", self.tail_log(), "")
        return False

    def tail_log(self, lines=50):
        return Shell.get_output(f"tail -n {lines} {self.log_file}")


def write_html_report(outcomes, stale, info):
    """Render the examples that fail the run, so that a failure names the page and the source file.

    The verdict of every example comes from the runner itself (the `verdict` field of the report),
    so this rendering cannot disagree with the exit code: `unexpected` and `fixed` examples and
    `stale` baseline entries fail the run, everything else passes."""
    import html

    def block(text, prefix):
        return "\n".join(prefix + html.escape(line) for line in text.splitlines())

    unexpected = [o for o in outcomes if o["verdict"] == "unexpected"]
    fixed = [o for o in outcomes if o["verdict"] == "fixed"]

    with open(REPORT_HTML, "w", encoding="utf-8") as f:
        f.write("<html><body><pre style='font-size: 12pt; padding: 1em; line-height: 1.25;'>\n")
        f.write(f"<b>{html.escape(info)}</b>\n\n")

        if unexpected:
            f.write(
                f"<b style='color: red;'>{len(unexpected)} example(s) unexpectedly not ok.</b>\n"
                "Fix the example in the source file below, or, if it cannot pass, add it to"
                " tests/docs_examples/known_failures.txt with the reason.\n\n"
            )
        for outcome in unexpected:
            f.write(
                f"<b style='color: red;'>{html.escape(outcome['id'])}</b>"
                f"  [{outcome['status']}]  {html.escape(outcome['source'])}\n"
            )
            f.write(block(outcome["query"], "    | ") + "\n")
            if outcome["status"] == "output":
                f.write("    documented response:\n")
                f.write(f"<span style='color: gray;'>{block(outcome['documented'], '    - ')}</span>\n")
                f.write("    actual response:\n")
                f.write(f"<span style='color: orange;'>{block(outcome['detail'], '    + ')}</span>\n")
            else:
                f.write(f"<span style='color: orange;'>{block(outcome['detail'], '    ! ')}</span>\n")
            f.write("\n")

        if fixed:
            f.write(
                f"\n<b style='color: green;'>{len(fixed)} example(s) now pass and must be removed"
                " from tests/docs_examples/known_failures.txt:</b>\n"
            )
            for outcome in fixed:
                f.write(f"  {html.escape(outcome['id'])}\n")

        if stale:
            f.write(
                f"\n<b style='color: red;'>{len(stale)} known failure(s) no longer exist and must be"
                " removed from tests/docs_examples/known_failures.txt:</b>\n"
            )
            for example_id in stale:
                f.write(f"  {html.escape(example_id)}\n")

        f.write("</pre></body></html>\n")


def main():
    stop_watch = Utils.Stopwatch()
    results = []
    server = Server()
    info = ""

    results.append(Result.from_commands_run(name="Start ClickHouse", command=lambda: server.install() and server.start()))

    if results[-1].is_ok():
        def run():
            # Not piped into `tee`: the shell praktika runs commands with has neither `PIPESTATUS`
            # nor `pipefail`, so the exit code of the runner would be lost.
            # `--global-objects`: the server above is started for this job alone, so the examples
            # that create users, roles or databases can run without disturbing anything.
            ok = Shell.check(
                f"python3 ./tests/docs_examples/runner.py --global-objects --report {REPORT} > {RUNNER_LOG} 2>&1",
                verbose=True,
            )
            print(Shell.get_output(f"cat {RUNNER_LOG}"))
            return ok

        results.append(Result.from_commands_run(name="Documentation examples", command=run))

    if os.path.isfile(REPORT):
        import json

        with open(REPORT, encoding="utf-8") as f:
            report = json.load(f)
        outcomes = report["examples"]
        stale = report["stale"]
        counts = {}
        for outcome in outcomes:
            counts[outcome["status"]] = counts.get(outcome["status"], 0) + 1
        if stale:
            counts["stale"] = len(stale)
        info = ", ".join(f"{status}: {count}" for status, count in sorted(counts.items()))
        write_html_report(outcomes, stale, info)

    files = [path for path in (REPORT_HTML, REPORT, RUNNER_LOG, server.log_file) if os.path.isfile(path)]
    Result.create_from(results=results, stopwatch=stop_watch, files=files, info=info).complete_job()


if __name__ == "__main__":
    main()
