#!/usr/bin/env python3
"""Run the SQL examples embedded in the documentation against a server started for the purpose.

The runner itself is `tests/docs_examples/runner.py`, which is plain Python and can be pointed at
any server. This job only provides that server: the shipped configuration plus the fragments in
`programs/server/config.d` (macros, the legacy geobase, the natural language processing data,
Keeper, the test clusters) and `programs/server/users.d` (the localhost-only network of the default
user, access management, query logging), and the fragments in `tests/docs_examples/config.d` and
`tests/docs_examples/users.d`, so that the features the examples demonstrate are actually
configured, and so that they are configured the way the shipped server configures them."""

import glob
import os
import shutil
import subprocess

from praktika.result import Result
from praktika.utils import Utils

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
        """Assemble the server directory with Python filesystem calls.

        Nothing here goes through a shell: the paths are interpolated from the checkout location,
        and a destructive command such as the removal of the previous server directory must not
        depend on how a shell splits or globs them."""
        shutil.rmtree(SERVER_DIR, ignore_errors=True)
        os.makedirs(f"{SERVER_DIR}/config.d")
        os.makedirs(f"{SERVER_DIR}/users.d")

        shutil.copy("./programs/server/config.xml", SERVER_DIR)
        shutil.copy("./programs/server/users.xml", SERVER_DIR)
        # Most of the fragments are symlinks into `tests/config`, and the server directory is
        # assembled outside the checkout, so they are copied by content, not as symlinks.
        for directory in ("config.d", "users.d"):
            shutil.copytree(f"./programs/server/{directory}", f"{SERVER_DIR}/{directory}", symlinks=False, dirs_exist_ok=True)
            for fragment in sorted(glob.glob(f"./tests/docs_examples/{directory}/*.xml")):
                shutil.copy(fragment, f"{SERVER_DIR}/{directory}/")
        shutil.copytree("./tests/config/top_level_domains", f"{SERVER_DIR}/top_level_domains", symlinks=False)

        binary = f"{TEMP_DIR}/clickhouse"
        os.chmod(binary, os.stat(binary).st_mode | 0o111)
        for name in ("clickhouse-server", "clickhouse-client"):
            link = f"{TEMP_DIR}/{name}"
            if os.path.islink(link) or os.path.exists(link):
                os.remove(link)
            os.symlink(binary, link)

        # The downloaded binary is self-extracting: the first invocation decompresses the real ELF
        # in place. Trigger that synchronously here, because `start` runs the server and probes it
        # with `clickhouse-client` - the same file - and two decompressors racing each other corrupt
        # the binary they both rewrite (the server then dies with `open: Is a directory`).
        subprocess.run([binary, "--version"], stdout=subprocess.DEVNULL, check=True)

        Utils.add_to_PATH(TEMP_DIR)
        return True

    def start(self):
        kill_leftover_server_processes()
        print("Starting ClickHouse server")
        server_env = {name: value for name, value in os.environ.items() if name not in {"OPENAI_API_KEY", "ANTHROPIC_API_KEY"}}
        self.process = subprocess.Popen(
            [f"{TEMP_DIR}/clickhouse-server", "--config-file=./config.xml"],
            cwd=SERVER_DIR,
            stdout=open(self.log_file, "w"),
            stderr=subprocess.STDOUT,
            env=server_env,
        )
        for _ in range(60):
            if self.process.poll() is not None:
                Utils.print_formatted_error("The server exited during startup", self.tail_log(), "")
                return False
            readiness = subprocess.run(
                [f"{TEMP_DIR}/clickhouse-client", "--host", "127.0.0.1", "--port", "9000", "--query", "SELECT 1"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if readiness.returncode == 0:
                print("ClickHouse server ready")
                return True
            Utils.sleep(2)
        Utils.print_formatted_error("The server did not become ready", self.tail_log(), "")
        return False

    def tail_log(self, lines=50):
        with open(self.log_file, encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-lines:])


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
            f.write(f"<b style='color: red;'>{html.escape(outcome['id'])}</b>  [{outcome['status']}]  {html.escape(outcome['source'])}\n")
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
            f.write(f"\n<b style='color: green;'>{len(fixed)} example(s) now pass and must be removed from tests/docs_examples/known_failures.txt:</b>\n")
            for outcome in fixed:
                f.write(f"  {html.escape(outcome['id'])}\n")

        if stale:
            f.write(f"\n<b style='color: red;'>{len(stale)} known failure(s) no longer exist and must be removed from tests/docs_examples/known_failures.txt:</b>\n")
            for example_id in stale:
                f.write(f"  {html.escape(example_id)}\n")

        f.write("</pre></body></html>\n")


def report_info(outcomes, stale):
    """Summarize the runner's verdicts, which determine whether the job passed."""
    counts = {}
    for outcome in outcomes:
        verdict = outcome["verdict"]
        counts[verdict] = counts.get(verdict, 0) + 1
    if stale:
        counts["stale"] = len(stale)
    return ", ".join(f"{verdict}: {count}" for verdict, count in sorted(counts.items()))


def main():
    stop_watch = Utils.Stopwatch()
    results = []
    server = Server()
    info = ""

    results.append(Result.from_commands_run(name="Start ClickHouse", command=lambda: server.install() and server.start()))

    if results[-1].is_ok():

        def run():
            # Run without a shell, so that the exit code of the runner is the exit code observed
            # here and the interpolated paths cannot be reinterpreted.
            # `--global-objects`: the server above is started for this job alone, so the examples
            # that create users, roles or databases can run without disturbing anything.
            # `--external-calls`: the server is configured with named collections that point at
            # a loopback port nothing listens on (see `tests/docs_examples/config.d`), so the
            # `ai*` examples resolve their credentials, reach the provider request, and fail
            # there instead of reaching any external service; running them keeps their
            # known-failures entries validated against that boundary.
            with open(RUNNER_LOG, "w", encoding="utf-8") as log:
                code = subprocess.run(
                    ["python3", "./tests/docs_examples/runner.py", "--global-objects", "--external-calls", "--report", REPORT],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    check=False,
                ).returncode
            with open(RUNNER_LOG, encoding="utf-8", errors="replace") as log:
                print(log.read())
            return code == 0

        results.append(Result.from_commands_run(name="Documentation examples", command=run))

    if os.path.isfile(REPORT):
        import json

        with open(REPORT, encoding="utf-8") as f:
            report = json.load(f)
        outcomes = report["examples"]
        stale = report["stale"]
        info = report_info(outcomes, stale)
        write_html_report(outcomes, stale, info)

    files = [path for path in (REPORT_HTML, REPORT, RUNNER_LOG, server.log_file) if os.path.isfile(path)]
    Result.create_from(results=results, stopwatch=stop_watch, files=files, info=info).complete_job()


if __name__ == "__main__":
    main()
