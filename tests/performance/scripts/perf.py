#!/usr/bin/env python3

import argparse
import itertools
import json
import logging
import os
import random
import re
import signal
import statistics
import string
import subprocess
import sys
import time
import traceback
import xml.etree.ElementTree as et
from concurrent.futures import ThreadPoolExecutor
from threading import Thread

import clickhouse_driver
from scipy import stats

logging.basicConfig(
    format="%(asctime)s: %(levelname)s: %(module)s: %(message)s", level="WARNING"
)

total_start_seconds = time.perf_counter()
stage_start_seconds = total_start_seconds


# Thread executor that does not hides exception that happens during function
# execution, and rethrows it after join()
class SafeThread(Thread):
    run_exception = None

    def run(self):
        try:
            super().run()
        except:
            self.run_exception = sys.exc_info()

    def join(self):
        super().join()
        if self.run_exception:
            raise self.run_exception[1]


def reportStageEnd(stage):
    global stage_start_seconds, total_start_seconds

    current = time.perf_counter()
    print(
        f"stage\t{stage}\t{current - stage_start_seconds:.3f}\t{current - total_start_seconds:.3f}"
    )
    stage_start_seconds = current


def tsv_escape(s):
    return (
        s.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "")
    )


parser = argparse.ArgumentParser(description="Run performance test.")
# Explicitly decode files as UTF-8 because sometimes we have Russian characters in queries, and LANG=C is set.
parser.add_argument(
    "file",
    metavar="FILE",
    type=argparse.FileType("r", encoding="utf-8"),
    nargs=1,
    help="test description file",
)
parser.add_argument(
    "--host",
    nargs="*",
    default=["localhost"],
    help="Space-separated list of server hostname(s). Corresponds to '--port' options.",
)
parser.add_argument(
    "--port",
    nargs="*",
    default=[9000],
    help="Space-separated list of server port(s). Corresponds to '--host' options.",
)
parser.add_argument(
    "--user",
    default="default",
    help="Username for ClickHouse authentication.",
)
parser.add_argument(
    "--password",
    default="",
    help="Password for ClickHouse authentication.",
)
parser.add_argument(
    "--secure",
    action="store_true",
    help="Use SSL/TLS connection.",
)
parser.add_argument(
    "--binary",
    nargs="*",
    default=["clickhouse"],
    help="Space-separated list of clickhouse binary path(s), parallel to '--host'/'--port'. "
    'Used by shell-script queries (<query type="shell">) to build $CLICKHOUSE_BINARY, '
    "$CLICKHOUSE_LOCAL and $CLICKHOUSE_CLIENT for the corresponding server. "
    "A shorter list (e.g. a single value) is reused for every server.",
)
parser.add_argument(
    "--http-port",
    nargs="*",
    default=[8123],
    help="Space-separated list of HTTP port(s), parallel to '--host'/'--port'. "
    'Used by shell-script queries (<query type="shell">) to build $CLICKHOUSE_URL '
    "for the corresponding server. A shorter list is reused for every server.",
)
parser.add_argument(
    "--runs", type=int, default=1, help="Number of query runs per server."
)
parser.add_argument(
    "--max-queries",
    type=int,
    default=None,
    help="Test no more than this number of queries, chosen at random.",
)
parser.add_argument(
    "--queries-to-run",
    nargs="*",
    type=int,
    default=None,
    help="Space-separated list of indexes of queries to test.",
)
parser.add_argument(
    "--max-query-seconds",
    type=int,
    default=15,
    help="For how many seconds at most a query is allowed to run. The script finishes with error if this time is exceeded.",
)
parser.add_argument(
    "--prewarm-max-query-seconds",
    type=int,
    default=180,
    help="For how many seconds at most a prewarm (cold storage) query is allowed to run. The script finishes with error if this time is exceeded.",
)
parser.add_argument(
    "--profile-seconds",
    type=int,
    default=0,
    help="For how many seconds to profile a query for which the performance has changed.",
)
parser.add_argument(
    "--long", action="store_true", help="Do not skip the tests tagged as long."
)
parser.add_argument(
    "--print-queries", action="store_true", help="Print test queries and exit."
)
parser.add_argument(
    "--print-settings", action="store_true", help="Print test settings and exit."
)
parser.add_argument(
    "--keep-created-tables",
    action="store_true",
    help="Don't drop the created tables after the test.",
)
parser.add_argument(
    "--use-existing-tables",
    action="store_true",
    help="Don't create or drop the tables, use the existing ones instead.",
)
parser.add_argument(
    "--jemalloc-purge",
    choices=["disabled", "after-fill", "before-each-query", "before-each-run"],
    default="before-each-run",
    help="When to issue SYSTEM JEMALLOC PURGE on all server connections. "
    "Purging equalises per-process jemalloc dirty-page state between LEFT and "
    "RIGHT, which otherwise diverges across the test and causes background "
    "purges to do asymmetric work during measured queries.",
)
args = parser.parse_args()

reportStageEnd("start")

test_name = os.path.splitext(os.path.basename(args.file[0].name))[0]
xml_dir = os.path.dirname(os.path.abspath(args.file[0].name))

tree = et.parse(args.file[0])
root = tree.getroot()

reportStageEnd("parse")

# Process query parameters
subst_elems = root.findall("substitutions/substitution")
available_parameters = {}  # { 'table': ['hits_10m', 'hits_100m'], ... }
for e in subst_elems:
    name = e.find("name").text
    values = [v.text for v in e.findall("values/value")]
    if not values:
        raise Exception(f"No values given for substitution {{{name}}}")

    available_parameters[name] = values


# Takes parallel lists of templates, substitutes them with all combos of
# parameters. The set of parameters is determined based on the first list.
# Note: keep the order of queries -- sometimes we have DROP IF EXISTS
# followed by CREATE in create queries section, so the order matters.
def substitute_parameters(query_templates, other_templates=[]):
    query_results = []
    other_results = [[]] * (len(other_templates))
    for i, q in enumerate(query_templates):
        # We need stable order of keys here, so that the order of substitutions
        # is always the same, and the query indexes are consistent across test
        # runs.
        keys = sorted(set(n for _, n, _, _ in string.Formatter().parse(q) if n))
        values = [available_parameters[k] for k in keys]
        combos = itertools.product(*values)
        for c in combos:
            with_keys = dict(zip(keys, c))
            query_results.append(q.format(**with_keys))
            for j, t in enumerate(other_templates):
                other_results[j].append(t[i].format(**with_keys))
    if len(other_templates):
        return query_results, other_results
    else:
        return query_results


def split_sql_statements(sql):
    """Split SQL into statements on semicolons, respecting quoted literals/identifiers, -- and # comments, and /* block comments */."""
    statements = []
    current = []
    quote_char = ""
    in_line_comment = False
    in_block_comment = False
    i = 0
    while i < len(sql):
        ch = sql[i]
        next_ch = sql[i + 1] if i + 1 < len(sql) else ""
        if in_line_comment:
            current.append(ch)
            if ch == "\n":
                in_line_comment = False
        elif in_block_comment:
            current.append(ch)
            if ch == "*" and next_ch == "/":
                current.append("/")
                i += 1
                in_block_comment = False
        elif quote_char:
            current.append(ch)
            if ch == quote_char and next_ch == quote_char:
                current.append(quote_char)
                i += 1
            elif ch == quote_char:
                quote_char = ""
        elif (ch == "-" and next_ch == "-") or (ch == "#"):
            in_line_comment = True
            current.append(ch)
        elif ch == "/" and next_ch == "*":
            in_block_comment = True
            current.append(ch)
        elif ch in ("'", '"', "`"):
            quote_char = ch
            current.append(ch)
        elif ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
        else:
            current.append(ch)
        i += 1
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)
    return statements


def first_keyword(sql):
    """Return the first SQL keyword from a statement, skipping --, #, and /* */ comments."""
    i = 0
    while i < len(sql):
        ch = sql[i]
        next_ch = sql[i + 1] if i + 1 < len(sql) else ""
        if ch in (" ", "\t", "\n", "\r"):
            i += 1
        elif (ch == "-" and next_ch == "-") or ch == "#":
            nl = sql.find("\n", i)
            i = nl + 1 if nl != -1 else len(sql)
        elif ch == "/" and next_ch == "*":
            end = sql.find("*/", i + 2)
            i = end + 2 if end != -1 else len(sql)
        else:
            # Found start of a token → read until whitespace or special chars
            j = i
            while j < len(sql) and sql[j] not in (" ", "\t", "\n", "\r", "(", ";"):
                j += 1
            return sql[i:j].upper()
    return ""


def execute_query_group(connection, q_list, query_id, settings):
    """
    Execute a group of SQL statements sequentially and return the total server-side elapsed time.
    Some benchmark queries (e.g. TPC-DS Q14) consist of multiple SELECT statements in one file.
    We run them together and sum the elapsed time so that they appear as a single entry in the performance report.
    """
    total = 0
    for q in q_list:
        connection.execute(q, query_id=query_id, settings=settings)
        total += connection.last_query.elapsed
    return total


def load_settings_file(xml_root, base_dir):
    """Load settings from a JSON file referenced by <settings file="..."/> attribute."""
    elem = xml_root.find("settings")
    if elem is None or "file" not in elem.attrib:
        return {}
    path = os.path.join(base_dir, elem.attrib["file"])
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["settings"]


# Build a list of test queries, substituting parameters to query templates.
# Queries can be specified inline or loaded from external files via the "file" attribute.
# Multi-statement files (e.g. TPC-H Q15: CREATE VIEW; SELECT; DROP VIEW) are split:
# CREATE/DROP become global setup/teardown queries, and only the SELECT becomes the timed test query. 
# This is a simplification: the setup/teardown is shared across all queries, not per-query.
# This works, but will break if two query files create views with the same name. 
# Not worth fixing properly unless we have such scenario, the whole perf test suite needs a rewrite.
extra_create_queries = []
extra_drop_queries = []
# Each entry is a "query item" dict describing one timed unit of work:
#   {"kind": "sql",   "statements": [...]}  -- run over the native protocol,
#   {"kind": "shell", "script": "..."}      -- run as a bash script, see below.
test_queries = []
for e in root.findall("query"):
    if e.get("type") == "shell":
        # A shell-script query: a snippet of bash executed for each server and
        # measured by its wall-clock time. This lets us benchmark things the
        # native protocol cannot express -- HTTP end-to-end latency, response
        # compression, tool startup time, etc. The script talks to the server
        # using the $CLICKHOUSE_* environment variables (binary, host, ports,
        # client, local, curl, url) prepared per-server, mirroring the stateless
        # tests in tests/queries/shell_config.sh.
        #
        # Parameter substitution is intentionally NOT applied to shell scripts,
        # because they routinely use '${var}' and '{a,b}' brace expansion that
        # would collide with the '{name}' substitution syntax. Settings from the
        # <settings> element are not applied either -- pass them via the URL or
        # client arguments inside the script.
        if not e.text or not e.text.strip():
            raise Exception('Empty <query type="shell"> in the test file')
        test_queries.append({"kind": "shell", "script": e.text})
        continue

    if "file" in e.attrib:
        query_path = os.path.join(xml_dir, e.attrib["file"])
        with open(query_path, "r", encoding="utf-8") as f:
            full_text = f.read().strip()
        statements = split_sql_statements(full_text)
        select_stmts = []
        for stmt in statements:
            keyword = first_keyword(stmt)
            if keyword in ("CREATE", "ALTER"):
                extra_create_queries.append(stmt)
            elif keyword in ("DROP", "TRUNCATE"):
                extra_drop_queries.append(stmt)
            else:
                select_stmts.append(stmt)
        # Some benchmark queries (e.g. TPC-DS Q14) have multiple SELECTs in one file.
        # They are executed together and timed as a single entry in the report.
        # Substitution parameters are expanded per-statement and then zipped, so that each group contains statements with same parameters.
        # Example:
        #   select_stmts = ["SELECT FROM {t}.x", "SELECT FROM {t}.y"], {t} = [a, b]
        #   expanded = [["SELECT FROM a.x", "SELECT FROM b.x"], ["SELECT FROM a.y", "SELECT FROM b.y"]]
        #   zip(*expanded) -> ("SELECT FROM a.x", "SELECT FROM a.y"), ("SELECT FROM b.x", "SELECT FROM b.y")
        expanded = [substitute_parameters([s]) for s in select_stmts]
        for group in zip(*expanded):
            test_queries.append({"kind": "sql", "statements": list(group)})
    else:
        test_queries += [
            {"kind": "sql", "statements": [s]} for s in substitute_parameters([e.text])
        ]


def query_display(item):
    """Human-readable representation of a query item for the report."""
    if item["kind"] == "shell":
        return item["script"]
    return ";\n".join(item["statements"])


# If we're given a list of queries to run, check that it makes sense.
for i in args.queries_to_run or []:
    if i < 0 or i >= len(test_queries):
        print(
            f"There is no query no. {i} in this test, only [{0}-{len(test_queries) - 1}] are present"
        )
        exit(1)

# If we're only asked to print the queries, do that and exit.
if args.print_queries:
    for i in args.queries_to_run or range(0, len(test_queries)):
        print(query_display(test_queries[i]))
    exit(0)

# If we're only asked to print the settings, do that and exit. These are settings
# for clickhouse-benchmark, so we print them as command line arguments, e.g.
# '--max_memory_usage=10000000'.
if args.print_settings:
    # Settings from JSON file: <settings file="path/to/settings.json"/>
    for key, value in load_settings_file(root, xml_dir).items():
        print(f"--{key}={value}")
    # Inline settings: <settings><key>value</key></settings> (can override file settings)
    for s in root.findall("settings/*"):
        print(f"--{s.tag}={s.text}")

    exit(0)

# Skip long tests
if not args.long:
    for tag in root.findall(".//tag"):
        if tag.text == "long":
            print("skipped\tTest is tagged as long.")
            sys.exit(0)

# Shell-script queries do not yet carry the connection options that the SQL path
# honours. SQL queries connect through `clickhouse_driver.Client` with `--user` /
# `--password` / `--secure`, but the per-server shell environment built by
# `shell_env_for` always uses an unauthenticated, plaintext `$CLICKHOUSE_CLIENT`
# and a `http://` `$CLICKHOUSE_URL`. Running shell queries under non-default
# credentials or TLS would therefore either fail outright or, worse, measure a
# different (default plaintext) endpoint than the SQL setup/prewarm connected to,
# silently invalidating the comparison. Until the shell helpers carry these
# options, fail closed: reject the test up front rather than benchmark the wrong
# endpoint. (Done after the `--print-queries` / `--print-settings` early exits so
# those read-only paths are unaffected.)
if any(q["kind"] == "shell" for q in test_queries) and (
    args.user != "default" or args.password != "" or args.secure
):
    raise Exception(
        'Shell-script queries (<query type="shell">) do not support the '
        "--user / --password / --secure connection options yet: the shell "
        "environment always connects without authentication over plaintext HTTP. "
        "Remove these options, or run this test without shell-script queries."
    )

# Print report threshold for the test if it is set.
ignored_relative_change = 0.05
if "max_ignored_relative_change" in root.attrib:
    ignored_relative_change = float(root.attrib["max_ignored_relative_change"])
    print(f"report-threshold\t{ignored_relative_change}")

reportStageEnd("before-connect")

# Open connections
servers = [
    { "host": host or args.host[0], "port": port or args.port[0], "user": args.user, "password": args.password, "secure": args.secure }
    for (host, port) in itertools.zip_longest(args.host, args.port)
]
# Force settings_is_important to fail queries on unknown settings.
all_connections = [
    clickhouse_driver.Client(**server, settings_is_important=True) for server in servers
]

# Long-lived workers to fan out per-connection commands (SYSTEM JEMALLOC PURGE
# etc.) in parallel so both servers see the same operation at the same wall
# clock moment.
purge_pool = ThreadPoolExecutor(max_workers=max(1, len(all_connections)))


def nth_or_first(seq, index):
    """Value parallel to server `index`, reusing the first one for a short list
    (e.g. a single --binary shared by every server)."""
    return seq[index] if index < len(seq) else seq[0]


def shell_env_for(conn_index):
    """Build the environment for a shell-script query targeting server
    `conn_index`. The variable names mirror tests/queries/shell_config.sh so that
    perf shell scripts look like ordinary stateless tests. Each server gets its
    own binary, ports and derived commands, so the same script measures the LEFT
    and the RIGHT build independently."""
    server = servers[conn_index]
    host = str(server["host"])
    tcp_port = str(server["port"])
    http_port = str(nth_or_first(args.http_port, conn_index))
    binary = nth_or_first(args.binary, conn_index)
    # Resolve a relative path (e.g. 'left/clickhouse') to an absolute one so the
    # script can be run from any working directory. A bare name like 'clickhouse'
    # is left untouched and resolved via $PATH.
    if "/" in binary:
        binary = os.path.abspath(binary)

    env = dict(os.environ)
    env["CLICKHOUSE_BINARY"] = binary
    env["CLICKHOUSE_HOST"] = host
    env["CLICKHOUSE_PORT_TCP"] = tcp_port
    env["CLICKHOUSE_PORT_HTTP"] = http_port
    env["CLICKHOUSE_DATABASE"] = "default"
    env["CLICKHOUSE_CLIENT"] = f"{binary} client --host {host} --port {tcp_port}"
    env["CLICKHOUSE_LOCAL"] = f"{binary} local"
    # `--fail` makes curl exit non-zero on an HTTP 4xx/5xx response, and `-S`
    # prints the error even under `-s`. Unlike tests/queries/shell_config.sh
    # (which omits `--fail` because some stateless tests inspect error bodies),
    # a performance benchmark must fail closed: otherwise an HTTP error response
    # would be timed and reported as a fast successful sample, silently
    # invalidating the comparison.
    env["CLICKHOUSE_CURL"] = "curl -q -sS --fail --max-time 120"
    env["CLICKHOUSE_URL"] = f"http://{host}:{http_port}/"
    return env


shell_envs = {}


def run_shell_query(conn_index, script, timeout):
    """Run a shell-script query for one server and return its wall-clock time.

    The script is executed with `bash -e -o pipefail` in its own session
    (`start_new_session=True`), so the whole process tree it spawns shares one
    process group. On timeout the entire group is killed: `subprocess` alone
    would only terminate the immediate `bash`, leaving children such as `curl`
    or `$CLICKHOUSE_LOCAL` running, which could keep consuming CPU/network or
    hold a server-side query open and pollute later measurements. Standard
    output is discarded (the script itself decides what to read and where to
    write it); standard error is captured for diagnostics. A non-zero exit code
    raises an exception, which the caller treats the same way as a failed SQL
    query."""
    env = shell_envs.setdefault(conn_index, shell_env_for(conn_index))
    start = time.perf_counter()
    proc = subprocess.Popen(
        ["bash", "-e", "-o", "pipefail", "-c", script],
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        _, stderr_bytes = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        # Kill the whole process group, not just `bash`, then reap it.
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
        raise
    elapsed = time.perf_counter() - start
    if proc.returncode != 0:
        stderr = stderr_bytes.decode("utf-8", errors="replace").strip()
        raise Exception(
            f"Shell query failed on server {conn_index} with exit code "
            f"{proc.returncode}:\n{stderr}"
        )
    return elapsed


for i, s in enumerate(servers):
    ssl_status = "SSL" if s["secure"] else "no-SSL"
    print(f'server\t{i}\t{s["host"]}\t{s["port"]}\t{s["user"]}\t{ssl_status}')

reportStageEnd("connect")

if not args.use_existing_tables:
    # Run drop queries, ignoring errors. Do this before all other activity,
    # because clickhouse_driver disconnects on error (this is not configurable),
    # and the new connection loses the changes in settings.
    drop_query_templates = [q.text for q in root.findall("drop_query")]
    drop_query_templates += extra_drop_queries
    drop_queries = substitute_parameters(drop_query_templates)
    for conn_index, c in enumerate(all_connections):
        for q in drop_queries:
            try:
                c.execute(q)
                print(f"drop\t{conn_index}\t{c.last_query.elapsed}\t{tsv_escape(q)}")
            except:
                pass

    reportStageEnd("drop-1")

# First apply JSON settings (<settings file="..."/>), then inline (<settings><key>value</key></settings>).
# Inline settings override file settings.
file_settings = load_settings_file(root, xml_dir)
inline_settings = root.findall("settings/*")
for conn_index, c in enumerate(all_connections):
    for key, value in file_settings.items():
        c.settings[key] = str(value)
    for s in inline_settings:
        c.settings[s.tag] = s.text
    # We have to perform a query to make sure the settings work. Otherwise an
    # unknown setting will lead to failing precondition check, and we will skip
    # the test, which is wrong.
    c.execute("select 1")

reportStageEnd("settings")

if not args.use_existing_tables:
    # Run create and fill queries. We will run them simultaneously for both
    # servers, to save time. The weird XML search + filter is because we want to
    # keep the relative order of elements, and etree doesn't support the
    # appropriate xpath query.
    create_query_templates = [
        q.text for q in root.findall("./*") if q.tag in ("create_query", "fill_query")
    ]
    create_query_templates += extra_create_queries
    create_queries = substitute_parameters(create_query_templates)

    # Disallow temporary tables, because the clickhouse_driver reconnects on
    # errors, and temporary tables are destroyed. We want to be able to continue
    # after some errors.
    for q in create_queries:
        if re.search("create temporary table", q, flags=re.IGNORECASE):
            print(
                f"Temporary tables are not allowed in performance tests: '{q}'",
                file=sys.stderr,
            )
            sys.exit(1)

    def do_create(connection, index, queries):
        for q in queries:
            connection.execute(q)
            print(f"create\t{index}\t{connection.last_query.elapsed}\t{tsv_escape(q)}")

    threads = [
        SafeThread(target=do_create, args=(connection, index, create_queries))
        for index, connection in enumerate(all_connections)
    ]

    for t in threads:
        t.start()

    for t in threads:
        t.join()

    reportStageEnd("create")


def purge_jemalloc_on_all_connections(reason):
    def purge_one(indexed_conn):
        conn_index, c = indexed_conn
        try:
            c.execute("SYSTEM JEMALLOC PURGE")
            return f"purging jemalloc arenas\t{conn_index}\t{c.last_query.elapsed}\t{reason}"
        except KeyboardInterrupt:
            raise
        except:
            return None
    for line in purge_pool.map(purge_one, enumerate(all_connections)):
        if line:
            print(line)


if args.jemalloc_purge != "disabled":
    purge_jemalloc_on_all_connections("after-fill")


# Let's sync the data to avoid writeback affects performance
os.system("sync")
reportStageEnd("sync")

# By default, test all queries.
queries_to_run = range(0, len(test_queries))

if args.max_queries:
    # If specified, test a limited number of queries chosen at random.
    queries_to_run = random.sample(
        range(0, len(test_queries)), min(len(test_queries), args.max_queries)
    )

if args.queries_to_run:
    # Run the specified queries.
    queries_to_run = args.queries_to_run

# Run test queries.
profile_total_seconds = 0
for query_index in queries_to_run:
    q_item = test_queries[query_index]
    query_prefix = f"{test_name}.query{query_index}"

    # We have some crazy long queries (about 100kB), so trim them to a sane
    # length. This means we can't use query text as an identifier and have to
    # use the test name + the test-wide query index.
    query_display_name = query_display(q_item)
    if len(query_display_name) > 1000:
        query_display_name = f"{query_display_name[:1000]}...({query_index})"

    print(f"display-name\t{query_index}\t{tsv_escape(query_display_name)}")

    if args.jemalloc_purge in ("before-each-query", "before-each-run"):
        purge_jemalloc_on_all_connections(f"before-query-{query_index}")

    # Prewarm: run once on both servers. Helps to bring the data into memory,
    # precompile the queries, etc.
    # A query might not run on the old server if it uses a function added in the
    # new one. We want to run them on the new server only, so that the PR author
    # can ensure that the test works properly. Remember the errors we had on
    # each server.
    query_error_on_connection = [None] * len(all_connections)
    for conn_index, c in enumerate(all_connections):
        try:
            prewarm_id = f"{query_prefix}.prewarm0"

            if q_item["kind"] == "shell":
                # A failing shell script on the old server (e.g. it uses a tool
                # option added in the new build) is handled the same way as a
                # failing SQL query: the run continues on the rest of the servers.
                prewarm_elapsed = run_shell_query(
                    conn_index, q_item["script"], args.prewarm_max_query_seconds
                )
            else:
                try:
                    # During the warm-up runs, we will also:
                    # * detect queries that are exceedingly long, to fail fast,
                    # * collect profiler traces, which might be helpful for analyzing
                    #   test coverage. We disable profiler for normal runs because
                    #   it makes the results unstable.
                    prewarm_elapsed = execute_query_group(
                        c,
                        q_item["statements"],
                        prewarm_id,
                        {
                            "max_execution_time": args.prewarm_max_query_seconds,
                            "query_profiler_real_time_period_ns": 10000000,
                            "query_profiler_cpu_time_period_ns": 10000000,
                            "metrics_perf_events_enabled": 1,
                            "memory_profiler_step": "4Mi",
                        },
                    )
                except clickhouse_driver.errors.Error as e:
                    # Add query id to the exception to make debugging easier.
                    e.args = (prewarm_id, *e.args)
                    e.message = prewarm_id + ": " + e.message
                    raise

            print(
                f"prewarm\t{query_index}\t{prewarm_id}\t{conn_index}\t{prewarm_elapsed}"
            )
        except KeyboardInterrupt:
            raise
        except:
            # FIXME the driver reconnects on error and we lose settings, so this
            # might lead to further errors or unexpected behavior.
            query_error_on_connection[conn_index] = traceback.format_exc()
            continue

    # Report all errors that occurred during prewarm and decide what to do next.
    # If prewarm fails for the query on all servers -- skip the query and
    # continue testing the next query.
    # If prewarm fails on one of the servers, run the query on the rest of them.
    no_errors = []
    for i, e in enumerate(query_error_on_connection):
        if e:
            print(e, file=sys.stderr)
        else:
            no_errors.append(i)

    # A shell-script query is a benchmark we control end to end, so -- unlike an
    # SQL query that may legitimately use a function missing from the old server
    # -- it is expected to run on every server. If it fails on any of them the
    # comparison is meaningless. Previously such a failure was swallowed as a
    # "partial" query: the run continued on whatever server survived and the
    # query was dropped from the report and CIDB with no error anywhere (empty
    # run-errors.tsv, green job), so a broken shell test looked like it simply
    # produced no data. Fail loudly instead, and echo the captured error from
    # each failing server to stdout as a `run-error` line so it survives in the
    # archived per-test raw .tsv (the per-test stderr log is not uploaded).
    # compare.sh parses raw .tsv by known leading tag and ignores the rest, so
    # adding this tag is safe.
    if q_item["kind"] == "shell" and len(no_errors) < len(all_connections):
        failed = []
        for i, e in enumerate(query_error_on_connection):
            if e:
                failed.append(i)
                print(f"run-error\t{query_index}\t{i}\t{tsv_escape(e)}")
        # Flush before raising so the diagnostics reach the raw .tsv even though
        # we are about to exit through an unhandled exception.
        sys.stdout.flush()
        raise Exception(
            f"Shell query {query_prefix} failed on server(s) {failed}: a shell "
            "performance test must run on all servers to be comparable. See the "
            "'run-error' lines in the raw output for the captured stderr."
        )

    if len(no_errors) == 0:
        continue
    elif len(no_errors) < len(all_connections):
        print(f"partial\t{query_index}\t{no_errors}")

    this_query_connections = [all_connections[index] for index in no_errors]

    # Now, perform measured runs.
    # Track the time spent by the client to process this query, so that we can
    # notice the queries that take long to process on the client side, e.g. by
    # sending excessive data.
    start_seconds = time.perf_counter()
    server_seconds = 0
    profile_seconds = 0
    run = 0

    # Arrays of run times for each connection.
    all_server_times = []
    for conn_index, c in enumerate(this_query_connections):
        all_server_times.append([])

    while True:
        run_id = f"{query_prefix}.run{run}"

        if args.jemalloc_purge == "before-each-run":
            purge_jemalloc_on_all_connections(f"before-{run_id}")

        # Alternate the server order each measured run (L,R,R,L,L,R,...) so that
        # the post-query / pre-purge idle time averages out between connections.
        conn_order = list(enumerate(this_query_connections))
        if run % 2 == 1:
            conn_order = list(reversed(conn_order))

        for conn_index, c in conn_order:
            # conn_index addresses this_query_connections (the servers that
            # survived prewarm); map it back to the real server to pick the right
            # binary/ports for a shell query.
            server_index = no_errors[conn_index]
            if q_item["kind"] == "shell":
                try:
                    elapsed = run_shell_query(
                        server_index, q_item["script"], args.max_query_seconds
                    )
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    raise Exception(f"{run_id}: {e}")
            else:
                try:
                    elapsed = execute_query_group(
                        c,
                        q_item["statements"],
                        run_id,
                        {"max_execution_time": args.max_query_seconds},
                    )
                except clickhouse_driver.errors.Error as e:
                    # Add query id to the exception to make debugging easier.
                    e.args = (run_id, *e.args)
                    e.message = run_id + ": " + e.message
                    raise

            all_server_times[conn_index].append(elapsed)

            server_seconds += elapsed
            print(f"query\t{query_index}\t{run_id}\t{conn_index}\t{elapsed}")

            if elapsed > args.max_query_seconds:
                # Do not stop processing pathologically slow queries,
                # since this may hide errors in other queries.
                print(
                    f"The query no. {query_index} is taking too long to run ({elapsed} s)",
                    file=sys.stderr,
                )

        # Be careful with the counter, after this line it's the next iteration
        # already.
        run += 1

        avg_time_per_server = server_seconds / len(this_query_connections)

        # We break if all the min stop conditions are met (1 second arg.runs iterations)
        # or at lest one of the max stop conditions is met (8 seconds or 500 iterations)
        if (avg_time_per_server >= 1 and run >= args.runs) or (
            avg_time_per_server >= 8 or run >= 500
        ):
            break

    client_seconds = time.perf_counter() - start_seconds
    print(f"client-time\t{query_index}\t{client_seconds}\t{server_seconds}")
    median = [statistics.median(t) for t in all_server_times]
    print(f"median\t{query_index}\t{median[0]}")

    # Run additional profiling queries to collect profile data, but only if test times appeared to be different.
    # We have to do it after normal runs because otherwise it will affect test statistics too much
    if len(all_server_times) != 2:
        continue

    if len(all_server_times[0]) < 3:
        # Don't fail if for some reason there are not enough measurements.
        continue

    pvalue = stats.ttest_ind(
        all_server_times[0], all_server_times[1], equal_var=False
    ).pvalue
    # Keep this consistent with the value used in report. Should eventually move
    # to (median[1] - median[0]) / min(median), which is compatible with "times"
    # difference we use in report (max(median) / min(median)).
    relative_diff = (median[1] - median[0]) / median[0]
    print(f"diff\t{query_index}\t{median[0]}\t{median[1]}\t{relative_diff}\t{pvalue}")
    if abs(relative_diff) < ignored_relative_change or pvalue > 0.05:
        continue

    if q_item["kind"] == "shell":
        # The server-side profiler runs cannot be attributed to a shell script
        # (there is no single query to profile), so we stop after reporting the
        # timing difference.
        continue

    # Perform profile runs for fixed amount of time. Don't limit the number
    # of runs, because we also have short queries.
    profile_start_seconds = time.perf_counter()
    run = 0
    while time.perf_counter() - profile_start_seconds < args.profile_seconds:
        run_id = f"{query_prefix}.profile{run}"

        for conn_index, c in enumerate(this_query_connections):
            try:
                profile_elapsed = execute_query_group(
                    c,
                    q_item["statements"],
                    run_id,
                    {
                        "query_profiler_real_time_period_ns": 10000000,
                        "query_profiler_cpu_time_period_ns": 10000000,
                        "metrics_perf_events_enabled": 1,
                        # Dedicated profile runs are not timed, so we can afford
                        # the overhead of allocation sampling to also collect
                        # MemorySample and JemallocSample stacks for flamegraphs.
                        "memory_profiler_sample_probability": 0.1,
                        "jemalloc_enable_profiler": 1,
                        "jemalloc_collect_profile_samples_in_trace_log": 1,
                    },
                )
                print(
                    f"profile\t{query_index}\t{run_id}\t{conn_index}\t{profile_elapsed}"
                )
            except clickhouse_driver.errors.Error as e:
                # Add query id to the exception to make debugging easier.
                e.args = (run_id, *e.args)
                e.message = run_id + ": " + e.message
                raise

        run += 1

    profile_total_seconds += time.perf_counter() - profile_start_seconds

print(f"profile-total\t{profile_total_seconds}")

reportStageEnd("run")

# Run drop queries
if not args.keep_created_tables and not args.use_existing_tables:
    drop_queries = substitute_parameters(drop_query_templates)
    for conn_index, c in enumerate(all_connections):
        for q in drop_queries:
            c.execute(q)
            print(f"drop\t{conn_index}\t{c.last_query.elapsed}\t{tsv_escape(q)}")

    reportStageEnd("drop-2")
