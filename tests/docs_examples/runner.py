#!/usr/bin/env python3
"""Run the SQL examples embedded in the ClickHouse documentation.

Functions, aggregate functions, table functions and other components carry their reference
documentation in the source code (`FunctionDocumentation` / `Documentation`), including named
examples made of a query and the response it produces. The same content is exposed by the
`system.documentation` table and published on the website, so a broken example is a broken
documentation page.

This runner takes the examples from a running server, executes them, and compares the output with
the response the documentation claims. The examples of one entity are executed in order in a single
session and in a database of their own, because a documentation page is written to be followed from
top to bottom: one example commonly creates the table that a later example queries.

The runner covers the structured `examples` fields of the embedded documentation - the ones
`FunctionDocumentation::examplesAsString` renders as unquoted ```sql title=Query fences. Examples
spelled by hand inside free-form description Markdown (quoted ```sql title="Query" fences, often
paired with `text`, `json` or unmarked response fences) are not parsed and not run; extending the
parser to that form is a known follow-up, so edits to those pages are not checked by this runner
yet.

An example whose statements create or change global, server-wide state (users, roles, quotas,
databases, writes through a table function into a file or external storage) cannot be isolated
inside the scratch database of its entity, so by default it is skipped and
reported as `skipped`. The runner always creates and drops scratch databases, so it must be pointed
at a dedicated server and needs permission to create and drop databases. Pass `--global-objects` to
run these examples too; that additionally permits changes to server-wide state outside the scratch
database.

Similarly, an example that calls an external service (the `ai*` functions, which send prompts to a
model provider) is skipped by default: on a server with provider credentials configured, running it
would make real outbound calls, incur spend, and ship the example's text off the box. Pass
`--external-calls` to run these examples too.

Each example that runs gets one of three outcomes:

  * `ok`     - the example ran, and its output matches the documented response (or the example
               documents no response, or it documents an exception and indeed threw it);
  * `error`  - the example failed to run (or documents an exception and did not throw it);
  * `output` - the example ran, but its output differs from the documented response.

Examples that are not `ok` must be listed in the known-failures file, which records why each one
cannot pass. The run fails if an example fails that is not on the list, and also if a listed example
starts passing, so that the list shrinks as the documentation is fixed. Regenerate it with
`--update-known-failures` (which requires a complete run: `--global-objects`, `--external-calls`
and no `--filter`)
after checking that every new entry is justified. An entry can also be
marked `unstable`, for the handful of examples whose output is random enough to sometimes match the
documented one; any outcome of those is accepted.

`--filter` narrows the entities that are executed, and the baseline is still honoured for them: an
entry is reported as stale only when the server has no such example at all, not when the run simply
did not select it, so a filtered run is red exactly when one of the examples it selected is.

Usage:

    python3 tests/docs_examples/runner.py --port 8123
    python3 tests/docs_examples/runner.py --port 8123 --filter 'argM' --verbose
"""

import argparse
import http.client
import json
import os
import re
import sys
import urllib.parse
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

DEFAULT_KNOWN_FAILURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "known_failures.txt")

# The opening fence of a structured example query, as `FunctionDocumentation::examplesAsString`
# renders it. Also the only fenced block allowed to follow an example that documents no response.
QUERY_FENCE = "```sql title=Query"

# The examples are read back from the Markdown that `system.documentation` renders, where
# `FunctionDocumentation::examplesAsString` wraps every example into a pair of fenced code blocks
# with these exact info strings. Hand-written Markdown in a description spells the title with quotes
# (```sql title="Query"), so the unquoted form selects the structured examples and nothing else.
EXAMPLE_RE = re.compile(
    r"^```sql title=Query\n(?P<query>.*?)\n```[ \t]*$"
    r"(?:\s*^```response title=Response\n(?P<result>.*?)\n```[ \t]*$)?",
    re.DOTALL | re.MULTILINE,
)

# A sanity bound on the number of examples found, so that a change in the way the documentation is
# rendered cannot silently turn this into a test that checks nothing. Keep it comfortably below the
# actual count; raise it when the documentation grows substantially.
MIN_EXAMPLES = 1800

# An example whose documented response is an exception demonstrates an error on purpose (`throwIf`,
# `aggThrow`). It is expected to fail, and the text of the message is not compared: it carries a
# version number and a query pipeline description that are not part of what the example teaches. The
# error code is compared, though, so that the example fails for the reason it is meant to show.
DOCUMENTED_EXCEPTION_RE = re.compile(r"\A(Received exception|Code: \d+\. DB::Exception)")
ERROR_CODE_RE = re.compile(r"Code:\s*(\d+)")

# The documented responses show the plain rendering of the Pretty formats: no row numbers, no ANSI
# colors, long column names spelled out in full, no readable-number tip next to a single large
# number, a named tuple printed as a tuple rather than as JSON, and no repetition of the column
# names in a footer. Pinning that keeps the comparison about the data and about the shape of the
# result, rather than about a rendering default that changed after a page was written.
OUTPUT_SETTINGS = {
    "output_format_pretty_row_numbers": "0",
    "output_format_pretty_color": "0",
    "output_format_pretty_max_column_name_width_cut_to": "1000",
    "output_format_pretty_max_column_name_width_min_chars_to_cut": "1000",
    "output_format_pretty_single_large_number_tip_threshold": "0",
    "output_format_pretty_named_tuples_as_json": "0",
    "output_format_pretty_display_footer_column_names": "0",
}

OK, ERROR, OUTPUT = "ok", "error", "output"
# Not an outcome, only a known-failures entry: accept whatever this example does.
UNSTABLE = "unstable"
# The example creates global, server-wide objects and `--global-objects` was not given, or it
# calls an external service and `--external-calls` was not given.
SKIPPED = "skipped"

# A statement that creates, changes or removes state that lives outside the scratch database of
# the entity: RBAC objects, databases, named collections, the grants that go with them, and writes
# through a table function, which land in a file under `user_files` or in external storage rather
# than in a table of the scratch database. Such an example cannot run concurrently with another
# invocation of itself, and must not run against a server that is not dedicated to this check.
GLOBAL_OBJECT_RE = re.compile(
    r"^\s*(?:CREATE|ATTACH|ALTER|DROP|RENAME)\s+(?:OR\s+REPLACE\s+)?"
    r"(?:USER|ROLE|ROW\s+POLICY|QUOTA|SETTINGS\s+PROFILE|PROFILE|DATABASE|NAMED\s+COLLECTION)\b"
    r"|^\s*(?:GRANT|REVOKE)\b"
    r"|^\s*INSERT\s+INTO\s+(?:TABLE\s+)?FUNCTION\b",
    re.IGNORECASE,
)

# `generateSerialID` stores a counter in Keeper under the server-wide
# `series_keeper_path`. Its state is therefore not isolated by the scratch database.
GLOBAL_STATE_FUNCTION_RE = re.compile(r"\bgenerateSerialID\s*\(", re.IGNORECASE)

# A call of a function that reaches out to an external service: the `ai*` family sends the given
# text to a model provider. On a server that has provider credentials configured, such an example
# would make a real outbound call, incur spend, and ship the example's text off the box, so it only
# runs with an explicit opt-in. The functions have both `ai*` and `AI*` registered spellings.
EXTERNAL_CALL_RE = re.compile(r"\b(?:ai|AI)[A-Z][A-Za-z0-9]*\s*\(")


class Example:
    """One documented example, and where it came from."""

    def __init__(self, entity_type, entity_name, source, index, query, result):
        self.entity_type = entity_type
        self.entity_name = entity_name
        self.source = source
        self.index = index
        self.query = query
        self.result = result
        # The type without its space, so that an entry of the known-failures file is one word.
        self.id = f"{entity_type.replace(' ', '')}/{entity_name}#{index}"

    @property
    def expects_exception(self):
        return bool(DOCUMENTED_EXCEPTION_RE.match(self.result))

    @property
    def creates_global_objects(self):
        return any(GLOBAL_OBJECT_RE.match(strip_leading_comments(statement)) for statement in split_statements(self.query)) or bool(
            GLOBAL_STATE_FUNCTION_RE.search(self.query)
        )

    @property
    def calls_external_services(self):
        return bool(EXTERNAL_CALL_RE.search(self.query))

    @property
    def output_format(self):
        """The format the documented response is written in.

        The response is a verbatim copy of what the server printed, so its shape identifies the
        format: a box drawing for the Pretty family, `Row 1:` for `Vertical`, and tab separated
        values for everything else. A query that carries its own `FORMAT` clause overrides this.
        """
        if self.result.startswith(("┌", "┏", "╔", "─", "━")):
            return "PrettyCompact"
        if self.result.startswith("Row 1:"):
            return "Vertical"
        return "TSV"


class Outcome:
    def __init__(self, example, status, detail=""):
        self.example = example
        self.status = status
        self.detail = detail


def split_statements(script):
    """Split a script into individual statements at the semicolons that separate them.

    The HTTP interface takes one statement per request, while an example is commonly a small script
    (create a table, fill it, query it). Semicolons inside string literals, quoted identifiers and
    comments do not separate anything, so they are skipped over. A piece that holds nothing but a
    comment, such as the note an example often ends with, is not a statement and is dropped.
    """
    statements = []
    start = 0
    i = 0
    n = len(script)
    has_code = False

    def take(end):
        nonlocal start, has_code
        if has_code:
            statements.append(script[start:end].strip())
        start = end + 1
        has_code = False

    while i < n:
        c = script[i]
        if c in "'\"`":
            quote = c
            has_code = True
            i += 1
            while i < n:
                if script[i] == "\\":
                    i += 2
                    continue
                if script[i] == quote:
                    i += 1
                    break
                i += 1
        elif script.startswith("--", i):
            i = script.find("\n", i)
            if i < 0:
                i = n
        elif script.startswith("/*", i):
            end = script.find("*/", i + 2)
            i = n if end < 0 else end + 2
        elif c == ";":
            take(i)
            i += 1
        else:
            has_code = has_code or not c.isspace()
            i += 1

    take(n)
    return statements


def strip_leading_comments(statement):
    """The statement with the comments before its first token removed.

    `split_statements` keeps comments in place, so a statement may open with an explanatory `--` or
    `/* */` comment. A classification anchored at the start of the statement must look at its first
    token: otherwise a leading comment would hide a `CREATE USER` from the global-object check.
    """
    i = 0
    n = len(statement)
    while i < n:
        if statement.startswith("--", i):
            end = statement.find("\n", i)
            i = n if end < 0 else end + 1
        elif statement.startswith("/*", i):
            end = statement.find("*/", i + 2)
            i = n if end < 0 else end + 2
        elif statement[i].isspace():
            i += 1
        else:
            break
    return statement[i:]


class Client:
    """A minimal HTTP client for the server, with one fresh connection per statement."""

    def __init__(self, host, port, user, password, timeout):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout

    def query(self, sql, **params):
        """Run one statement. Returns (ok, output_or_error_message)."""
        params = {
            "user": self.user,
            "password": self.password,
            # Buffer the response so that an error in the middle of the output is reported as a
            # failed request instead of a truncated body with the message appended to it.
            "wait_end_of_query": "1",
            **params,
        }
        url = "/?" + urllib.parse.urlencode(params)
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            connection.request("POST", url, body=sql.encode())
            response = connection.getresponse()
            body = response.read().decode("utf-8", "replace")
            return response.status == 200, body
        except (http.client.HTTPException, OSError) as e:
            # Retrying can replay a mutating statement after the server has accepted it.
            return False, f"Connection error: {e}"
        finally:
            connection.close()


def load_examples(client):
    """Read the documentation from the server and cut the examples out of it."""
    ok, body = client.query(
        "SELECT name, toString(type) AS type, description, source FROM system.documentation WHERE description LIKE '%```sql title=Query%' ORDER BY type, name",
        default_format="JSONEachRow",
    )
    if not ok:
        raise RuntimeError(f"Cannot read system.documentation: {body}")

    examples = []
    for line in body.splitlines():
        row = json.loads(line)
        for index, match in enumerate(EXAMPLE_RE.finditer(row["description"])):
            if match.group("result") is None:
                reject_unrecognized_response_fence(row, index, match)
            examples.append(
                Example(
                    row["type"],
                    row["name"],
                    row["source"],
                    index,
                    match.group("query").strip(),
                    (match.group("result") or "").strip(),
                )
            )
    return examples


def reject_unrecognized_response_fence(row, index, match):
    """Fail closed on a fenced block that follows a query but was not recognized as its response.

    `EXAMPLE_RE` makes the response block optional, because an example is allowed to document no
    response. Left alone, that fails open: if `FunctionDocumentation::examplesAsString` changes how
    it renders responses, every query still matches on its own, and the run silently degrades from
    comparing the output to merely checking that the query runs. The rendering puts nothing between
    a query and its response, so the only fenced block that may follow a query directly is the query
    of the next example; anything else - a differently spelled ```` ```response ```` fence, or a
    response rendered as ```` ```text ````, ```` ```json ````, ... - is renderer drift and stops the
    run.
    """
    following = row["description"][match.end():].lstrip()
    if not following.startswith("```"):
        return
    fence = following.splitlines()[0]
    if fence.strip() == QUERY_FENCE:
        return
    raise RuntimeError(
        f"{row['type']}/{row['name']}: example {index} documents no response and is followed by a fenced block"
        f" the parser does not recognize as its response: {fence!r}."
        f"\nThe way the documentation renders responses has probably changed; update EXAMPLE_RE in {__file__}."
    )


def normalize(text):
    """Compare responses the way the documentation renders them.

    `FunctionDocumentation::examplesAsString` trims the documented response before wrapping it into
    its fenced block, so leading and trailing whitespace of a response is not observable here, while
    the response of the server always ends with a line terminator. Everything between the ends is
    kept intact, including `TSV` separators and empty rows.
    """
    return text.strip()


# The scratch databases and sessions carry a name unique to this run, so that concurrent
# invocations, the leftovers of an interrupted run, or a database that happens to exist on the
# target server cannot collide with them.
RUN_ID = uuid.uuid4().hex[:8]


def run_entity(client, entity_index, examples, global_objects, external_calls):
    """Run all examples of one entity, in order, in one session and one database of its own."""
    database = f"docs_examples_{RUN_ID}_{entity_index}"
    session = f"docs_examples_{RUN_ID}_{entity_index}"
    outcomes = []

    ok, body = client.query(f"CREATE DATABASE {database}")
    if not ok:
        return [Outcome(e, ERROR, f"Cannot create the database for the example: {body}") for e in examples]

    try:
        for example in examples:
            if not global_objects and example.creates_global_objects:
                outcomes.append(
                    Outcome(
                        example,
                        SKIPPED,
                        "The example creates global, server-wide objects; pass --global-objects to run it",
                    )
                )
            elif not external_calls and example.calls_external_services:
                outcomes.append(
                    Outcome(
                        example,
                        SKIPPED,
                        "The example calls an external service; pass --external-calls to run it",
                    )
                )
            else:
                outcomes.append(run_example(client, database, session, example))
    finally:
        client.query(f"DROP DATABASE IF EXISTS {database} SYNC")

    return outcomes


def error_code(message):
    """The numeric code of an exception message, or None if it carries none."""
    match = ERROR_CODE_RE.search(message)
    return int(match.group(1)) if match else None


def run_example(client, database, session, example):
    statements = split_statements(example.query)
    output = []
    for number, statement in enumerate(statements, 1):
        ok, body = client.query(
            statement,
            database=database,
            session_id=session,
            default_format=example.output_format,
            **OUTPUT_SETTINGS,
        )
        if ok:
            output.append(body)
            continue
        if not example.expects_exception:
            return Outcome(example, ERROR, body.strip())
        # An example that documents an exception documents it for its last statement: what comes
        # before is the setup that has to succeed for the demonstration to show anything. Accepting
        # a failure anywhere would turn a broken setup into a passing example.
        if number != len(statements):
            return Outcome(
                example,
                ERROR,
                f"The example documents an exception, but statement {number} of {len(statements)} failed before the last one:\n{body.strip()}",
            )
        documented_code = error_code(example.result)
        actual_code = error_code(body)
        if documented_code is not None and documented_code != actual_code:
            return Outcome(
                example,
                ERROR,
                f"The example documents the error {documented_code}, and got {actual_code} instead:\n{body.strip()}",
            )
        return Outcome(example, OK)

    if example.expects_exception:
        return Outcome(example, ERROR, "The example documents an exception, but the query succeeded")
    if not example.result:
        return Outcome(example, OK)

    actual = normalize("".join(output))
    if actual == normalize(example.result):
        return Outcome(example, OK)
    return Outcome(example, OUTPUT, actual)


def load_known_failures(path):
    """Read the baseline. Returns {example_id: (status, comment)}."""
    known = {}
    if not os.path.isfile(path):
        return known
    with open(path, "r", encoding="utf-8") as f:
        for number, line in enumerate(f, 1):
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            # The example name itself holds a `#`, so the fields come first and the comment is
            # whatever follows the status.
            fields = line.split(maxsplit=2)
            comment = fields[2].strip().lstrip("#").strip() if len(fields) > 2 else ""
            if len(fields) < 2 or fields[1] not in (ERROR, OUTPUT, UNSTABLE) or (len(fields) > 2 and not fields[2].lstrip().startswith("#")):
                raise RuntimeError(f"{path}:{number}: expected '<example> {ERROR}|{OUTPUT}|{UNSTABLE}  # why', got: {line.strip()}")
            known[fields[0]] = (fields[1], comment)
    return known


def save_known_failures(path, outcomes, known):
    """Write the baseline, keeping the comment of every entry that is still there."""
    unstable = {i for i, (status, _) in known.items() if status == UNSTABLE}
    entries = []
    for o in outcomes:
        # A skipped example did not run, so nothing new is known about it: keep its entry as it is.
        if o.status == SKIPPED:
            if o.example.id in known:
                entries.append((o.example, known[o.example.id][0]))
        elif o.status != OK or o.example.id in unstable:
            entries.append((o.example, UNSTABLE if o.example.id in unstable else o.status))
    entries.sort(key=lambda e: (e[0].entity_type, e[0].entity_name, e[0].index))
    width = max((len(e[0].id) for e in entries), default=0)
    with open(path, "w", encoding="utf-8") as f:
        f.write(HEADER)
        for example, status in entries:
            comment = known.get(example.id, (None, ""))[1]
            line = f"{example.id:<{width}} {status}"
            f.write(f"{line}  # {comment}\n" if comment else f"{line}\n")
    print(f"Wrote {len(entries)} entries to {path}")


HEADER = """\
# The examples of the embedded documentation that do not pass yet, with the reason for each one.
# See tests/docs_examples/runner.py; regenerate with its `--update-known-failures`.
#
# Every line is `<Type>/<name>#<index> error|output|unstable  # why`, where `error` means the
# example does not run, `output` means it runs but does not produce the response the documentation
# shows, and `unstable` means its output is random and any outcome is accepted.
# An entry is not an excuse: fix the example and drop the line whenever you can.

"""


def classify(outcome, known):
    """The verdict of one outcome against the baseline: how the run treats it.

    `ok` and `known` pass. `unstable` passes whatever the status is. `skipped` passes as well: the
    example did not run, so nothing can be said about it, and its baseline entry, if any, is kept.
    `unexpected` fails the run, and so does `fixed`, because the baseline entry has to be removed.
    """
    if outcome.status == SKIPPED:
        return SKIPPED
    known_status = known.get(outcome.example.id, (None, ""))[0]
    if known_status == UNSTABLE:
        return "unstable"
    if outcome.status == OK:
        return "fixed" if known_status is not None else OK
    return "known" if known_status == outcome.status else "unexpected"


def stale_entries(example_ids, known):
    """The baseline entries whose examples no longer exist. They fail the run as well.

    The scope is every example the server has, not only the ones this run executed, so that a
    `--filter`ed run does not call the whole rest of the baseline stale.
    """
    return sorted(i for i in known if i not in example_ids)


def report(outcomes, known, example_ids, verbose):
    """Compare the outcomes with the baseline and print what changed. Returns True if all is well."""
    unexpected = [o for o in outcomes if classify(o, known) == "unexpected"]
    fixed = sorted(o.example.id for o in outcomes if classify(o, known) == "fixed")
    stale = stale_entries(example_ids, known)

    total = len(outcomes)
    skipped = sum(1 for o in outcomes if o.status == SKIPPED)
    failing = sum(1 for o in outcomes if o.status not in (OK, SKIPPED))
    entities = {(o.example.entity_type, o.example.entity_name) for o in outcomes}
    print(f"\nRan {total - skipped} examples of {len(entities)} entities: {total - skipped - failing} ok, {failing} not ok ({len(known)} known)")
    if skipped:
        print(
            f"{skipped} example(s) create global, server-wide objects or call external services and were skipped; pass --global-objects and/or --external-calls to run them against a dedicated server"
        )

    if unexpected:
        print(f"\n{len(unexpected)} example(s) unexpectedly not ok:\n")
        for outcome in sorted(unexpected, key=lambda o: o.example.id):
            example = outcome.example
            print(f"  {example.id}  [{outcome.status}]  {example.source}")
            print(indent(example.query, "    | "))
            if outcome.status == OUTPUT:
                print("    documented response:")
                print(indent(example.result, "    - "))
                print("    actual response:")
                print(indent(outcome.detail, "    + "))
            else:
                print(indent(outcome.detail, "    ! "))
            print()

    if fixed:
        print(f"\n{len(fixed)} example(s) now pass and must be removed from the known failures:")
        for example_id in fixed:
            print(f"  {example_id}")

    if stale:
        print(f"\n{len(stale)} known failure(s) no longer exist and must be removed:")
        for example_id in stale:
            print(f"  {example_id}")

    if verbose:
        print("\nAll outcomes:")
        for outcome in sorted(outcomes, key=lambda o: o.example.id):
            print(f"  {outcome.status:7} {outcome.example.id}")

    return not (unexpected or fixed or stale)


def indent(text, prefix):
    return "\n".join(prefix + line for line in text.splitlines())


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8123, help="the HTTP port of the dedicated server")
    parser.add_argument("--user", default="default")
    parser.add_argument("--password", default="")
    parser.add_argument("--jobs", type=int, default=8, help="how many entities to run in parallel")
    parser.add_argument("--timeout", type=int, default=120, help="per-statement timeout, seconds")
    parser.add_argument("--known-failures", default=DEFAULT_KNOWN_FAILURES)
    parser.add_argument("--update-known-failures", action="store_true", help="rewrite the known failures from this run")
    parser.add_argument("--filter", help="run only the entities whose name matches this regular expression")
    parser.add_argument(
        "--global-objects",
        action="store_true",
        help="run the examples that create or change global, server-wide state (users, roles,"
        " databases, files written through a table function);"
        " additionally requires a server dedicated to this check",
    )
    parser.add_argument(
        "--external-calls",
        action="store_true",
        help="run the examples that call an external service (the ai* functions); on a server with provider credentials configured they make real outbound calls",
    )
    parser.add_argument("--report", help="write the outcome of every example to this file, as JSON")
    parser.add_argument("--verbose", action="store_true", help="list the outcome of every example")
    args = parser.parse_args()

    # Only a complete run describes the whole picture: a filtered run would drop every other entry,
    # and a run without --global-objects skips the global-object examples, so the rewritten baseline
    # would silently disagree with the CI mode.
    if args.update_known_failures and args.filter:
        print("--update-known-failures cannot be combined with --filter", file=sys.stderr)
        return 1
    if args.update_known_failures and not args.global_objects:
        print("--update-known-failures requires --global-objects", file=sys.stderr)
        return 1
    if args.update_known_failures and not args.external_calls:
        print("--update-known-failures requires --external-calls", file=sys.stderr)
        return 1

    client = Client(args.host, args.port, args.user, args.password, args.timeout)
    examples = load_examples(client)

    if len(examples) < MIN_EXAMPLES:
        print(
            f"Found only {len(examples)} examples in system.documentation, expected at least {MIN_EXAMPLES}."
            f"\nThe way the documentation renders examples has probably changed; update EXAMPLE_RE in {__file__}.",
            file=sys.stderr,
        )
        return 1

    by_entity = defaultdict(list)
    for example in examples:
        by_entity[(example.entity_type, example.entity_name)].append(example)

    entities = sorted(by_entity)
    if args.filter:
        pattern = re.compile(args.filter)
        entities = [e for e in entities if pattern.search(e[1])]
    print(f"Running {sum(len(by_entity[e]) for e in entities)} examples of {len(entities)} entities")

    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        outcomes = [
            outcome
            for group in pool.map(
                lambda i: run_entity(client, i, by_entity[entities[i]], args.global_objects, args.external_calls),
                range(len(entities)),
            )
            for outcome in group
        ]

    known = load_known_failures(args.known_failures)
    # Every example the server has, whether this run executed it or not: the baseline is written
    # against the documentation, so what makes an entry stale is the example being gone.
    example_ids = {example.id for example in examples}

    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "examples": [
                        {
                            "id": o.example.id,
                            "type": o.example.entity_type,
                            "name": o.example.entity_name,
                            "source": o.example.source,
                            "status": o.status,
                            "known": known.get(o.example.id, ("", ""))[0],
                            "verdict": classify(o, known),
                            "query": o.example.query,
                            "documented": o.example.result,
                            "detail": o.detail,
                        }
                        for o in sorted(outcomes, key=lambda o: o.example.id)
                    ],
                    "stale": stale_entries(example_ids, known),
                },
                f,
                indent=1,
            )

    if args.update_known_failures:
        save_known_failures(args.known_failures, outcomes, known)
        return 0

    return 0 if report(outcomes, known, example_ids, args.verbose) else 1


if __name__ == "__main__":
    sys.exit(main())
