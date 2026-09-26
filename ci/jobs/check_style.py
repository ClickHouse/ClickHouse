import argparse
import json
import math
import multiprocessing
import os
import re
import shlex
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from praktika.info import Info
from praktika.result import Result
from praktika.utils import Shell, Utils
from ci.jobs.scripts.settings_history import (
    SETTINGS_DECLARATION_FILES,
    current_version as settings_current_version,
    violation_message as settings_violation_message,
    violations as settings_history_violations,
)
from ci.jobs.scripts.check_style.clickhouse_spelling import (
    CLICKHOUSE_ANY_SPELLING,
    CLICKHOUSE_CORRECT_SPELLINGS,
    clickhouse_misspellings,
)

NPROC = multiprocessing.cpu_count()


def chunk_list(data, n):
    """Split the data list into n nearly equal-sized chunks."""
    chunk_size = math.ceil(len(data) / n)
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def run_check_concurrent(check_name, check_function, files, nproc=NPROC):
    stop_watch = Utils.Stopwatch()

    if not files:
        print(f"File list is empty [{files}]")
        raise

    file_chunks = list(chunk_list(files, nproc))
    results = []

    # Run check_function concurrently on each chunk
    with ProcessPoolExecutor(max_workers=NPROC) as executor:
        futures = [executor.submit(check_function, chunk) for chunk in file_chunks]
        # Wait for results and process them (optional)
        for future in futures:
            try:
                res = future.result()
                if res and res not in results:
                    results.append(res)
            except Exception as e:
                results.append(f"Exception in {check_name}: {e}")

    result = Result(
        name=check_name,
        status=Result.Status.OK if not results else Result.Status.FAIL,
        start_time=stop_watch.start_time,
        duration=stop_watch.duration,
        info="\n".join(results) if results else "",
    )
    return result


def check_duplicate_includes(file_path):
    includes = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if re.match(r"^#include ", line):
                includes.append(line.strip())

    include_counts = {line: includes.count(line) for line in includes}
    duplicates = {line: count for line, count in include_counts.items() if count > 1}

    if duplicates:
        return f"{file_path}: {duplicates}"
    return ""


def _embedded_doc_lines(lines):
    """Return the set of 0-based indices of lines covered by R"DOCS_MD( ... )DOCS_MD"
    raw-string literals (verbatim Markdown documentation embedded into source files)."""
    exempt = set()
    in_raw = False
    for i, line in enumerate(lines):
        if not in_raw:
            idx = line.find('R"DOCS_MD(')
            if idx != -1:
                exempt.add(i)
                if ')DOCS_MD"' not in line[idx + len('R"DOCS_MD(') :]:
                    in_raw = True
        else:
            exempt.add(i)
            if ')DOCS_MD"' in line:
                in_raw = False
    return exempt


def check_whitespaces(files) -> str:
    """
    Returns True if all files pass (no ugly double spaces after comma
    outside of alignment/exception cases). Prints each offending line
    as: "<file>:<line_number><original line>".
    """
    # Exceptions: lines matching any of these patterns are skipped
    EXCEPTIONS = [
        re.compile(r'^\s*"SELECT splitByWhitespace\(\'[^\']*\'\);",$'),
    ]

    # Detect ",  " or ",   " followed by a non-space and not a slash
    DOUBLE_WS_AFTER_COMMA = re.compile(r",( {2,3})[^ /]")

    # Exempt lines that look like number tables, e.g. "{ 10, -1,  2 }"
    NUM_TABLE_RE = re.compile(r"(?:-?\d+\w*,\s+){3,}")

    # Alignment check on neighboring lines at the same column
    ALIGN_RE = re.compile(r"^[ -][^ ]$")

    violations = []

    for file in files:
        try:
            with open(file, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except OSError as e:
            print(f"{file}: could not read file: {e}")
            violations.append(f"{file}: could not read file: {e}")
            continue

        # Skip the verbatim Markdown documentation embedded as R"DOCS_MD( ... )DOCS_MD"
        # raw-string literals in the format source files: it contains aligned Markdown
        # tables that legitimately have double spaces.
        embedded_doc = _embedded_doc_lines(lines)

        # Need previous and next line for alignment checks, so skip first/last
        for i in range(1, len(lines) - 1):
            line = lines[i]

            # Skip lines inside embedded documentation raw strings
            if i in embedded_doc:
                continue

            # Skip exception lines entirely
            if any(p.search(line) for p in EXCEPTIONS):
                continue

            m = DOUBLE_WS_AFTER_COMMA.search(line)
            if not m:
                continue

            # Column right before the end of the matched spaces (Perl $+[1] - 1)
            pos = m.end(1) - 1

            prev_slice = lines[i - 1][pos : pos + 2] if pos < len(lines[i - 1]) else ""
            next_slice = lines[i + 1][pos : pos + 2] if pos < len(lines[i + 1]) else ""

            # If either neighbor looks like alignment at that column, skip
            if ALIGN_RE.match(prev_slice) or ALIGN_RE.match(next_slice):
                continue

            # Skip numeric table-like lines
            if NUM_TABLE_RE.search(line):
                continue
            # Violation
            print(f"{file}:{i + 1}{line}")
            violations.append(f"{file}:{i + 1}{line}")

    return "\n".join(violations)


def check_yamllint(file_paths):
    file_paths = " ".join([f"'{file}'" for file in file_paths])
    exit_code, out, err = Shell.get_res_stdout_stderr(
        f"yamllint --config-file=./.yamllint {file_paths}", verbose=False
    )
    return out or err


def check_xmllint(file_paths):
    if not isinstance(file_paths, list):
        file_paths = [file_paths]
    file_paths = " ".join([f"'{file}'" for file in file_paths])
    exit_code, out, err = Shell.get_res_stdout_stderr(
        f"xmllint --noout --nonet {file_paths}", verbose=False
    )
    return out or err


def check_functional_test_cases(files):
    """
    Queries with event_date should have yesterday() not today()
    NOTE: it is not that accurate, but at least something.
    """

    patterns = [
        re.compile(
            r"(?i)where.*?\bevent_date\s*(=|>=)\s*today\(\)(?!\s*-\s*1)",
            re.IGNORECASE | re.DOTALL,
        )
    ]

    errors = []
    for test_case in files:
        try:
            with open(test_case, "r", encoding="utf-8", errors="replace") as f:
                file_content = " ".join(
                    f.read().splitlines()
                )  # Combine lines into a single string

            # Check if any pattern matches in the concatenated string
            if any(pattern.search(file_content) for pattern in patterns):
                errors.append(
                    f"event_date should be filtered using >=yesterday() in {test_case} (to avoid flakiness)"
                )

            if "0_stateless" in test_case:
                name = os.path.basename(test_case)
                has_streaming_queries_in_name = "_streaming_queries_" in name
                has_streaming_in_content = (
                    re.search(
                        r"enable_streaming_queries\s*=?\s*(0|1|true|false)\b",
                        file_content,
                    )
                    or "streaming.lib" in file_content
                )

                if has_streaming_in_content and not has_streaming_queries_in_name:
                    errors.append(
                        f"{test_case} uses enable_streaming_queries or streaming.lib but has no _streaming_queries_ in its name"
                    )

                if has_streaming_queries_in_name and not has_streaming_in_content:
                    errors.append(
                        f"{test_case} has _streaming_queries_ in its name but uses neither enable_streaming_queries nor streaming.lib"
                    )

        except Exception as e:
            errors.append(f"Error checking {test_case}: {e}")

    for test_case in files:
        if "fail" in test_case:
            errors.append(f"test case {test_case} includes 'fail' in its name")

    return " ".join(errors)


# A `SELECT` star projection can include optional `DISTINCT` / `ALL` and a table alias.
# It still materializes the server-side path column in the shell.
STAR_PROJECTION_RE = (
    r"\bselect\s+(?:(?:distinct|all)\s+)?"
    r"(?:[^\"`|;]*?,\s*)?(?:[A-Za-z_]\w*\.)?\*"
)

# A query that pulls a server-side filesystem path out of a system table into the shell.
# Single quotes are allowed inside the window so that a derived expression such as
# `concat(path, '/data.bin')` is still recognized; double quotes, backticks, pipes, and
# semicolons still bound the match to one query.
SYSTEM_PATH_TABLE_RE = (
    r"`?(?:parts|detached_parts|projection_parts|tables|disks|databases|detached_tables|"
    r"distribution_queue|remote_data_paths|filesystem_cache_settings)`?(?=[\s;])"
)
FETCHES_SERVER_PATH_RE = re.compile(
    r"(?i)(?:[`\"]?\b(?:path|data_path|data_paths|metadata_path|cache_paths|local_path)\b[`\"]?[^\"`|;]{0,300}?"
    rf"\bfrom\s+system\.{SYSTEM_PATH_TABLE_RE}"
    rf"|{STAR_PROJECTION_RE}[^\"`|;]{{0,300}}?\bfrom\s+system\.{SYSTEM_PATH_TABLE_RE})"
)
# The server data root fetched as `SELECT value` or a star projection from
# `system.server_settings` where `name = 'path'` or `name IN ('path')`. The `value` token (or `*`) is required
# so that queries that merely inspect the setting without materializing the path (e.g.
# `SELECT count()`) are not classified as a path fetch.
FETCHES_SERVER_ROOT_RE = re.compile(
    rf"(?i)(?:[`\"]?\bvalue\b[`\"]?|{STAR_PROJECTION_RE})[^\"`|;]{{0,100}}?\bfrom\s+system\.`?server_settings`?(?=[\s;])"
    r"[^\"`|;]{0,100}?(?:`?name`?\s*=\s*'path'|'path'\s*=\s*`?name`?|`?name`?\s+in\s*\([^)]*'path'[^)]*\))"
)
# Wrapper commands that can precede the actual mutation verb without changing what it does,
# their options and numeric arguments (`sudo -n rm ...`, `timeout 60 rm ...`), options that
# consume a following value (`sudo -u nobody rm ...`, `env -u HOME rm ...`,
# `timeout -s KILL 60 rm ...`), and leading variable assignments (`FOO=1 rm ...`). All of
# these must be skipped over, otherwise the mutation verb is not recognized and the check is
# trivially bypassed. An option's value may not start with `-`, and the regex engine
# backtracks when the value would swallow the mutation verb itself (`sudo -n rm ...`).
MUTATION_CMD_WRAPPER = (
    r"(?:sudo|command|builtin|exec|env|time|nice|ionice|nohup|stdbuf|timeout|xargs)"
)
MUTATION_CMD_WRAPPER_ARG = r"(?:-{1,2}[\w-]+(?:=[^\s;|&<>`]+|\s+[^-\s;|&<>`][^\s;|&<>`]*)?|[0-9]+(?:\.[0-9]+)?[smhd]?)"
MUTATION_CMD_ASSIGNMENT_VALUE = r"(?:'[^']*'|\"(?:\\.|[^\"\\])*\"|[^\s;|&<>`]+)"
MUTATION_CMD_PREFIX = (
    rf"(?:[A-Za-z_]\w*={MUTATION_CMD_ASSIGNMENT_VALUE}\s+"
    rf"|{MUTATION_CMD_WRAPPER}\s+(?:{MUTATION_CMD_WRAPPER_ARG}\s+)*)*"
)
FILE_MUTATION_VERBS = r"rm|cp|mv|dd|truncate|ln|chmod|touch|mkdir|tar|install|shred|tee"
# Shell commands that create, modify, or delete files, at the start of a line or after
# anything that can precede a command: a pipe / & / ; / subshell / group / backtick /
# `case` branch delimiter, or a shell keyword (`if true; then rm ...; fi` compressed onto
# one line must not slip through), optionally behind benign wrappers and assignments.
FILE_MUTATION_CMD_RE = re.compile(
    r"(?:^|[!|&;(){)`]|\b(?:if|then|elif|else|do|while|until)\b)\s*"
    + MUTATION_CMD_PREFIX
    + rf"(?:{FILE_MUTATION_VERBS})\s"
)
SED_IN_PLACE_RE = re.compile(
    r"\bsed\s+(?:(?:--?[\w-]+)(?:=[^\s]+)?\s+)*(?:-\w*i\w*|--in-place(?:=[^\s]+)?)(?:\s|$)"
)
# Redirection into a path built from a shell variable, except well-known test scratch areas.
REDIRECT_TO_VAR_RE = re.compile(
    r"(?<!-)>(?:>|\|)?\s*\"?\$\{?(?!CLICKHOUSE_TMP|CUR_DIR|CURDIR|USER_FILES_PATH"
    r"|CLICKHOUSE_USER_FILES|CLICKHOUSE_SCHEMA_FILES|CLICKHOUSE_LOG|\()"
)
# Redirection into a path built from command substitution. A `$(mktemp)` scratch path is
# fine, but any other substitution is suspicious once the file fetches a server path. This
# includes helpers that conceal the system-table query from the redirection line.
REDIRECT_TO_COMMAND_SUBSTITUTION_RE = re.compile(r"(?<!-)>(?:>|\|)?\s*\"?(?:\$\(|`)")
# Exempt only a redirect whose complete target is the default `mktemp` scratch path.
# Arguments can select a server-owned directory, so they must be treated as suspicious.
REDIRECT_TO_MKTEMP_RE = re.compile(
    r"(?<!-)>(?:>|\|)?\s*\"?\$\(\s*mktemp\s*\)\"?\s*(?:$|[;|&])"
)
# An `sh -c` / `bash -c` / `eval` payload is opaque: a quote in front of a mutation verb hides
# it from the command-position anchor of the patterns above.
SHELL_COMMAND_STRING_RE = re.compile(
    r"(?:^|[!|&;(){)`]|\b(?:if|then|elif|else|do|while|until)\b)\s*"
    r"(?P<executor>(?:sh|bash)\s+-c\b)|(?P<eval>\beval\s+)"
)
# ... except when the payload is provably a plain call of a shell function of the test itself:
# a literal command word that is not a mutation verb, followed only by plain arguments and
# simple variable expansions, with no way to reach a second command (no separator, redirection,
# or command substitution). Such a payload hides nothing - the function body lives in the same
# file, and every one of its lines is scanned by this check on its own - so it must not arm the
# check. This is the `bash -c insert_thread &` idiom that stress tests use to spawn background
# threads. Only the payload is exempted: the rest of the line is still matched by every pattern
# above, so a mutation or a redirection that follows it is still reported.
SHELL_COMMAND_STRING_PLAIN_CALL_RE = re.compile(
    rf"""(?x)
    (?:sh|bash)\s+-c\s+
    (?P<quote>["']?)
    (?!(?:{FILE_MUTATION_VERBS})\b)
    [A-Za-z_]\w*                                 # the function or command to call
    (?:\s+(?:\$\{{?\w+\}}?|[\w@%.,:=+/-]+))*     # plain arguments and simple variables
    (?P=quote)
    """
)
CLICKHOUSE_DISKS_WRITE_RE = re.compile(
    r"""(?x)
    \bclickhouse(?:-|\s+)disks\b
    [^|;&]*?
    (?:--query|(?<![\w-])-q)\s*(?:=\s*)?[\"']?\s*
    (?:write|w|remove|rm|delete|copy|cp|move|mv|mkdir|link|ln|touch|create|sed|packed-io|packed_io)\b
    """
)

# Do not add new entries: tests that modify the server's data on disk must be integration
# tests instead. The only acceptable additions are false positives - tests that only touch
# their own scratch files - and they must say so in a comment.
SERVER_DATA_MANIPULATION_EXCLUSIONS = {
    # False positive: writes only an mktemp scratch file under CLICKHOUSE_TMP.
    "04326_disks_app_read_checksums.sh",
}


def strip_shell_comment(line):
    """
    Cut a shell line at the first `#` that actually starts a comment: unquoted, not
    backslash-escaped, and at the start of a word. A naive `line.split("#")[0]` would also
    chop quoted payloads such as `echo '# broken' > "$path/data.bin"`, truncating the line
    before the redirection and bypassing the check.
    """
    quote = None
    i = 0
    while i < len(line):
        c = line[i]
        if quote == "'":
            if c == "'":
                quote = None
        elif quote == '"':
            if c == "\\":
                i += 1
            elif c == '"':
                quote = None
        elif c == "\\":
            i += 1
        elif c in "'\"":
            quote = c
        elif c == "#" and (i == 0 or line[i - 1] in " \t;|&(){}"):
            return line[:i]
        i += 1
    return line


SQL_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def strip_sql_comments(text):
    """
    Remove both comment forms of ClickHouse SQL from `text`: `/* ... */`, which may span lines, and
    `--` to the end of its line. Quoting is not tracked, so a `--` inside a string literal is cut
    as well; no check here reads such a literal.
    """
    text = SQL_BLOCK_COMMENT_RE.sub(" ", text)
    return "\n".join(line.split("--")[0] for line in text.splitlines())


# `clickhouse-local` is not the server: it runs against its own `--path`, so a filesystem
# path it reports from a system table points into the test's own scratch directory, and
# removing or rewriting a file there is not a manipulation of the server's data.
CLICKHOUSE_LOCAL_RE = re.compile(r"\$\{?CLICKHOUSE_LOCAL\}?|\bclickhouse(?:-|\s+)local\b")


def command_end(text, quote=None):
    """
    Where the command that `text` starts inside ends, as `(index, quote)`.

    `index` is the position of the first separator that is not inside a quoted string, or
    `None` when the command continues past the end of `text`; `quote` is then the quote
    character left open, so the scan can resume on the following line.
    """
    i = 0
    continued = False
    while i < len(text):
        c = text[i]
        if quote == "'":
            if c == "'":
                quote = None
        elif quote == '"':
            if c == "\\":
                i += 1
            elif c == '"':
                quote = None
        elif c == "\\":
            continued = i == len(text) - 1
            i += 1
        elif c in "'\"":
            quote = c
        elif c in ";|&)`":
            return i, None
        i += 1
    if quote is None and not continued:
        # A command ends with its line unless it is continued by a trailing backslash.
        return len(text), None
    return None, quote


def strip_clickhouse_local(code, pending=None):
    """
    Remove every `clickhouse-local` invocation from `code`, as `(remainder, pending)`.

    Only the invocation itself is removed, up to the end of its command - the text around it
    stays, so a server-side query that precedes or follows it on the same line is still seen.
    `pending` carries an invocation whose command continues on the following lines: `None`
    when there is none, the quote character left open by its query, or an empty string when
    it continues unquoted.
    """
    kept = []
    while True:
        if pending is not None:
            end, quote = command_end(code, pending or None)
            if end is None:
                return " ".join(kept), quote or ""
            pending = None
            code = code[end:]

        match = CLICKHOUSE_LOCAL_RE.search(code)
        if match is None:
            kept.append(code)
            return " ".join(kept), None

        kept.append(code[: match.start()])
        code = code[match.end() :]
        pending = ""


def executable_shell_content(lines):
    """
    Return the shell text that can execute a system-table query.

    `echo` and `printf` payloads are output, not commands, and quoted heredoc bodies are
    similarly inert. Omitting them avoids arming the file-level check on documentation or
    generated SQL text while retaining every executable command and unquoted heredoc.
    A `clickhouse-local` invocation is omitted as well: it queries its own `--path`, so the
    paths it reports are the test's own, not the server's.
    """
    result = []
    quoted_heredoc_end = None
    clickhouse_local_pending = None
    quoted_heredoc_re = re.compile(r"<<-?\s*(['\"])([A-Za-z_][A-Za-z0-9_]*)\1")
    output_command_re = re.compile(r"^\s*(?:echo|printf)\b")
    command_substitution_re = re.compile(r"`([^`]*)`|\$\(([^()]*)\)")

    for line in lines:
        if quoted_heredoc_end is not None:
            if line.strip() == quoted_heredoc_end:
                quoted_heredoc_end = None
            continue

        code = strip_shell_comment(line)
        heredoc_match = quoted_heredoc_re.search(code)
        if heredoc_match:
            quoted_heredoc_end = heredoc_match.group(2)

        # Drop every `clickhouse-local` invocation, including one whose query started on an
        # earlier line, but keep the text around it: a server-side query before or after the
        # invocation - even on the line where its multiline query closes - still counts.
        code, clickhouse_local_pending = strip_clickhouse_local(code, clickhouse_local_pending)

        if output_command_re.match(code):
            # An output command can still execute a query in command substitution, e.g.
            # `echo > `clickhouse-client -q "SELECT path ..."``. Keep that executable
            # fragment while discarding the ordinary payload text.
            # An empty substitution (``` in a `grep -qF` pattern, `$()`) matches with the
            # captured group empty or `None`; contribute an empty string, never `None`,
            # otherwise the final `join` raises.
            result.extend(
                match.group(1) or match.group(2) or ""
                for match in command_substitution_re.finditer(code)
            )
            continue
        result.append(code)

    return " ".join(result)


def has_opaque_shell_command_string(code):
    """
    Whether the line runs a shell command string whose payload could hide a file mutation.
    Every `sh -c` / `bash -c` / `eval` on the line counts, except a `-c` payload that just calls
    a shell function of the test itself - see `SHELL_COMMAND_STRING_PLAIN_CALL_RE`. Checking the
    occurrences one by one, rather than the line as a whole, keeps an exempt call from covering
    for a second, opaque executor on the same line.
    """
    for match in SHELL_COMMAND_STRING_RE.finditer(code):
        if match.group("eval"):
            return True
        if not SHELL_COMMAND_STRING_PLAIN_CALL_RE.match(code, match.start("executor")):
            return True
    return False


def check_no_server_data_manipulation(files):
    """
    Stateless tests must not modify the server's on-disk data: part directories, detached
    parts, table metadata, or anything else under the server path. The stateless suite runs
    against arbitrary server configurations - object storage, shared MergeTree, encrypted
    disks - where the local part layout either does not exist or does not mean what the
    test assumes, and modifying it corrupts shared state (on a remote disk, a plain `cp` of
    a part directory duplicates metadata files without incrementing blob reference counts,
    and the later removal deletes blobs still referenced by the live part - see
    https://github.com/ClickHouse/ClickHouse/pull/113978). Such scenarios belong in
    integration tests, where the environment is fully controlled.

    Heuristic: the test fetches a server-side filesystem path from a system table and also
    runs file-modifying shell commands.
    """

    errors = []
    for test_case in files:
        if "0_stateless" not in test_case or not test_case.endswith(".sh"):
            continue
        if os.path.basename(test_case) in SERVER_DATA_MANIPULATION_EXCLUSIONS:
            continue
        try:
            with open(test_case, "r", encoding="utf-8", errors="replace") as f:
                file_content = f.read()
        except Exception as e:
            errors.append(f"Error checking {test_case}: {e}")
            continue

        # Use the same comment handling as the mutation scan below. Otherwise a commented
        # query could arm the file-level check but could not itself produce a violation.
        joined_content = executable_shell_content(file_content.splitlines())
        if not FETCHES_SERVER_PATH_RE.search(
            joined_content
        ) and not FETCHES_SERVER_ROOT_RE.search(joined_content):
            continue

        for line_number, line in enumerate(file_content.splitlines(), 1):
            code = strip_shell_comment(line)
            if (
                FILE_MUTATION_CMD_RE.search(code)
                or SED_IN_PLACE_RE.search(code)
                or REDIRECT_TO_VAR_RE.search(code)
                or (
                    REDIRECT_TO_COMMAND_SUBSTITUTION_RE.search(code)
                    and not REDIRECT_TO_MKTEMP_RE.search(code)
                )
                or has_opaque_shell_command_string(code)
                or CLICKHOUSE_DISKS_WRITE_RE.search(code)
            ):
                errors.append(
                    f"{test_case}:{line_number} fetches a server-side filesystem path from a system table "
                    f"and runs file-modifying commands: `{code.strip()}`. Modifying the server's data "
                    f"on disk is not allowed in stateless tests (they run against arbitrary server "
                    f"configurations: object storage, shared MergeTree, encrypted disks). "
                    f"Write an integration test instead."
                )
                break

    return "\n".join(errors)


FAILPOINT_STATEMENT_RE = re.compile(
    r"\bSYSTEM\s+(?:(?P<disable_all>DISABLE\s+ALL\s+FAILPOINTS)"
    r"|(?P<action>ENABLE|DISABLE)\s+FAILPOINT\s+(?P<name>[^\s;'\")]+))",
    re.IGNORECASE,
)

# A fail point name spelled through a shell variable cannot be resolved here, so a disable of one
# stands for a disable of any name - see `check_failpoints_are_disabled`.
FAILPOINT_LITERAL_NAME_RE = re.compile(r"^\w+$")

# Only a test that talks to the server can leave a fail point armed for the next one. A
# `clickhouse-local` invocation keeps its fail points in its own process, which exits with the test.
SERVER_CLIENT_RE = re.compile(
    r"\$\{?CLICKHOUSE_(?:CLIENT|CURL|BENCHMARK)|\bclickhouse(?:-|\s+)(?:client|benchmark)\b"
)

# Where the statement that surrounds a `SYSTEM ... FAILPOINT` match begins. Only the text of that
# statement is inspected, so a keyword from a neighbouring one cannot leak into the decision.
FAILPOINT_STATEMENT_SEPARATORS = ";\"'`"

# `SYSTEM DISABLE ALL FAILPOINTS` is server-wide in the other direction as well: it disarms the
# fail points a concurrently running test armed, so it stands in for the per-name disables only in
# a test that runs alone. `SYSTEM DROP` carries the same requirement, in `various_checks.sh`.
NO_PARALLEL_TAG_RE = re.compile(r"(--|#)\s*[Tt]ags:.*\bno-parallel\b")


def failpoint_statements(text):
    """
    Yield `(action, name)` for every `SYSTEM ENABLE|DISABLE FAILPOINT` statement in `text`, and
    `("disable_all", None)` for every `SYSTEM DISABLE ALL FAILPOINTS`, which takes no name.

    `EXPLAIN SYSTEM ENABLE FAILPOINT ...` is skipped: it prints the parsed statement and arms
    nothing, so it neither needs a disable nor stands in for one.
    """
    for match in FAILPOINT_STATEMENT_RE.finditer(text):
        statement_start = max(
            text.rfind(separator, 0, match.start())
            for separator in FAILPOINT_STATEMENT_SEPARATORS
        )
        if "explain" in text[statement_start + 1 : match.start()].lower():
            continue
        if match.group("disable_all"):
            yield "disable_all", None
        else:
            yield match.group("action").lower(), match.group("name")


def check_failpoints_are_disabled(files):
    """
    A test that arms a fail point must disarm it: every `SYSTEM ENABLE FAILPOINT <name>` needs a
    `SYSTEM DISABLE FAILPOINT <name>` in the same test.

    Fail point state lives in the server process, not in the connection or the session, so one that
    a test leaves armed goes on firing in whatever runs next. The stateless suite runs tests in
    parallel, so that is another test's query: it fails, or hangs on a pauseable fail point, for a
    reason that is nowhere in its own source, and the report blames the wrong change. Disarming on
    the way out keeps the damage inside the test that asked for it.

    The disable belongs on every path out of the test, not only the successful one - in a `.sh` test
    that means a `trap ... EXIT`, since an early `exit` or a failing command under `set -e` skips
    the rest of the file.

    `SYSTEM DISABLE ALL FAILPOINTS` disarms everything the test armed, so it stands in for the
    per-name disables - but only in a `no-parallel` test, because it disarms what the tests running
    alongside armed as well.
    """

    errors = []
    for test_case in files:
        if "0_stateless" not in test_case:
            continue
        try:
            with open(test_case, "r", encoding="utf-8", errors="replace") as f:
                file_content = f.read()
        except Exception as e:
            errors.append(f"Error checking {test_case}: {e}")
            continue

        if "FAILPOINT" not in file_content.upper():
            continue

        if test_case.endswith(".sh"):
            # Drop comments, `echo` payloads and `clickhouse-local` invocations, so only the
            # statements that reach the server are left.
            content = executable_shell_content(file_content.splitlines())
            if not SERVER_CLIENT_RE.search(content):
                continue
        elif test_case.endswith(".py"):
            content = "\n".join(
                strip_shell_comment(line) for line in file_content.splitlines()
            )
        else:
            content = strip_sql_comments(file_content)

        enabled = []
        disabled = set()
        disables_all = False
        for action, name in failpoint_statements(content):
            if action == "enable":
                enabled.append(name)
            elif action == "disable_all":
                disables_all = True
            else:
                disabled.add(name)

        if not enabled:
            continue

        if disables_all and NO_PARALLEL_TAG_RE.search(file_content):
            continue

        # A name that comes from a shell variable cannot be resolved by a text check, on either
        # side: a disable of an unresolvable name therefore stands for a disable of any name, and
        # an unresolvable enable is satisfied by any disable in the file. The test that needs this
        # - a helper function disabling the fail point its caller armed - is already correct.
        disables_unresolvable_name = any(
            not FAILPOINT_LITERAL_NAME_RE.match(name) for name in disabled
        )

        for name in dict.fromkeys(enabled):
            if name in disabled or disables_unresolvable_name:
                continue
            if disabled and not FAILPOINT_LITERAL_NAME_RE.match(name):
                continue
            line_number = next(
                (
                    number
                    for number, line in enumerate(file_content.splitlines(), 1)
                    if re.search(
                        r"ENABLE\s+FAILPOINT\s+" + re.escape(name), line, re.IGNORECASE
                    )
                ),
                1,
            )
            if disables_all:
                errors.append(
                    f"{test_case}:{line_number} enables the fail point `{name}` and clears it only "
                    f"with `SYSTEM DISABLE ALL FAILPOINTS`, in a test that has no `no-parallel` "
                    f"tag. That statement also disarms the fail points the tests running alongside "
                    f"armed. Either disable `{name}` by name, or tag the test `no-parallel`."
                )
                continue
            errors.append(
                f"{test_case}:{line_number} enables the fail point `{name}` and never disables it. "
                f"Add `SYSTEM DISABLE FAILPOINT {name}` on every path out of the test (in a `.sh` "
                f"test, from a `trap ... EXIT`). A fail point is server-global state: one left "
                f"armed fires in a concurrently running test and fails it for a reason that is not "
                f"in its own source."
            )

    return "\n".join(errors)


def check_gaps_in_tests_numbers(file_paths, gap_threshold=100):
    test_numbers = set()

    pattern = re.compile(r"(\d+)")

    for file in file_paths:
        file_name = os.path.basename(file)
        match = pattern.search(file_name)
        if match:
            test_numbers.add(int(match.group(1)))

    sorted_numbers = sorted(test_numbers)
    large_gaps = []
    for i in range(1, len(sorted_numbers)):
        prev_num = sorted_numbers[i - 1]
        next_num = sorted_numbers[i]
        diff = next_num - prev_num
        if diff >= gap_threshold:
            large_gaps.append(f"Gap ({prev_num}, {next_num}) > {gap_threshold}")

    return large_gaps


def check_broken_links(path, exclude_paths):
    broken_symlinks = []

    for path in Path(path).rglob("*"):
        if any(exclude_path in str(path) for exclude_path in exclude_paths):
            continue
        if path.is_symlink():
            if not path.exists():
                broken_symlinks.append(str(path))

    if broken_symlinks:
        for symlink in broken_symlinks:
            print(symlink)
        return f"Broken symlinks found: {broken_symlinks}"
    else:
        return ""


def check_cpp_code():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_cpp.sh"
    )
    if err:
        out += err
    return out


def check_other():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/various_checks.sh"
    )
    if err:
        out += err
    return out


def check_embedded_doc_snippets():
    # A shared `docs/snippets/*.mdx` is hand-embedded into the built-in help surfaces
    # (TerminalMarkdownRenderer.cpp and docs.html); fail if those copies drift from the source.
    res, out, err = Shell.get_res_stdout_stderr(
        "python3 ./ci/jobs/scripts/check_style/check_embedded_doc_snippets.py"
    )
    if err:
        out += err
    return out


def check_mypy():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check-mypy"
    )
    if err:
        out += err
    return out


def check_pylint():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check-pylint"
    )
    if err:
        out += err
    return out


def check_system_table_documentation_pages():
    # The system-table reference pages are generated from the structured `COMMENT` of each table.
    # Generating them needs a `clickhouse` binary, which this job does not have, but the extraction
    # from the C++ sources and the rewriting of a page are pure Python, and a page which was not
    # regenerated after its source-owned comment changed is detected from the sources alone.
    res, out, err = Shell.get_res_stdout_stderr(
        "python3 ./ci/jobs/scripts/docs/autogenerate/test_system_table_pages.py"
    )
    if res == 0:
        return ""
    if err:
        out += err
    return out


def check_ruff():
    # Configuration lives under [tool.ruff] in pyproject.toml.
    # --quiet suppresses the "All checks passed!" success message so the result
    # framework (which treats a truthy return value as failure) sees an empty
    # string on success.
    res, out, err = Shell.get_res_stdout_stderr(
        "ruff check --output-format=concise --quiet"
    )
    if err:
        out += err
    return out


def _find_enclosing_function_lines(lines, catch_line_idx):
    """Return signature lines of the function enclosing the catch at *catch_line_idx*.

    Walks backwards from the catch, tracking brace depth.  When depth goes
    negative we have reached an enclosing scope's opening brace.  If that
    scope is a control-flow block (``if``/``else``/``for``/``while``/``try``
    /``switch``/``do``/``catch``) we reset and keep looking for the actual
    function scope.  Returns a list of up to 6 source lines around the
    opening brace (the signature area), or an empty list if nothing is found.
    """
    control_flow_re = re.compile(
        r"^\s*(if\b|else\b|for\b|while\b|try\b|switch\b|do\b|catch\b)"
    )
    depth = 0
    for i in range(catch_line_idx - 1, -1, -1):
        line = lines[i]
        stripped = line.strip()
        if stripped.startswith("//"):
            continue
        depth += line.count("}") - line.count("{")
        if depth < 0:
            # Crossed into an enclosing scope.  Determine its kind.
            is_control_flow = control_flow_re.match(stripped) is not None
            if not is_control_flow:
                for j in range(i - 1, max(i - 3, -1), -1):
                    prev = lines[j].strip()
                    if not prev or prev.startswith("//"):
                        continue
                    if control_flow_re.match(prev):
                        is_control_flow = True
                    break

            if is_control_flow:
                # Skip this control-flow scope and keep looking outward.
                depth = 0
                continue

            # Looks like a function (or class/namespace) scope.
            sig = []
            for j in range(i, max(i - 6, -1), -1):
                if j < i and (lines[j].strip() == "" or "}" in lines[j]):
                    break
                sig.append(lines[j])

            # If the signature has no parentheses, it is likely a
            # namespace/class scope rather than a function.  In that case
            # fall through to the function-try-block scan below.
            if any("(" in l for l in sig):
                return sig
            break

    # Handle function-try blocks: "Type func(...) try { ... } catch (...) { ... }"
    # In this pattern there is no separate function opening brace, so the loop
    # above never reaches depth < 0 within the function, or it reaches
    # a namespace/class scope.  Re-scan for a bare ``try`` at depth 0.
    depth = 0
    for i in range(catch_line_idx - 1, -1, -1):
        line = lines[i]
        stripped = line.strip()
        if stripped.startswith("//"):
            continue
        depth += line.count("}") - line.count("{")
        if depth < 0:
            break
        if depth == 0 and re.match(r"^\s*try\b", stripped):
            sig = []
            for j in range(i - 1, max(i - 7, -1), -1):
                s = lines[j].strip()
                if not s or s.startswith("//"):
                    continue
                if "}" in lines[j]:
                    break
                sig.append(lines[j])
            return sig
    return []


def _is_in_destructor(lines, catch_line_idx):
    """Check if the catch at the given line index is inside a destructor."""
    sig = _find_enclosing_function_lines(lines, catch_line_idx)
    return any(re.search(r"~\w+", l) for l in sig)


def _is_in_main_or_fuzzer(lines, catch_line_idx):
    """Check if the catch is inside ``main`` or ``LLVMFuzzerTestOneInput``."""
    sig = _find_enclosing_function_lines(lines, catch_line_idx)
    return any(re.search(r"\b(main|LLVMFuzzerTestOneInput)\b", l) for l in sig)


def _get_catch_block_lines(lines, catch_line_idx):
    """Return lines from the catch statement through the closing brace."""
    result = []
    depth = 0
    started = False
    for i in range(catch_line_idx, len(lines)):
        line = lines[i]
        result.append(line)
        for ch in line:
            if ch == "{":
                started = True
                depth += 1
            elif ch == "}":
                depth -= 1
                if started and depth == 0:
                    return result
    return result


def check_catch_all(files) -> str:
    """Find ``catch (...)`` blocks that silently swallow exceptions.

    Flags catch-all blocks that do none of the following:
    * rethrow (``throw;``),
    * throw a different exception (``throw ...``),
    * log the error (``tryLogCurrentException``, ``LOG_*``, ``std::cerr``),
    * terminate (``std::terminate``, ``abort``, ``exit``),
    * save the exception (``current_exception``),
    * have a comment containing the word 'Ok'.

    Also skips blocks inside destructors, ``main``/``LLVMFuzzerTestOneInput``,
    and poco.
    """
    violations = []
    catch_pattern = re.compile(r"\bcatch\s*\(\s*\.\.\.\s*\)")
    ok_pattern = re.compile(r"(//|/\*).*\bok\b", re.IGNORECASE)

    # Patterns that indicate the exception is handled somehow
    handled_patterns = [
        re.compile(r"\bthrow\b"),
        re.compile(r"\btryLogCurrentException\b"),
        re.compile(r"\bLOG_(ERROR|WARNING|FATAL)\b"),
        re.compile(r"\bgetLogger\b"),
        re.compile(r"\bstd::cerr\b"),
        re.compile(r"\bstd::terminate\b"),
        re.compile(r"\babort\s*\("),
        re.compile(r"\bexit\s*\("),
        re.compile(r"\bcurrent_exception\b"),
        re.compile(r"\bgetCurrentExceptionMessage\b"),
        re.compile(r"\bgetCurrentExceptionCode\b"),
        re.compile(r"\bgetCurrentExceptionMessageAndPattern\b"),
        re.compile(r"\bExecutionStatus::fromCurrentException\b"),
        re.compile(r"\bonBackgroundException\b"),
        re.compile(r"\bstoreException\b"),
        re.compile(r"\bSTDERR_FILENO\b"),
        re.compile(r"\bwriteRetry\b"),
        re.compile(r"\bhandle_exception\b"),
        re.compile(r"\bhandleException\b"),
        re.compile(r"\bfinishWithException\b"),
    ]

    for file_path in files:
        if "/poco/" in file_path:
            continue

        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except OSError:
            continue

        for i, line in enumerate(lines):
            catch_match = catch_pattern.search(line)
            if not catch_match:
                continue

            # Skip if the catch is inside a single-line comment
            comment_pos = line.find("//")
            if comment_pos >= 0 and comment_pos < catch_match.start():
                continue

            block_lines = _get_catch_block_lines(lines, i)
            body = "".join(block_lines)

            if any(p.search(body) for p in handled_patterns):
                continue

            if _is_in_destructor(lines, i):
                continue

            if _is_in_main_or_fuzzer(lines, i):
                continue

            # Check for an 'Ok' comment in the block and a few lines before
            context_start = max(0, i - 2)
            all_lines = lines[context_start:i] + block_lines
            if any(ok_pattern.search(cl) for cl in all_lines):
                continue

            violations.append(
                f"{file_path}:{i + 1}: "
                "catch (...) that silently swallows exceptions. "
                "Either handle the exception (log, rethrow, save) or add a comment containing 'Ok' to suppress this warning."
            )

    return "\n".join(violations)


def check_file_names(files):
    files_set = set()
    for file in files:
        file_ = file.lower()
        if file_ in files_set:
            return f"Non-uniq file name in lower case: {file}"
        files_set.add(file_)
    return ""


def check_compose_images(files) -> str:
    """Ensure every image referenced in docker compose files is served from Docker Hub.

    CI runners pull Docker Hub images through the dockerhub-proxy cache (see
    tests/ci/terraform/dockerhub-proxy.md). Images hosted on other registries
    (ghcr.io, mcr.microsoft.com, quay.io, ...) bypass that proxy and are pulled
    directly, exposing CI to those registries' anonymous rate limits. Mirror such
    images into the clickhouse/ Docker Hub namespace (see
    tests/integration/compose/mirror-images.sh) and reference the mirror instead.
    """
    image_re = re.compile(r"^\s*image:\s*(.+?)\s*$")
    # ${VAR:-default} -> default; used to resolve compose variable interpolation.
    var_default_re = re.compile(r"\$\{[^}:]+:-([^}]*)\}")
    hub_aliases = {"docker.io", "registry-1.docker.io", "index.docker.io"}

    violations = []
    for file in files:
        try:
            with open(file, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except OSError as e:
            violations.append(f"{file}: could not read file: {e}")
            continue

        for i, line in enumerate(lines):
            m = image_re.match(line)
            if not m:
                continue

            # Strip trailing inline comment and surrounding quotes.
            value = re.sub(r"\s+#.*$", "", m.group(1)).strip().strip("'\"")
            # Resolve ${VAR:-default} interpolations to their default value.
            ref = var_default_re.sub(r"\1", value)
            # A bare ${VAR} without default leaves the registry undeterminable; skip.
            if "${" in ref:
                continue

            # Docker's rule: the first path component is a registry host only when
            # it contains a '.' or ':' or equals 'localhost'. Otherwise it is a
            # Docker Hub namespace/official image.
            first = ref.split("/", 1)[0] if "/" in ref else ""
            is_registry_host = first and (
                "." in first or ":" in first or first == "localhost"
            )
            if is_registry_host and first not in hub_aliases:
                violations.append(
                    f"{file}:{i + 1}: image '{ref}' is not from Docker Hub "
                    f"(registry '{first}'). Mirror it into the clickhouse/ namespace "
                    f"via tests/integration/compose/mirror-images.sh and reference the mirror."
                )

    return "\n".join(violations)


# Release notes are a record of what was published and are not edited afterwards. Matches the
# versioned files inside any `changelogs/` or `private-changelogs/` directory
# (`docs/changelogs/v25.9.2.1-stable.md`, `docs/resources/changelogs/cloud/release-notes/25_10.mdx`)
# and leaves the landing and status pages that live next to them enforced.
CHANGELOG_RECORD_RE = re.compile(r"(^|/)(?:private-)?changelogs/(.+/)?v?[0-9][^/]*$")

CLICKHOUSE_SPELLING_IGNORE_FILE = (
    "ci/jobs/scripts/check_style/clickhouse_spelling_ignore.txt"
)


def _clickhouse_spelling_ignore_list():
    """Parse the exception list. A line with a path alone exempts the whole file; a path followed
    by a literal exempts only the lines of that file containing that literal. Returns
    {path: set of literals}, where `None` in the set means the whole file."""
    ignore_list = {}
    with open(CLICKHOUSE_SPELLING_IGNORE_FILE, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            parts = line.rstrip("\n").split(maxsplit=1)
            path = parts[0]
            literal = parts[1].strip() if len(parts) > 1 else None
            ignore_list.setdefault(path, set()).add(literal)
    return ignore_list


def _clickhouse_spelling_generated_locale_prefixes():
    """Path prefixes of the generated documentation translations. The English pages are the
    source, so a misspelling in a locale directory is fixed by fixing the English page and
    letting the translation workflow regenerate it (see docs/README.md)."""
    config = json.loads(Path("docs/gt.config.json").read_text(encoding="utf-8"))
    prefixes = []
    for locale in config["locales"]:
        if locale == config["defaultLocale"]:
            continue
        prefixes.append(f"docs/{locale}/")
        prefixes.append(f"docs/snippets/{locale}/")
    return prefixes


def _autogenerated_line_numbers(file_path):
    """1-based numbers of the lines between `AUTOGENERATED_START` and `AUTOGENERATED_END`
    markers, inclusive."""
    generated = set()
    inside = False
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f, 1):
            if "AUTOGENERATED_START" in line:
                inside = True
            if inside:
                generated.add(i)
            if "AUTOGENERATED_END" in line:
                inside = False
    return generated


def check_clickhouse_spelling():
    """The product name is written `ClickHouse` - one word, capital `C`, capital `H`. The
    checker also accepts the conventional token spellings `clickhouse` and `CLICKHOUSE`;
    `Clickhouse`, `clickHouse`,
    `click_house`, `CLICK_HOUSE`, `click-house` and `Click House` are misspellings.

    Every file tracked by git is checked, and so are the file names themselves, except:
    * `contrib` - third-party sources;
    * the generated locale directories of the documentation - a machine translation of the
      English pages, which are checked instead;
    * release notes - a record of what was published;
    * `docs/_specs` - API specifications pinned from their generator;
    * regions between `AUTOGENERATED_START` and `AUTOGENERATED_END` markers - their source of
      truth (a `FunctionDocumentation` string, for instance) is checked instead;
    * URLs, and the exceptions listed in `clickhouse_spelling_ignore.txt`.
    """
    ignore_list = _clickhouse_spelling_ignore_list()
    excluded_prefixes = _clickhouse_spelling_generated_locale_prefixes() + [
        # Pinned copies of API specifications, refreshed from their generator by
        # ci/jobs/cloud_api_docs_nightly.py.
        "docs/_specs/",
        # This check also runs on the private repository, on the synced tree, where this
        # directory holds nothing but release records, some of them named by release codename
        # without a version prefix, so CHANGELOG_RECORD_RE does not cover them.
        "docs/private-changelogs/",
    ]

    def is_excluded(path):
        return any(path.startswith(p) for p in excluded_prefixes) or bool(
            CHANGELOG_RECORD_RE.search(path)
        )

    def is_exempt(path, text):
        literals = ignore_list.get(path)
        if literals is None:
            return False
        return None in literals or any(l in text for l in literals if l is not None)

    # A superset of what is reported below - the same spellings, minus the correct ones - used
    # only to find the candidate lines quickly, without reading every file in Python.
    prefilter = "(?!{})(?:{})".format(
        "|".join(CLICKHOUSE_CORRECT_SPELLINGS), CLICKHOUSE_ANY_SPELLING
    )
    # `-I` skips the binary files, `-z` separates the file name and the line number with a NUL
    # so that a path holding a colon cannot be mistaken for one.
    exit_code, out, err = Shell.get_res_stdout_stderr(
        f"git grep -I -n -z -P {shlex.quote(prefilter)} -- . ':(exclude)contrib'",
        verbose=False,
    )
    # 0 - matches found, 1 - none found, anything else is a failure of the search itself.
    if exit_code > 1:
        return f"Failed to search for misspellings of ClickHouse: {err}"

    violations = []
    generated_lines = {}
    for record in out.splitlines():
        path, line_number, line = record.split("\0", 2)
        if is_excluded(path) or is_exempt(path, line):
            continue
        misspellings = clickhouse_misspellings(line)
        if not misspellings:
            continue
        if path not in generated_lines:
            generated_lines[path] = _autogenerated_line_numbers(path)
        if int(line_number) in generated_lines[path]:
            continue
        for misspelling in sorted(set(misspellings)):
            violations.append(f"{path}:{line_number}: {misspelling}")

    # `-z` keeps the paths verbatim; without it git quotes the unusual ones.
    exit_code, out, err = Shell.get_res_stdout_stderr(
        "git ls-files -z -- . ':(exclude)contrib'", verbose=False
    )
    if exit_code != 0:
        return f"Failed to list the files of the repository: {err}"
    for path in out.split("\0"):
        if not path:
            continue
        if is_excluded(path) or is_exempt(path, path):
            continue
        for misspelling in sorted(set(clickhouse_misspellings(path))):
            violations.append(f"{path}: in the file name: {misspelling}")

    if violations:
        return (
            "The product name is spelled `ClickHouse`; `clickhouse` and `CLICKHOUSE` are the "
            "only accepted case variants. Fix the spellings below, or, if the text is not ours "
            f"to spell, add an exception to {CLICKHOUSE_SPELLING_IGNORE_FILE}:\n"
            + "\n".join(violations)
        )
    return ""


def check_settings_changes_history():
    """The settings-history rules of ci/jobs/scripts/settings_history.py, applied to the PR / merge-queue diff of
    the settings declaration files as parsed by the store_data.py workflow hook. The same check runs locally with
    `python3 -m ci.jobs.scripts.settings_history`. Returns "" on success or a non-empty error string on failure
    (consumed by Result.from_commands_run).

    Fail-close: if a declaration file changed but the hook could not fetch the diff (e.g. in the merge queue),
    fail rather than silently pass - a green here would defeat the purpose."""
    kv = Info().get_kv_data() or {}
    changed_files = kv.get("changed_files")

    if changed_files is None:
        # changed_files is stored fail-close by the store_data.py hook for every PR and
        # merge-queue run; its absence means the check cannot know whether the files changed.
        return (
            "Could not determine changed files (no 'changed_files' recorded by the "
            "store_data.py workflow hook); refusing to pass the settings-history check."
        )
    if not any(path in changed_files for path in SETTINGS_DECLARATION_FILES):
        return ""

    fetch_error = kv.get("settings_history_fetch_error")
    changes = kv.get("settings_declaration_changes")
    if fetch_error or changes is None:
        return (
            f"The settings declaration files changed but their diff could not be fetched to "
            f"validate the settings history (the check must not be skipped when they changed). "
            f"Error: {fetch_error or 'no data recorded by the store_data.py workflow hook'}."
        )

    version = settings_current_version()
    found = settings_history_violations(changes, version)
    return settings_violation_message(found, version) if found else ""


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Style Check Job")
    parser.add_argument("--test", help="Sub check name", default="")
    return parser.parse_args()


if __name__ == "__main__":
    results = []
    args = parse_args()
    testpattern = args.test

    cpp_files = Utils.traverse_paths(
        include_paths=["./src", "./base", "./programs", "./utils"],
        exclude_paths=[
            "./base/glibc-compatibility",
            "./contrib/consistent-hashing",
            "./base/widechar_width",
        ],
        file_suffixes=[".h", ".cpp"],
    )

    yaml_workflow_files = Utils.traverse_paths(
        include_paths=["./.github"],
        exclude_paths=[],
        file_suffixes=[".yaml", ".yml"],
    )

    xml_files = Utils.traverse_paths(
        include_paths=["./tests", "./programs/"],
        exclude_paths=[],
        file_suffixes=[".xml"],
    )

    functional_test_files = Utils.traverse_paths(
        include_paths=["./tests/queries"],
        exclude_paths=[],
        file_suffixes=[".sql", ".sh", ".py", ".j2"],
    )

    compose_files = Utils.traverse_paths(
        include_paths=["./tests/integration/compose"],
        exclude_paths=[],
        file_suffixes=[".yml", ".yaml"],
    )

    testname = "whitespace_check"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_whitespaces,
                files=cpp_files,
            )
        )
    testname = "yamllint"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_yamllint,
                files=yaml_workflow_files,
            )
        )
    testname = "xmllint"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_xmllint,
                files=xml_files,
            )
        )
    testname = "functional_tests_check"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_functional_test_cases,
                files=functional_test_files,
            )
        )
    testname = "server_data_manipulation_in_stateless_tests"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_no_server_data_manipulation,
                files=functional_test_files,
            )
        )
    testname = "failpoints_disabled_in_tests"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_failpoints_are_disabled,
                files=functional_test_files,
            )
        )
    testname = "test_numbers_check"
    # Skip on release branches and backport PRs: backports cherry-pick a small
    # subset of test files, which legitimately leaves large gaps in the numbering.
    info = Info()
    release_branch_re = re.compile(r"^\d{2}\.\d+$")
    branch_to_check = (info.base_branch or info.git_branch or "").removeprefix(
        "release/"
    )
    is_release_branch = bool(release_branch_re.match(branch_to_check))
    if testpattern.lower() in testname.lower() and not is_release_branch:
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_gaps_in_tests_numbers,
                command_args=[functional_test_files],
            )
        )
    testname = "symlinks"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_broken_links,
                command_kwargs={
                    "path": "./",
                    "exclude_paths": ["contrib/", "metadata/", "programs/server/data"],
                },
            )
        )
    testname = "catch_all"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_catch_all,
                files=cpp_files,
            )
        )
    testname = "compose_images_from_dockerhub"
    if testpattern.lower() in testname.lower():
        results.append(
            run_check_concurrent(
                check_name=testname,
                check_function=check_compose_images,
                files=compose_files,
            )
        )
    testname = "clickhouse_spelling"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_clickhouse_spelling,
            )
        )
    # Disabled while the migration of the history into the setting declarations lands: GitHub
    # omits the patch of its huge Settings.cpp diff, so the check can only fail closed here.
    # Re-enable in a follow-up.
    # testname = "settings_changes_history"
    # if testpattern.lower() in testname.lower():
    #     results.append(
    #         Result.from_commands_run(
    #             name=testname,
    #             command=check_settings_changes_history,
    #         )
    #     )
    testname = "cpp"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_cpp_code,
            )
        )
    testname = "various"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_other,
            )
        )
    testname = "embedded_doc_snippets"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_embedded_doc_snippets,
            )
        )
    testname = "system_table_documentation_pages"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_system_table_documentation_pages,
            )
        )
    testname = "ruff"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_ruff,
            )
        )

    # testname = "mypy"
    # if testpattern.lower() in testname.lower():
    #     results.append(
    #         Result.from_commands_run(
    #             name=testname,
    #             command=check_mypy,
    #         )
    #     )
    Result.create_from(results=results).complete_job()
