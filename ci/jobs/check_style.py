import argparse
import math
import multiprocessing
import os
import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from praktika.info import Info
from praktika.result import Result
from praktika.utils import Shell, Utils

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
                if ')DOCS_MD"' not in line[idx + len('R"DOCS_MD('):]:
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
                has_streaming_in_content = re.search(r"enable_streaming_queries\s*=?\s*(0|1|true|false)\b", file_content) or "streaming.lib" in file_content

                if has_streaming_in_content and not has_streaming_queries_in_name:
                    errors.append(f"{test_case} uses enable_streaming_queries or streaming.lib but has no _streaming_queries_ in its name")

                if has_streaming_queries_in_name and not has_streaming_in_content:
                    errors.append(f"{test_case} has _streaming_queries_ in its name but uses neither enable_streaming_queries nor streaming.lib")

        except Exception as e:
            errors.append(f"Error checking {test_case}: {e}")

    for test_case in files:
        if "fail" in test_case:
            errors.append(f"test case {test_case} includes 'fail' in its name")

    return " ".join(errors)


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
    return any(
        re.search(r"\b(main|LLVMFuzzerTestOneInput)\b", l) for l in sig
    )


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


def check_settings_changes_history():
    """Every setting added or value-changed in src/Core/SettingsChangesHistory.cpp by this
    change must be recorded under the CURRENT version block (in addition to any older block
    used for backports), so the settings history stays consistent with the release version
    (together with the 03999_stateless_settings_history functional test, which checks that
    the recorded value matches the final Settings state).

    Runs only when that file changed; the list of changed setting names is provided by the
    store_data.py workflow hook (which parses the PR / merge-queue diff). Returns "" on
    success or a non-empty error string on failure (consumed by Result.from_commands_run).
    Pure text parsing - no C++ syntax analysis.

    A change that touches no C++ source at all besides SettingsChangesHistory.cpp cannot have
    changed any setting's compiled default, so it is a historical correction - fixing what a past
    release recorded - not a default change made now, and it is allowed (the check skips).
    Requiring it under the current version block would tell `compatibility` the value changed
    again in this release. The gate deliberately keys off any src/ source file rather than the
    declaration files alone: a default can come from a constant defined elsewhere (for example
    `DEFAULT_INSERT_BLOCK_SIZE` or `DBMS_DEFAULT_LOCK_ACQUIRE_TIMEOUT_SEC` in src/Core/Defines.h),
    and a narrower gate would let such a change be recorded in an older block unnoticed. Build
    definitions are treated the same way: defaults also switch on compile definitions such as
    `CLICKHOUSE_CLOUD` and `ENABLE_DISTRIBUTED_CACHE`, which come from CMake files and config
    templates, so changes to CMakeLists.txt / *.cmake / *.h.in also enforce the rule.

    Fail-close: if the file changed but the hook could not fetch the diff (e.g. in the merge
    queue), fail rather than silently pass - a green here would defeat the purpose."""
    path = "src/Core/SettingsChangesHistory.cpp"
    kv = Info().get_kv_data() or {}
    changed_files = kv.get("changed_files")

    if changed_files is None:
        # changed_files is stored fail-close by the store_data.py hook for every PR and
        # merge-queue run; its absence means the check cannot know whether the file changed.
        return (
            "Could not determine changed files (no 'changed_files' recorded by the "
            "store_data.py workflow hook); refusing to pass the settings-history check."
        )
    if path not in changed_files:
        # The history file was not changed in this run - nothing to validate.
        return ""

    # A change that touches no default-bearing source besides this file cannot have changed any
    # setting's compiled default, so it is a historical correction (fixing what a past release
    # recorded), not a default change made now - it must not be forced into the current version
    # block. Enforce the current-block rule as soon as any other source file changed: defaults
    # are not only written in the declaration files, they can come from constants defined
    # anywhere (for example src/Core/Defines.h). Build definitions count as sources too:
    # defaults switch on compile definitions such as `CLICKHOUSE_CLOUD` and
    # `ENABLE_DISTRIBUTED_CACHE`, which are driven by CMake files and config templates
    # (CMakeLists.txt, *.cmake, src/Common/config.h.in), so anything narrower would leave a
    # silent hole. The price is over-strictness for a change that corrects an old entry and
    # edits unrelated code in the same commit - the message below says how to proceed.
    def is_default_bearing_source(f):
        if f == path:
            return False
        if f.startswith("src/") and f.endswith((".h", ".cpp", ".inc")):
            return True
        return f.rsplit("/", 1)[-1] == "CMakeLists.txt" or f.endswith(
            (".cmake", ".cmake.in", ".h.in", ".hpp.in")
        )

    other_sources_changed = any(is_default_bearing_source(f) for f in changed_files)
    if not other_sources_changed:
        return ""

    fetch_error = kv.get("settings_history_fetch_error")
    changed = kv.get("settings_history_changed_settings")
    if fetch_error or changed is None:
        return (
            f"{path} changed but its diff could not be fetched to validate the settings "
            f"history (the check must not be skipped when the file changed). "
            f"Error: {fetch_error or 'no data recorded by the store_data.py workflow hook'}."
        )
    if not changed:
        # The file changed but no setting entries were added (e.g. only reason-text edits or
        # removals) - nothing to validate against the current version block.
        return ""

    version_txt = Path("cmake/autogenerated_versions.txt").read_text(encoding="utf-8")
    current_version = "{}.{}".format(
        re.search(r"SET\(VERSION_MAJOR (\d+)\)", version_txt).group(1),
        re.search(r"SET\(VERSION_MINOR (\d+)\)", version_txt).group(1),
    )

    namespace_by_map = {
        "settings_changes_history": "Session",
        "merge_tree_settings_changes_history": "MergeTree",
    }
    block_re = re.compile(r'addSettingsChanges\(\s*(\w+)\s*,\s*"([\d.]+)"')
    entry_re = re.compile(r'^\s*\{\s*"([A-Za-z0-9_]+)"')

    # Names recorded under the current version block, per namespace, from the final file.
    current_block = {"Session": set(), "MergeTree": set()}
    namespace, version = None, None
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            mb = block_re.search(line)
            if mb and mb.group(1) in namespace_by_map:
                namespace, version = namespace_by_map[mb.group(1)], mb.group(2)
                continue
            me = entry_re.match(line)
            if me and namespace and version == current_version:
                current_block[namespace].add(me.group(1))

    # `changed` is a list of {"namespace", "name"} records produced by the hook, where the
    # namespace is taken from the block the added line sits in - so an overlapping name
    # changed only in one namespace is not spuriously required in the other.
    violations = []
    for item in changed:
        namespace, name = item["namespace"], item["name"]
        if name not in current_block.get(namespace, set()):
            violations.append(
                f"  {namespace} setting '{name}' must be recorded in the '{current_version}' block"
            )

    if violations:
        return (
            f"These settings were added or value-changed in {path} but are not recorded under "
            f"the current version ('{current_version}') block of SettingsChangesHistory.cpp. Add "
            f"an entry for each under the '{current_version}' block (older blocks may keep their "
            f"entries for backports). If this is a correction of what an older release recorded "
            f"and not a default change made here, split it into a change that touches only "
            f"{path}:\n" + "\n".join(sorted(set(violations)))
        )
    return ""


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
    testname = "test_numbers_check"
    # Skip on release branches and backport PRs: backports cherry-pick a small
    # subset of test files, which legitimately leaves large gaps in the numbering.
    info = Info()
    release_branch_re = re.compile(r"^\d{2}\.\d+$")
    branch_to_check = (info.base_branch or info.git_branch or "").removeprefix("release/")
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
    testname = "settings_changes_history"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_settings_changes_history,
            )
        )
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
