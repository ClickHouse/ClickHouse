import argparse
import math
import multiprocessing
import os
import re
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

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

        # Need previous and next line for alignment checks, so skip first/last
        for i in range(1, len(lines) - 1):
            line = lines[i]

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


def check_repo_submodules():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_submodules.sh"
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


def check_codespell():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_typos.sh"
    )
    if err:
        out += err
    return out


def check_aspell():
    res, out, err = Shell.get_res_stdout_stderr(
        "./ci/jobs/scripts/check_style/check_aspell.sh"
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
    if testpattern.lower() in testname.lower():
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
    testname = "cpp"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_cpp_code,
            )
        )
    testname = "submodules"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_repo_submodules,
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
    testname = "codespell"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_codespell,
            )
        )
    testname = "aspell"
    if testpattern.lower() in testname.lower():
        results.append(
            Result.from_commands_run(
                name=testname,
                command=check_aspell,
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
