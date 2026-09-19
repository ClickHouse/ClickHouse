"""Limited clang-tidy check - analyze only the translation units a change touches.

The full clang-tidy check (`Build (arm_tidy)`) runs clang-tidy over every
translation unit in the tree. That takes hours, so it cannot be part of the
merge queue, and a pull request whose own CI was green can still break
clang-tidy on `master` once it is merged on top of changes that landed after it
last ran. This module is the narrow counterpart that is cheap enough to gate the
merge queue: the job configures the very same tidy build (so the diagnostics are
the ones the full check would report), builds only the generated sources the
selected translation units depend on, and runs `clang-tidy` on those translation
units alone.

What this job spends its time on is everything that has to happen before
clang-tidy can parse anything: configuring the tidy build, and building the
generated sources the affected targets are compiled against - protobuf and
TableGen output, the `cbindgen` headers of the Rust crates. The tidy build
compiles and links through `cmake/dummy_compiler_linker.sh`, so none of the tree
itself is compiled, but those generators are real work and they are the floor on
this job's runtime; the clang-tidy runs themselves are the cheap part.

Diagnostics are reported only for files the change touches, and that filter is
what makes the check safe as a merge-queue gate. clang-tidy reports diagnostics
for every analyzed header a translation unit pulls in, so without the filter a
single pre-existing diagnostic in a widely included header would fail the check
for unrelated pull requests - blocking every merge, including the merge of its
own fix.
"""

import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

# Name of the result this module reports inside the job.
RESULT_NAME = "Clang-tidy"

# What the tidy build analyzes, and therefore what this check analyzes - the two
# have to agree, or this check would report diagnostics the full one never does
# and fail the merge queue on findings nobody introduced:
#  * the four roots whose `CMakeLists.txt` set `CMAKE_CXX_CLANG_TIDY`,
#  * C++ sources only, since `CMAKE_C_CLANG_TIDY` is never set,
#  * headers that `HeaderFilterRegex` in `.clang-tidy` matches, i.e. the ones
#    ending in `h` or `hpp` (a `.inl` is compiled but never reported).
ANALYZED_ROOTS = ("base", "src", "programs", "utils")
SOURCE_SUFFIXES = (".cpp", ".cc", ".cxx")
HEADER_SUFFIXES = (".h", ".hpp")

# A changed header can be included by thousands of translation units, and
# analyzing all of them is exactly the full check this job exists to avoid. The
# sibling translation unit (`Foo.h` -> `Foo.cpp`) is the one that covers a header
# best; for a header that has none - a header-only addition, for instance - this
# many direct includers are analyzed instead.
MAX_INCLUDERS_PER_HEADER = 3

# Upper bound on the translation units one run analyzes, so that a sweeping
# change cannot turn the merge-queue gate back into a multi-hour build. Beyond
# it the selection is truncated and the job says so; the full check still covers
# the rest on `master`.
MAX_TRANSLATION_UNITS = 200

# The address space the build allows a single compilation
# (`cmake/heavy_build_check_scripts/prlimit_generic.sh`), reused here to size
# how many clang-tidy processes fit in the runner's memory.
MEMORY_PER_PROCESS = 5 * 1024**3

# Wall-clock bound for one translation unit. Nothing in the tree comes close;
# this only keeps a pathological input from consuming the whole job timeout.
TIDY_TIMEOUT = 1800

# `WarningsAsErrors: '*'` in `.clang-tidy` turns every clang-tidy warning into an
# `error:` diagnostic. This job decides pass/fail from the parsed diagnostics
# rather than from the exit code, and it needs the level to keep telling a
# clang-tidy finding apart from a genuine compile error, so warnings are left as
# warnings. An empty value does not work: clang-tidy keeps the value from the
# config file unless the option is given a non-empty one.
NO_WARNINGS_AS_ERRORS = "--warnings-as-errors=-*"

# Diagnostics whose check list contains this are not clang-tidy findings but the
# compiler failing to parse the translation unit, so they are reported wherever
# they occur: the analysis did not actually run.
COMPILE_ERROR_CHECK = "clang-diagnostic-error"

# clang-tidy prints these when it crashes. A crash is an infrastructure error,
# not a finding.
CRASH_MARKERS = ("PLEASE submit a bug report", "Stack dump:")

_DIAGNOSTIC_RE = re.compile(
    r"^(?P<file>[^\s:][^:]*):(?P<line>\d+):(?P<column>\d+): "
    r"(?P<level>error|warning|note): (?P<message>.*)$"
)
# A message clang-tidy prints without a source location, e.g. `error: unknown
# check`, or a missing entry in the compilation database.
_BARE_ERROR_RE = re.compile(r"^error: (?P<message>.*)$")
# clang-tidy's own bookkeeping, printed between and after the diagnostics. It
# ends the diagnostic above it rather than belonging to it, so that a reported
# finding does not carry a tree-wide warning count in its text.
_SUMMARY_RE = re.compile(
    r"^(\d+ (warning|error)s?( and \d+ (warning|error)s?)? generated\.|"
    r"Suppressed \d+ warnings.*|"
    r"Use -header-filter=.*|"
    r"Error while processing .*)$"
)
_REGEX_SPECIAL_RE = re.compile(r"([.^$*+?()\[\]{}|\\])")


def read_cmake_cache_value(build_dir, name):
    """The value of a cmake cache entry, or an empty string when it is unset."""
    path = f"{build_dir}/CMakeCache.txt"
    prefix = f"{name}:"
    with open(path, "r", encoding="utf-8") as cache:
        for line in cache:
            if line.startswith(prefix) and "=" in line:
                return line.split("=", 1)[1].strip()
    return ""


def clang_tidy_binary(build_dir):
    """The clang-tidy the tidy build resolved, so both checks run the same one.

    `cmake/clang_tidy.cmake` puts the caching wrapper in front of the binary in
    `CLANG_TIDY_PATH` when it finds one. The wrapper is deliberately dropped
    here: it caches a run by its arguments and replays it without its output,
    and the arguments this job adds mean its entries would never be the full
    check's anyway.
    """
    value = read_cmake_cache_value(build_dir, "CLANG_TIDY_PATH")
    if not value:
        raise RuntimeError(
            f"CLANG_TIDY_PATH is not set in {build_dir}/CMakeCache.txt - "
            "the build directory was not configured with -DENABLE_CLANG_TIDY=1"
        )
    return value.split(";")[-1]


def is_analyzed_path(path):
    """True for a C/C++ file under a root clang-tidy analyzes."""
    root = path.split("/", 1)[0]
    return root in ANALYZED_ROOTS and path.endswith(SOURCE_SUFFIXES + HEADER_SUFFIXES)


def analyzable_changed_files(changed_files, repo_dir):
    """The change's C/C++ files clang-tidy can analyze, repo-relative and sorted.

    Paths that no longer exist are dropped: `changed_files` carries removed
    files and the pre-rename path of a rename, and neither can be analyzed.
    """
    return sorted(
        path
        for path in {f.removeprefix("./") for f in changed_files}
        if is_analyzed_path(path) and os.path.isfile(f"{repo_dir}/{path}")
    )


def load_compile_commands(build_dir):
    """Translation unit path -> its compilation database entry.

    Keyed by the normalized path so that a lookup built from a repo-relative
    path finds the entry. Normalizing rather than resolving is deliberate: the
    database has ~18k entries and `os.path.realpath` on each of them costs
    seconds of syscalls for paths cmake already writes absolute.
    """
    with open(f"{build_dir}/compile_commands.json", "r", encoding="utf-8") as db:
        entries = json.load(db)
    return {os.path.normpath(entry["file"]): entry for entry in entries}


def include_needles(header):
    """Include spellings to search for, longest first.

    A header is included by its path below its include root - `src/Common/Foo.h`
    as `<Common/Foo.h>`, `base/base/Foo.h` as `<base/Foo.h>` - and the root is
    not known here, so every suffix of the path is a candidate.
    """
    parts = header.split("/")
    return ["/".join(parts[i:]) for i in range(1, len(parts))]


def find_includers(header, repo_dir, limit):
    """Translation units that include `header` directly, at most `limit` of them.

    Only direct includers, and only a few of them: this is the coverage a
    header-only change gets, not the transitive closure the full check has.
    """
    pathspecs = [
        f"{root}/*{suffix}" for root in ANALYZED_ROOTS for suffix in SOURCE_SUFFIXES
    ]
    for needle in include_needles(header):
        completed = subprocess.run(
            [
                "git",
                "grep",
                "--files-with-matches",
                "--fixed-strings",
                "-e",
                needle,
                "--",
            ]
            + pathspecs,
            cwd=repo_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        includers = sorted(
            line.strip() for line in completed.stdout.splitlines() if line.strip()
        )
        if includers:
            return includers[:limit]
    return []


def select_translation_units(changed, repo_dir, compile_commands):
    """Pick the translation units to analyze for `changed`.

    Returns `(translation_units, notes)`: the selected paths as the compilation
    database spells them, and human-readable notes about what the selection
    could and could not cover.
    """
    selected = {}
    notes = []
    unbuilt = []
    uncovered_headers = []

    def add(path, reason):
        entry = compile_commands.get(os.path.normpath(f"{repo_dir}/{path}"))
        if entry is None:
            return False
        selected.setdefault(entry["file"], reason)
        return True

    for path in changed:
        if path.endswith(SOURCE_SUFFIXES):
            if not add(path, f"changed: {path}"):
                unbuilt.append(path)
            continue

        stem = path.rsplit(".", 1)[0]
        if any(
            add(f"{stem}{suffix}", f"sibling of changed {path}")
            for suffix in SOURCE_SUFFIXES
        ):
            continue

        includers = find_includers(path, repo_dir, MAX_INCLUDERS_PER_HEADER)
        covered = [inc for inc in includers if add(inc, f"includes changed {path}")]
        if not covered:
            uncovered_headers.append(path)

    if unbuilt:
        notes.append(
            "Not part of the build with this configuration, so not analyzed: "
            + ", ".join(unbuilt)
        )
    if uncovered_headers:
        notes.append(
            "No translation unit in the build includes these changed headers, "
            "so they are covered only by the full clang-tidy check: "
            + ", ".join(uncovered_headers)
        )

    translation_units = sorted(selected)
    if len(translation_units) > MAX_TRANSLATION_UNITS:
        notes.append(
            f"{len(translation_units)} translation units are affected, analyzing "
            f"the first {MAX_TRANSLATION_UNITS}; the full clang-tidy check covers the rest"
        )
        translation_units = translation_units[:MAX_TRANSLATION_UNITS]
    return translation_units, notes


def escape_for_llvm_regex(value):
    return _REGEX_SPECIAL_RE.sub(r"\\\1", value)


def header_filter_regex(headers):
    """The `--header-filter` limiting reported header diagnostics to `headers`.

    Matches on the file name alone, not the path: clang reports a header by the
    path it was reached through, which can be non-canonical
    (`base/base/../base/types.h`), and a path-shaped filter would then not match
    it. A same-named header elsewhere in the tree does pass this filter, and is
    dropped afterwards by `relevant_diagnostics`, which compares resolved paths.

    With no changed headers the filter must match nothing, so that only the
    diagnostics of the translation units themselves are reported.
    """
    if not headers:
        return "^$"
    names = sorted({os.path.basename(header) for header in headers})
    alternatives = "|".join(escape_for_llvm_regex(name) for name in names)
    return f"(^|/)({alternatives})$"


def write_compile_commands_subset(entries, out_dir):
    """A compilation database holding only `entries`.

    The build's own database has ~18k entries and is over 100 MB; clang-tidy
    parses the whole file on every invocation, so it is handed a small one.
    """
    os.makedirs(out_dir, exist_ok=True)
    with open(f"{out_dir}/compile_commands.json", "w", encoding="utf-8") as db:
        json.dump(entries, db)
    return out_dir


def parse_diagnostics(output):
    """Split clang-tidy output into `file`/`level`/`message`/`text` diagnostics.

    `note:` diagnostics and the quoted source lines belong to the diagnostic
    above them and are kept in its `text`. An error clang-tidy cannot attribute
    to a source location - a preprocessor error, or a check name it does not
    know - is printed without a file prefix and becomes a diagnostic with an
    empty `file`.
    """
    diagnostics = []
    current = None
    for line in output.splitlines():
        match = _DIAGNOSTIC_RE.match(line)
        if match and match.group("level") in ("error", "warning"):
            current = {
                "file": match.group("file"),
                "level": match.group("level"),
                "message": match.group("message"),
                "text": [line],
            }
            diagnostics.append(current)
            continue
        bare = _BARE_ERROR_RE.match(line)
        if bare:
            current = {
                "file": "",
                "level": "error",
                "message": bare.group("message"),
                "text": [line],
            }
            diagnostics.append(current)
            continue
        if _SUMMARY_RE.match(line):
            current = None
            continue
        if current is not None:
            current["text"].append(line)
    for diagnostic in diagnostics:
        diagnostic["text"] = "\n".join(diagnostic["text"])
    return diagnostics


def infrastructure_error(output, returncode):
    """Why clang-tidy produced no verdict, or an empty string if it did.

    A negative return code is a signal - clang-tidy was killed or crashed. An
    `error:` line without a source location is either a compile error clang
    could not place, which is a finding and is reported as one, or clang-tidy
    refusing to run at all - an unknown check, a translation unit missing from
    the compilation database - which is this.
    """
    if returncode < 0:
        return f"clang-tidy was killed by signal {-returncode}"
    for marker in CRASH_MARKERS:
        if marker in output:
            return "clang-tidy crashed"
    for line in output.splitlines():
        match = _BARE_ERROR_RE.match(line)
        if match and "clang-diagnostic" not in match.group("message"):
            return f"clang-tidy failed to run: {match.group('message')}"
    return ""


def relevant_diagnostics(diagnostics, changed_real_paths):
    """The diagnostics this check reports: those in the files the change touches.

    A compile error is reported wherever it occurs - it means the translation
    unit was never analyzed, so silence about it would be a false green.
    """
    relevant = []
    for diagnostic in diagnostics:
        if COMPILE_ERROR_CHECK in diagnostic["message"]:
            relevant.append(diagnostic)
        elif (
            diagnostic["file"]
            and os.path.realpath(diagnostic["file"]) in changed_real_paths
        ):
            relevant.append(diagnostic)
    return relevant


def run_one(
    translation_unit, tidy, db_dir, filter_regex, log_dir, changed_real_paths, repo_dir
):
    """Analyze one translation unit and turn the outcome into a `Result`."""
    stop_watch = Utils.Stopwatch()
    name = os.path.relpath(translation_unit, repo_dir)
    log_file = f"{log_dir}/{Utils.normalize_string(name)}.log"
    command = [
        tidy,
        "-p",
        db_dir,
        "--quiet",
        NO_WARNINGS_AS_ERRORS,
        f"--header-filter={filter_regex}",
        translation_unit,
    ]

    print(f"> clang-tidy {name}")
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=TIDY_TIMEOUT,
            check=False,
        )
        output = completed.stdout + completed.stderr
        returncode = completed.returncode
    except subprocess.TimeoutExpired as timeout:
        output = (timeout.stdout or "") + (timeout.stderr or "")
        with open(log_file, "w", encoding="utf-8") as log:
            log.write(output)
        return Result(
            name=name,
            status=Result.Status.ERROR,
            start_time=stop_watch.start_time,
            duration=stop_watch.duration,
            info=f"clang-tidy did not finish within {TIDY_TIMEOUT} seconds",
            files=[log_file],
        )

    with open(log_file, "w", encoding="utf-8") as log:
        log.write(" ".join(command) + "\n\n" + output)

    error = infrastructure_error(output, returncode)
    if error:
        status, info = Result.Status.ERROR, error
    else:
        relevant = relevant_diagnostics(parse_diagnostics(output), changed_real_paths)
        if relevant:
            status = Result.Status.FAIL
            info = "\n".join(diagnostic["text"] for diagnostic in relevant)
        else:
            status, info = Result.Status.OK, ""

    return Result(
        name=name,
        status=status,
        start_time=stop_watch.start_time,
        duration=stop_watch.duration,
        info=info,
        files=[log_file],
    )


def max_workers(count):
    """How many clang-tidy processes to run at once, bounded by cores and memory."""
    by_memory = max(1, Utils.physical_memory() // MEMORY_PER_PROCESS)
    return max(1, min(count, Utils.cpu_count(), by_memory))


def generated_prerequisites(entries, build_dir):
    """The ninja targets that produce the sources `entries` are built against.

    cmake gives every target a `cmake_object_order_depends_target_<target>`
    phony that its object files order-depend on, which is exactly the "generate
    everything this target compiles against" target. The tidy build compiles and
    links through `cmake/dummy_compiler_linker.sh`, so building these generates
    the sources without compiling the tree.
    """
    targets = set()
    for entry in entries:
        match = re.search(r"CMakeFiles/([^/]+)\.dir/", entry.get("output", ""))
        if match is None:
            raise RuntimeError(
                f"Cannot tell which cmake target builds {entry['file']} from its "
                f"object path [{entry.get('output')}]"
            )
        targets.add(f"cmake_object_order_depends_target_{match.group(1)}")

    known = set()
    with open(f"{build_dir}/build.ninja", "r", encoding="utf-8") as ninja:
        for line in ninja:
            if line.startswith("build cmake_object_order_depends_target_"):
                known.add(line[len("build ") :].split(":", 1)[0])
    missing = sorted(targets - known)
    if missing:
        raise RuntimeError(f"ninja has no such targets: {', '.join(missing)}")
    return sorted(targets)


def changed_files_from_ci(info):
    """The change's files, from the workflow data - or from the pull request locally.

    Raises rather than returning nothing when the workflow data has no changed
    files: with an empty list there is nothing to analyze, and a check that
    analyzes nothing is a green one that guards nothing.
    """
    if info.is_local_run:
        return Shell.get_output(
            f"gh pr diff {info.pr_number} --repo {info.repo_name} --name-only",
            strict=True,
        ).splitlines()
    changed_files = info.get_changed_files()
    if changed_files is None:
        raise RuntimeError(
            "The workflow data holds no changed files, so there is nothing to "
            "select for the limited clang-tidy check"
        )
    return changed_files


def run(changed_files, repo_dir, build_dir, temp_dir):
    """Run the limited clang-tidy check and report it as a single `Result`."""
    stop_watch = Utils.Stopwatch()
    changed = analyzable_changed_files(changed_files, repo_dir)
    print(f"Changed files clang-tidy analyzes: {changed}")
    if not changed:
        return Result.create_from(
            name=RESULT_NAME,
            status=Result.Status.SKIPPED,
            stopwatch=stop_watch,
            info="The change touches no C or C++ file that clang-tidy analyzes",
        )

    compile_commands = load_compile_commands(build_dir)
    translation_units, notes = select_translation_units(
        changed, repo_dir, compile_commands
    )
    for note in notes:
        print(f"NOTE: {note}")
    if not translation_units:
        return Result.create_from(
            name=RESULT_NAME,
            status=Result.Status.SKIPPED,
            stopwatch=stop_watch,
            info=notes or ["No translation unit in the build covers the changed files"],
        )

    entries = [compile_commands[unit] for unit in translation_units]
    database_dir = write_compile_commands_subset(entries, f"{temp_dir}/clang_tidy_db")
    log_dir = f"{temp_dir}/clang_tidy_logs"
    os.makedirs(log_dir, exist_ok=True)

    generate = Result.from_commands_run(
        name="Generate sources",
        command=f"ninja {' '.join(generated_prerequisites(entries, build_dir))}",
        workdir=build_dir,
        with_log=True,
    )
    if not generate.is_ok():
        return Result.create_from(
            name=RESULT_NAME,
            results=[generate],
            stopwatch=stop_watch,
            info=notes,
        )

    tidy = clang_tidy_binary(build_dir)
    filter_regex = header_filter_regex(
        [path for path in changed if path.endswith(HEADER_SUFFIXES)]
    )
    changed_real_paths = {os.path.realpath(f"{repo_dir}/{path}") for path in changed}
    workers = max_workers(len(translation_units))
    print(
        f"Running [{tidy}] on {len(translation_units)} translation unit(s) "
        f"with {workers} worker(s), --header-filter={filter_regex}"
    )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(
            pool.map(
                lambda unit: run_one(
                    unit,
                    tidy,
                    database_dir,
                    filter_regex,
                    log_dir,
                    changed_real_paths,
                    repo_dir,
                ),
                translation_units,
            )
        )

    return Result.create_from(
        name=RESULT_NAME,
        results=[generate] + results,
        stopwatch=stop_watch,
        info=notes,
    )
