"""Limited clang-tidy check - analyze only the translation units a change touches.

The full clang-tidy check (the `Build (arm_tidy, i/N)` shards) runs clang-tidy
over every translation unit in the tree. That takes hours, so it cannot be part of the
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

import functools
import json
import os
import re
import shlex
import subprocess
from concurrent.futures import ThreadPoolExecutor

from ci.praktika.gh import GH
from ci.praktika.result import Result
from ci.praktika.utils import Utils

# Name of the result this module reports inside the job.
RESULT_NAME = "Clang-tidy"

# What the tidy build analyzes, and therefore what this check analyzes - the two
# have to agree, or this check would report diagnostics the full one never does
# and fail the merge queue on findings nobody introduced:
#  * the directories whose `CMakeLists.txt` set `CMAKE_CXX_CLANG_TIDY`, all of
#    them in these four roots, see `tidy_enabled_dirs` - `base/poco` and the
#    other siblings of `base/base` do not, so their sources are not analyzed,
#  * C++ sources only, since `CMAKE_C_CLANG_TIDY` is never set,
#  * headers that `HeaderFilterRegex` in `.clang-tidy` matches, i.e. the ones
#    ending in `h` or `hpp` (a `.inl` is compiled but never reported).
#  * minus the targets inside those roots that opt out with
#    `set_target_properties(... PROPERTIES CXX_CLANG_TIDY "")`, see
#    `tidy_disabled_dirs`.
ANALYZED_ROOTS = ("base", "src", "programs", "utils")
SOURCE_SUFFIXES = (".cpp", ".cc", ".cxx")
HEADER_SUFFIXES = (".h", ".hpp")

# C++ fragments that analyzed translation units `#include` but that are neither
# compiled on their own nor matched by `HeaderFilterRegex`: generated keyword
# lists (`SQLKeywordsForHighlighting.inc`), template implementations shared by
# several translation units (`RangeHashedDictionaryGetItemsImpl.txx`). The full
# check never reports a finding located in one, but a change to one can still
# break the compilation of its includers, which it does report as
# a compiler error. So a changed fragment is covered like a changed
# header, through the translation units including it, and only their compile
# errors and their own findings are reported.
FRAGMENT_SUFFIXES = (".inc", ".inl", ".ipp", ".tcc", ".tpp", ".txx")

# A changed header can be included by thousands of translation units, and
# analyzing all of them is exactly the full check this job exists to avoid. The
# sibling translation unit (`Foo.h` -> `Foo.cpp`) is the one that covers a header
# best when it includes the header; for a header that has none - a header-only
# addition, for instance - or whose sibling does not include it, this
# many direct includers are analyzed instead.
MAX_INCLUDERS_PER_HEADER = 3

# Files that decide what the clang-tidy checks report, or how this one selects
# and judges its work, without being analyzed themselves. A change to them alone
# touches no translation unit, yet it can make the full check fail on `master`,
# or break this gate. So instead of being skipped, such a change is validated on
# the fixed `CANARY_TRANSLATION_UNITS`: they are analyzed as the full check
# analyzes them, and every diagnostic it would report for them is reported,
# including those in the headers they include. `master` is clean under the full
# check, so such a diagnostic is one the change introduced - a newly enabled
# check firing, or this module misparsing clang-tidy output.
TIDY_CONFIG_PATHS = (
    ".clang-tidy",
    "cmake/clang_tidy.cmake",
    "ci/jobs/scripts/clang_tidy_changed_files.py",
    "ci/jobs/scripts/workflow_hooks/filter_job.py",
    # The producer and the readers of the changed-files list that selects what
    # is analyzed: a change that drops paths from it would merge unexercised.
    "ci/jobs/scripts/workflow_hooks/store_data.py",
    "ci/praktika/gh.py",
    "ci/praktika/info.py",
    "ci/jobs/build_clickhouse.py",
    # The definition of the job itself and of the workflow scheduling it.
    "ci/defs/defs.py",
    "ci/defs/job_configs.py",
    "ci/workflows/merge_queue.py",
    ".github/workflows/merge_queue.yml",
)
# Inputs of code generators whose output is C++ that clang-tidy parses, mapped
# to the suffixes of the headers generated from them: `Foo.proto` becomes
# `Foo.pb.h` (and `Foo.grpc.pb.h` for a service), a FlatBuffers schema `Foo.fbs`
# becomes `Foo_generated.h`, a `configure_file` template `Foo.h.in` becomes
# `Foo.h`. A change to such an input touches no C++ file, yet the code compiled
# against the regenerated header can stop compiling or start triggering a check.
# The generated headers live in the build directory, so a change to an input is
# covered through the analyzed files that include its generated headers, as if
# those had changed. The full check also reports diagnostics located in a
# generated header whose path its `HeaderFilterRegex` matches (`config.h` is
# generated into `src/Common` of the build directory), so those are reported
# here as well, see `GeneratedHeaders`.
#
# Only generators the build runs belong here. The gperf tables
# (`tldLookup.gperf`, `HTMLCharacterReference.gperf`) and the ANTLR grammars of
# `src/Parsers/Prometheus/grammar` are regenerated by hand (the `generate-source`
# target, `generate.sh`), and their output is committed: the build compiles the
# committed `.generated.cpp` and `generated/antlr4_grammars` files, so a change to
# such an input alone changes nothing clang-tidy sees, and regenerating it shows
# up as a change to the committed C++ files, which are selected like any other.
# The ANTLR output is not analyzed by the full check at all
# (`clickhouse_antlr4_grammars` sets `CXX_CLANG_TIDY` to empty).
GENERATOR_INPUTS = {
    ".proto": (".pb.h", ".grpc.pb.h"),
    ".fbs": ("_generated.h",),
    ".h.in": (".h",),
}
# `configure_file` templates of translation units: `GitHash.cpp.in` becomes
# `GitHash.generated.cpp`, `config_version.cpp.in` becomes `config_version.cpp`,
# both in the build directory and compiled by targets the full check analyzes.
# A change to such a template is covered by analyzing the generated translation
# unit itself, see `template_translation_units`.
SOURCE_TEMPLATE_SUFFIXES = tuple(f"{suffix}.in" for suffix in SOURCE_SUFFIXES)
# Generated headers whose name does not follow from the name of an input: the
# generator, its script, or the directory of its inputs (a submodule shows up in
# the changed files as the bare submodule path), mapped to the headers generated
# from it. A path matches an entry when it is the entry or lies under it; of
# several matching entries the longest one wins, so that a file inside a
# submodule maps to the output of its own generator rather than to everything
# generated from the submodule.
ARROW_IPC_GENERATED_HEADERS = (
    "File_generated.h",
    "Message_generated.h",
    "Schema_generated.h",
    "SparseTensor_generated.h",
    "Tensor_generated.h",
    "feather_generated.h",
)
# The Thrift bindings of the Parquet metadata, generated from `parquet.thrift`
# and committed to `contrib/arrow/cpp/src/generated`.
PARQUET_GENERATED_HEADERS = ("parquet_types.h",)
GENERATOR_INPUT_PATHS = {
    # `contrib/arrow-cmake` regenerates Arrow's IPC metadata bindings from the
    # schemas in `contrib/arrow` with the `flatc` built from `contrib/flatbuffers`.
    # A bump of the `contrib/arrow` submodule, which is all the changed files
    # show of it, can change both those schemas and the Parquet bindings.
    "contrib/arrow": ARROW_IPC_GENERATED_HEADERS + PARQUET_GENERATED_HEADERS,
    "contrib/arrow/format": ARROW_IPC_GENERATED_HEADERS,
    "contrib/arrow/cpp/src/arrow/ipc/feather.fbs": ARROW_IPC_GENERATED_HEADERS,
    "contrib/arrow/cpp/src/parquet/parquet.thrift": PARQUET_GENERATED_HEADERS,
    "contrib/arrow/cpp/src/generated/parquet_types.h": PARQUET_GENERATED_HEADERS,
    "contrib/arrow-cmake": ARROW_IPC_GENERATED_HEADERS,
    "contrib/flatbuffers": ARROW_IPC_GENERATED_HEADERS,
    # The embedded web UI resources, included by `WebUIRequestHandler.cpp`.
    "contrib/clickstack": ("ClickStackResources.generated.h",),
    "contrib/clickstack-cmake": ("ClickStackResources.generated.h",),
    "contrib/sql-console": ("SQLConsoleResources.generated.h",),
    "contrib/sql-console-cmake": ("SQLConsoleResources.generated.h",),
    # The Prometheus protocol bindings that `src/Storages/TimeSeries` includes as
    # `prompb/remote.pb.h`. `types.proto` imports `gogoproto/gogo.proto`, so the
    # generated `types.pb.h` includes `gogo.pb.h`.
    "contrib/prometheus-protobufs": ("remote.pb.h", "types.pb.h"),
    "contrib/prometheus-protobufs-gogo": ("gogo.pb.h", "remote.pb.h", "types.pb.h"),
}
# Generator inputs in `contrib` whose generated headers the analyzed code does not
# include by name, but only through the library's own public headers, and
# submodules whose public headers the analyzed code includes directly, mapped to
# the include spellings of those public headers: a spelling ending in `/` stands
# for every header under that directory. `llvm-config.h` and the other templates
# of `contrib/llvm-project-cmake/config` are reached through `<llvm/...>`
# headers, the `liburing-cmake` templates through `liburing.h`, and a bump of
# `contrib/arrow` or `contrib/llvm-project` can break any of the `<arrow/...>`,
# `<parquet/...>` or `<llvm/...>` headers the analyzed code includes, a bump of
# `contrib/libdivide` the `<libdivide.h>` that the integer division functions
# and `BloomFilter` include, next to the `libdivide-config.h` that
# `contrib/libdivide-cmake` provides. A path
# matches an entry the way it matches one of `GENERATOR_INPUT_PATHS`. Unlike the
# consumers of a generated header, every analyzed file directly including one of
# these is selected, not a few representatives: the surface of a submodule is a
# few dozen files, and each of them uses a different part of it. The public
# headers only select the consumers: they are not part of the analyzed tree, so
# diagnostics in them are not reported.
GENERATOR_INPUT_INCLUDED_THROUGH = {
    "contrib/arrow": ("arrow/", "parquet/"),
    "contrib/arrow-cmake": ("arrow/", "parquet/"),
    "contrib/llvm-project": ("llvm/",),
    "contrib/llvm-project-cmake": ("llvm/",),
    "contrib/liburing": ("liburing.h",),
    "contrib/liburing-cmake": ("liburing.h",),
    "contrib/libdivide": ("libdivide.h", "libdivide-config.h"),
    "contrib/libdivide-cmake": ("libdivide.h", "libdivide-config.h"),
}
# The suffix rules of `GENERATOR_INPUTS` apply only under the analyzed roots,
# where the name of a generated header identifies it: a basename such as
# `config.h` is generated from templates in several `contrib` directories, and
# matching those against the consumers and diagnostics of `src/Common/config.h`
# would check a change against an unrelated header. A generator input in
# `contrib` is covered only when it is listed in `GENERATOR_INPUT_PATHS` or
# `GENERATOR_INPUT_INCLUDED_THROUGH`; any other change there is left to the full
# check, like every other change to `contrib`.

CANARY_TRANSLATION_UNITS = (
    "base/base/JSON.cpp",
    "src/Common/Exception.cpp",
)

# The canaries are analyzed without `--header-filter` and with every diagnostic
# reported, so they exercise none of the selection and filtering this module
# implements for ordinary changes. A change to this module could disable the
# coverage of changed headers or of generator inputs for every later change and
# still merge green on them. So a change to this module also runs `self_test`,
# and these representative files are analyzed as if the change touched them,
# through the ordinary path: a header-only change, covered through the
# includers of its includer (`wide_integer.h` has no translation unit of its
# own), and a generator input, covered through the files including the header
# generated from it. `master` is clean under the full check, so a diagnostic
# reported for them is one the change to this module introduced.
SELF_PATH = "ci/jobs/scripts/clang_tidy_changed_files.py"
SELF_TEST_CHANGED_HEADER = "base/base/wide_integer_impl.h"
SELF_TEST_GENERATOR_INPUT = "src/Common/config.h.in"
SELF_TEST_CHANGED_FRAGMENT = "src/Dictionaries/RangeHashedDictionaryGetItemsImpl.txx"
# A header whose nearest translation unit is four include hops away:
# `NotificationStrategy.h` -> `DefaultStrategy.h` -> `BasicEvent.h` ->
# `EventChannel.h` -> `EventChannel.cpp`. Poco is not analyzed by the full
# check, so the translation unit is not in the filtered compilation database:
# this case exercises only the search over the include graph.
SELF_TEST_DISTANT_HEADER = "base/poco/Foundation/include/Poco/NotificationStrategy.h"
SELF_TEST_DISTANT_TRANSLATION_UNIT = "base/poco/Foundation/src/EventChannel.cpp"
SELF_TEST_SOURCE_TEMPLATE = "src/Common/config_version.cpp.in"
# A header with a same-stem source that does not include it: `divideImpl.cpp`
# instantiates the `libdivide` kernels, and `divideImpl.h` is included only by
# `intDiv.cpp`. The same-stem source must not be taken as its coverage.
SELF_TEST_UNRELATED_SIBLING_HEADER = "src/Functions/divideImpl.h"
SELF_TEST_UNRELATED_SIBLING = "src/Functions/divideImpl.cpp"
SELF_TEST_UNRELATED_SIBLING_INCLUDER = "src/Functions/intDiv.cpp"

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

# With `NO_WARNINGS_AS_ERRORS`, a diagnostic at these levels is not a clang-tidy
# finding but a compiler error, so it is reported wherever it occurs: the
# translation unit failed to compile, and the full check fails on it. That is
# not only `clang-diagnostic-error`: the tidy build compiles with `-Werror`, and
# a compiler warning promoted to an error keeps the name of its warning option,
# such as `clang-diagnostic-unused-variable`.
ERROR_LEVELS = ("fatal error", "error")

# clang-tidy prints these when it crashes. A crash is an infrastructure error,
# not a finding.
CRASH_MARKERS = ("PLEASE submit a bug report", "Stack dump:")

_DIAGNOSTIC_RE = re.compile(
    r"^(?P<file>[^\s:][^:]*):(?P<line>\d+):(?P<column>\d+): "
    r"(?P<level>fatal error|error|warning|note): (?P<message>.*)$"
)
# A message clang-tidy prints without a source location, e.g. `error: unknown
# check`, or a missing entry in the compilation database.
_BARE_ERROR_RE = re.compile(r"^(fatal )?error: (?P<message>.*)$")
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


def config_header_filter_regex(repo_dir):
    """The `HeaderFilterRegex` of `.clang-tidy`, the one the full check applies."""
    path = f"{repo_dir}/.clang-tidy"
    with open(path, "r", encoding="utf-8") as config:
        for line in config:
            match = re.match(r"^HeaderFilterRegex:\s*'(.*)'\s*$", line)
            if match:
                return re.compile(match.group(1))
    raise RuntimeError(f"No HeaderFilterRegex in {path}")


class GeneratedHeaders:
    """The headers regenerated from the change's generator inputs.

    A diagnostic is located in one of them when it is under the build directory,
    has one of their file names, and the full check would report it, i.e. its
    path matches the `HeaderFilterRegex` of `.clang-tidy`. clang-tidy itself
    applies `ExcludeHeaderFilterRegex` from the configuration, which drops the
    ones generated under `contrib`.
    """

    def __init__(self, names, build_dir, full_filter):
        self.names = frozenset(names)
        self.build_dir = os.path.realpath(build_dir) + os.sep
        self.full_filter = full_filter

    def contains(self, path):
        return (
            os.path.basename(path) in self.names
            and path.startswith(self.build_dir)
            and self.full_filter.search(path) is not None
        )


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


def is_fragment_path(path):
    """True for a C++ fragment under a root clang-tidy analyzes, see `FRAGMENT_SUFFIXES`."""
    root = path.split("/", 1)[0]
    return root in ANALYZED_ROOTS and path.endswith(FRAGMENT_SUFFIXES)


def is_selectable_path(path):
    """True for a changed file that selects translation units to analyze."""
    return is_analyzed_path(path) or is_fragment_path(path)


_TIDY_OPT_OUT_RE = re.compile(r'\bCXX_CLANG_TIDY\s+""')
_TIDY_OPT_IN_RE = re.compile(r'\bCMAKE_CXX_CLANG_TIDY\s+"\$\{CLANG_TIDY_PATH\}"')


def cmake_dirs_matching(repo_dir, pattern):
    """Directories under the analyzed roots whose `CMakeLists.txt` matches `pattern`."""
    found = []
    for root in ANALYZED_ROOTS:
        for dir_path, _, file_names in os.walk(f"{repo_dir}/{root}"):
            if "CMakeLists.txt" not in file_names:
                continue
            with open(
                f"{dir_path}/CMakeLists.txt", "r", encoding="utf-8"
            ) as cmake_lists:
                if pattern.search(cmake_lists.read()):
                    found.append(os.path.relpath(dir_path, repo_dir))
    return tuple(sorted(found))


@functools.lru_cache(maxsize=None)
def tidy_enabled_dirs(repo_dir):
    """Repo-relative directories whose targets the full check analyzes.

    `CMAKE_CXX_CLANG_TIDY` is set per directory (`src`, `programs`, `utils`,
    `base/base`, ...) and inherited by the subdirectories cmake enters from
    there, so a target is analyzed when the directory defining it lies in one
    of these. `base/CMakeLists.txt` does not set it, so `base/poco` and the
    other siblings of `base/base` are compiled without clang-tidy. Read from the
    repository rather than listed here, so that a new opt-in cannot be missed.
    """
    return cmake_dirs_matching(repo_dir, _TIDY_OPT_IN_RE)


def is_under_any(path, directories):
    return any(
        path == directory or path.startswith(f"{directory}/")
        for directory in directories
    )


def is_tidy_enabled_dir(path, repo_dir):
    """True for a repo-relative path where the full check analyzes the targets."""
    return is_under_any(path, tidy_enabled_dirs(repo_dir)) and not is_under_any(
        path, tidy_disabled_dirs(repo_dir)
    )


@functools.lru_cache(maxsize=None)
def tidy_disabled_dirs(repo_dir):
    """Repo-relative directories whose target the full check does not analyze.

    A target inside the analyzed roots can disable clang-tidy for itself by
    setting its `CXX_CLANG_TIDY` property to empty: the committed ANTLR output
    of `src/Parsers/Prometheus/grammar`, the protobuf bindings of
    `src/Server/grpc_protos` and `src/Client/BuzzHouse/Proto`. The tidy build
    compiles them without clang-tidy, so analyzing them here would fail the
    merge queue on findings the full check never reports. Such a target lives
    with its sources in the directory of its `CMakeLists.txt`, so that whole
    directory is excluded. Read from the repository rather than listed here, so
    that a new opt-out cannot be missed.
    """
    return cmake_dirs_matching(repo_dir, _TIDY_OPT_OUT_RE)


def is_tidy_disabled_path(path, repo_dir):
    """True for a repo-relative path inside a target that opts out of clang-tidy."""
    return is_under_any(path, tidy_disabled_dirs(repo_dir))


def is_local_tidy_config_path(path):
    """True for a directory-local `.clang-tidy` under the analyzed roots.

    Such a file (`src/Functions/.clang-tidy`, ...) changes the arguments the full
    check analyzes the translation units beneath it with, but none of the
    `CANARY_TRANSLATION_UNITS` lies beneath it. So a change to it is validated
    on a translation unit of its own subtree instead, see
    `representative_translation_unit`.
    """
    root = path.split("/", 1)[0]
    return root in ANALYZED_ROOTS and os.path.basename(path) == ".clang-tidy"


def is_tidy_config_path(path):
    """True for a file that changes what the clang-tidy checks report."""
    return path in TIDY_CONFIG_PATHS or is_local_tidy_config_path(path)


def generator_input_path_entry(path, table=GENERATOR_INPUT_PATHS):
    """The longest entry of `table` that `path` is or lies under, or `None`."""
    matching = [
        entry for entry in table if path == entry or path.startswith(f"{entry}/")
    ]
    return max(matching, key=len, default=None)


def is_suffix_generator_input(path):
    """True for an input under the analyzed roots matched by `GENERATOR_INPUTS`."""
    root = path.split("/", 1)[0]
    return root in ANALYZED_ROOTS and path.endswith(tuple(GENERATOR_INPUTS))


def is_source_template(path):
    """True for a template of a translation unit, see `SOURCE_TEMPLATE_SUFFIXES`."""
    root = path.split("/", 1)[0]
    return root in ANALYZED_ROOTS and path.endswith(SOURCE_TEMPLATE_SUFFIXES)


def is_generator_input(path):
    """True for an input of a code generator that produces C++ clang-tidy parses."""
    return (
        is_source_template(path)
        or generator_input_path_entry(path) is not None
        or generator_input_path_entry(path, GENERATOR_INPUT_INCLUDED_THROUGH)
        is not None
        or is_suffix_generator_input(path)
    )


def normalize_changed_path(path):
    return path.removeprefix("./").removeprefix("/")


def analyzable_changed_files(changed_files, repo_dir):
    """The change's C/C++ files clang-tidy can analyze, repo-relative and sorted.

    That includes the C++ fragments (see `FRAGMENT_SUFFIXES`), which are covered
    through their includers. `changed_files` carries removed files and the
    pre-rename path of a rename. A translation unit that no longer exists cannot
    be analyzed and is dropped. A header or fragment that no longer exists is
    kept: the translation units that still include it by its old path now fail
    to compile, and finding them is the point of selecting it. A file of a
    target that opts out of clang-tidy is dropped, see `tidy_disabled_dirs`, and
    so is a translation unit outside the directories whose targets the full
    check analyzes, see `tidy_enabled_dirs`. A header there is kept: the full
    check reports its diagnostics through the analyzed translation units
    including it.
    """
    return sorted(
        path
        for path in {normalize_changed_path(f) for f in changed_files}
        if is_selectable_path(path)
        and not is_tidy_disabled_path(path, repo_dir)
        and (
            not path.endswith(SOURCE_SUFFIXES)
            or (
                os.path.isfile(f"{repo_dir}/{path}")
                and is_tidy_enabled_dir(path, repo_dir)
            )
        )
    )


def changed_tidy_config_files(changed_files):
    """The change's files from `TIDY_CONFIG_PATHS`, sorted."""
    return sorted(
        path
        for path in {normalize_changed_path(f) for f in changed_files}
        if is_tidy_config_path(path)
    )


def generated_headers(path):
    """The file names of the headers generated from the generator input `path`."""
    headers = set()
    entry = generator_input_path_entry(path)
    if entry is not None:
        headers.update(GENERATOR_INPUT_PATHS[entry])
    if not is_suffix_generator_input(path):
        return sorted(headers)
    for input_suffix, output_suffixes in GENERATOR_INPUTS.items():
        if path.endswith(input_suffix):
            stem = os.path.basename(path)[: -len(input_suffix)]
            headers.update(f"{stem}{suffix}" for suffix in output_suffixes)
            break
    return sorted(headers)


def changed_generator_inputs(changed_files):
    """The change's inputs of code generators, sorted (see `is_generator_input`)."""
    return sorted(
        path
        for path in {normalize_changed_path(f) for f in changed_files}
        if is_generator_input(path)
    )


def generated_header_consumers(inputs, repo_dir, limit, is_coverable):
    """Analyzed files that include a header generated from one of `inputs`.

    At most `limit` per generated header, translation units before headers, so
    that a widely included one like `config.h` does not turn into a full check.
    Candidates are filtered by `is_coverable` before the limit is applied, so
    that files no translation unit in the build covers cannot crowd out ones it
    does. The include is matched by file name: the generated header is found
    through the build directory's include path, so it is included by its name
    alone. An input reached through a library's public headers selects every
    coverable file directly including one of them instead, see
    `GENERATOR_INPUT_INCLUDED_THROUGH`.
    """
    index = include_index(repo_dir)
    consumers = set()
    spellings = set()
    for path in inputs:
        entry = generator_input_path_entry(path, GENERATOR_INPUT_INCLUDED_THROUGH)
        if entry is not None:
            spellings.update(GENERATOR_INPUT_INCLUDED_THROUGH[entry])
        for header in generated_headers(path):
            includers = {includer for includer, _ in index.get(header, ())}
            found = sorted(
                includers,
                key=lambda f: (not f.endswith(SOURCE_SUFFIXES), f),
            )
            taken = 0
            for candidate in found:
                if taken == limit:
                    break
                if is_coverable(candidate):
                    consumers.add(candidate)
                    taken += 1
    if spellings:
        prefixes = tuple(s for s in spellings if s.endswith("/"))
        consumers.update(
            includer
            for directives in index.values()
            for includer, spelling in directives
            if (spelling in spellings or spelling.startswith(prefixes))
            and is_coverable(includer)
        )
    return sorted(consumers)


def load_compile_commands(build_dir, repo_dir):
    """Translation unit path -> its compilation database entry.

    Keyed by the normalized path so that a lookup built from a repo-relative
    path finds the entry. Normalizing rather than resolving is deliberate: the
    database has ~18k entries and `os.path.realpath` on each of them costs
    seconds of syscalls for paths cmake already writes absolute.

    Only the translation units of targets the full check analyzes are kept,
    see `is_tidy_enabled_dir`: the others must not be selected, nor picked to
    cover a changed header. A target is told by the directory defining it,
    which is where its object files go (`base/poco/Foundation/CMakeFiles/...`),
    rather than by the path of the source, which for a generated translation
    unit lies in the build directory.
    """
    with open(f"{build_dir}/compile_commands.json", "r", encoding="utf-8") as db:
        entries = json.load(db)
    compile_commands = {}
    for entry in entries:
        if not is_tidy_enabled_dir(target_directory(entry, build_dir), repo_dir):
            continue
        compile_commands[os.path.normpath(entry["file"])] = entry
    return compile_commands


def target_directory(entry, build_dir):
    """The source directory, repo-relative, of the cmake target compiling `entry`.

    cmake writes object files to `<directory>/CMakeFiles/<target>.dir/` of the
    build directory, with the path of the directory defining the target.
    """
    output = os.path.normpath(entry.get("output", ""))
    if os.path.isabs(output):
        output = os.path.relpath(output, os.path.normpath(build_dir))
    directory, separator, _ = f"/{output}".partition("/CMakeFiles/")
    if not separator:
        raise RuntimeError(
            f"Cannot tell which cmake target builds {entry['file']} from its "
            f"object path [{entry.get('output')}]"
        )
    return directory.lstrip("/")


def template_translation_units(templates, build_dir, compile_commands):
    """The translation units generated from the source `templates`, as database keys.

    `configure_file` writes them to the build directory as `Foo.cpp` or
    `Foo.generated.cpp` for a template `Foo.cpp.in`.
    """
    names = set()
    for path in templates:
        for template_suffix, suffix in zip(SOURCE_TEMPLATE_SUFFIXES, SOURCE_SUFFIXES):
            if path.endswith(template_suffix):
                stem = os.path.basename(path)[: -len(template_suffix)]
                names.update({f"{stem}{suffix}", f"{stem}.generated{suffix}"})
    prefix = os.path.normpath(build_dir) + os.sep
    return sorted(
        path
        for path in compile_commands
        if path.startswith(prefix) and os.path.basename(path) in names
    )


_INCLUDE_SPELLING_RE = re.compile(r'^\s*#\s*include\s*[<"](?P<spelling>[^">]+)[">]')


@functools.lru_cache(maxsize=None)
def include_index(repo_dir):
    """File name -> `(includer, spelling)` of every `#include` directive naming it.

    Built once from a single `git grep` over the analyzed files - sources and
    headers - so that following the include graph is a lookup rather than a
    search of the tree. Only real directives count, not a file name mentioned in
    a comment.
    """
    pathspecs = [
        f"{root}/*{suffix}"
        for root in ANALYZED_ROOTS
        for suffix in SOURCE_SUFFIXES + HEADER_SUFFIXES
    ]
    completed = subprocess.run(
        [
            "git",
            "grep",
            "--no-color",
            "-E",
            "-e",
            r"^[[:space:]]*#[[:space:]]*include[[:space:]]*[<\"]",
            "--",
        ]
        + pathspecs,
        cwd=repo_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    # `git grep` exits with 1 when nothing matches.
    if completed.returncode not in (0, 1):
        raise RuntimeError(f"git grep for include directives failed: {completed.stderr}")
    index = {}
    for line in completed.stdout.splitlines():
        includer, _, text = line.partition(":")
        match = _INCLUDE_SPELLING_RE.match(text)
        if not match:
            continue
        spelling = match.group("spelling")
        index.setdefault(os.path.basename(spelling), []).append((includer, spelling))
    return index


def direct_includers(header, repo_dir):
    """Analyzed files - sources and headers - whose `#include` directive names `header`.

    The include root of `header` is not known here, so a directive matches when
    it resolves to `header` relative to the including file, or when its spelling
    is a path suffix of `header` (`src/Common/Foo.h` as `<Common/Foo.h>`,
    `base/base/Foo.h` as `<base/Foo.h>`). A bare file name that does not resolve
    relative to the includer is accepted only when no directive matches more
    precisely, since it can as well name a same-named header elsewhere.
    """
    precise = set()
    by_name = set()
    for includer, spelling in include_index(repo_dir).get(os.path.basename(header), ()):
        if includer == header:
            continue
        relative = os.path.normpath(f"{os.path.dirname(includer)}/{spelling}")
        if relative == header or (
            "/" in spelling and header.endswith(f"/{spelling}")
        ):
            precise.add(includer)
        else:
            by_name.add(includer)
    return sorted(precise or by_name)


def includes_transitively(source, header, repo_dir):
    """True when `source` includes `header`, directly or through other headers.

    Followed backwards from `header` over the include graph, so that it is a
    lookup in the same index, with the same matching of directives, as the
    search for includers.
    """
    seen = {header}
    frontier = [header]
    while frontier:
        next_frontier = []
        for current in frontier:
            for includer in direct_includers(current, repo_dir):
                if includer == source:
                    return True
                if includer in seen or includer.endswith(SOURCE_SUFFIXES):
                    continue
                seen.add(includer)
                next_frontier.append(includer)
        frontier = next_frontier
    return False


def covering_sibling(header, repo_dir, is_built):
    """The sibling translation unit (`Foo.h` -> `Foo.cpp`) covering `header`, or `None`.

    A source of the same name is only a guess: it is accepted when the build
    compiles it and it actually includes `header`, since a sibling that does not
    (`divideImpl.cpp` next to `divideImpl.h`, which only `intDiv.cpp` includes)
    never parses the header, and a finding in it would pass unnoticed.
    """
    stem = header.rsplit(".", 1)[0]
    return next(
        (
            f"{stem}{suffix}"
            for suffix in SOURCE_SUFFIXES
            if is_built(f"{stem}{suffix}")
            and includes_transitively(f"{stem}{suffix}", header, repo_dir)
        ),
        None,
    )


def find_includers(header, repo_dir, limit, is_built):
    """Translation units that cover `header` through its includers, at most `limit`.

    Breadth-first over the include graph, level by level, until `limit`
    translation units are found or the graph is exhausted: a translation unit
    including `header` is taken directly, and a header including it is covered
    the way a changed header is - through its sibling translation unit, or else
    through its own includers on the next level. A header is often included only
    by other headers, so its nearest translation unit can be several levels away
    (`Poco/NotificationStrategy.h` reaches `EventChannel.cpp` only through
    `DefaultStrategy.h`, `BasicEvent.h` and `EventChannel.h`), and the search
    must not stop at an arbitrary depth. It runs over an in-memory index of the
    include directives, so every header on a level is followed: which includers
    are found must not depend on how their names sort. Only a few translation
    units, nearest first: this is the coverage a header-only change gets, not
    the transitive closure the full check has. Candidates are filtered by
    `is_built` before the limit is applied, so that includers the build does not
    compile cannot crowd out ones it does.
    """
    found = []
    seen = {header}
    frontier = [header]
    while frontier:
        sources = []
        siblings = []
        next_frontier = []
        for current in frontier:
            for includer in direct_includers(current, repo_dir):
                if includer in seen:
                    continue
                seen.add(includer)
                if includer.endswith(SOURCE_SUFFIXES):
                    if is_built(includer):
                        sources.append(includer)
                    continue
                sibling = covering_sibling(includer, repo_dir, is_built)
                if sibling is not None:
                    siblings.append(sibling)
                else:
                    next_frontier.append(includer)
        for candidate in sorted(sources) + sorted(set(siblings)):
            if candidate not in found:
                found.append(candidate)
        if len(found) >= limit:
            break
        frontier = sorted(next_frontier)
    return found[:limit]


def lookup_translation_unit(path, repo_dir, compile_commands):
    """The compilation database entry of the repo-relative `path`, or `None`."""
    return compile_commands.get(os.path.normpath(f"{repo_dir}/{path}"))


def representative_translation_unit(config, repo_dir, compile_commands):
    """A translation unit analyzed with the directory-local `config`, or `None`.

    The first one in path order beneath the directory of `config` that no
    deeper `.clang-tidy` overrides. `config` itself need not exist: a removed
    configuration is validated on the translation units that used it.
    """
    config_dir = os.path.normpath(f"{repo_dir}/{os.path.dirname(config)}")
    for unit in sorted(compile_commands):
        if not unit.startswith(f"{config_dir}/"):
            continue
        directory = os.path.dirname(unit)
        while directory != config_dir and not os.path.isfile(
            f"{directory}/.clang-tidy"
        ):
            directory = os.path.dirname(directory)
        if directory == config_dir:
            return compile_commands[unit]
    return None


def canary_translation_units(config_changed, repo_dir, compile_commands):
    """The translation units validating `config_changed`, analyzed as canaries.

    Returns `(units, notes)`: the paths as the compilation database spells them,
    mapped to the reason they are analyzed, and notes about the changed
    configurations no translation unit of the build uses. A change to a global
    configuration is validated on the `CANARY_TRANSLATION_UNITS`, and a change
    to a directory-local one on a translation unit of its subtree.
    """
    units = {}
    notes = []
    global_changed = [
        path for path in config_changed if not is_local_tidy_config_path(path)
    ]
    if global_changed:
        for canary in CANARY_TRANSLATION_UNITS:
            entry = lookup_translation_unit(canary, repo_dir, compile_commands)
            # A fixed list: a canary missing from the build is a bug in this
            # module, and skipping it would leave the change unvalidated.
            if entry is None:
                raise RuntimeError(
                    f"Canary translation unit {canary} is not in the compilation database"
                )
            units.setdefault(
                entry["file"], f"canary for changed {', '.join(global_changed)}"
            )
    for config in config_changed:
        if not is_local_tidy_config_path(config):
            continue
        entry = representative_translation_unit(config, repo_dir, compile_commands)
        if entry is None:
            notes.append(
                f"No translation unit in the build is analyzed with the changed "
                f"{config}, so it is covered only by the full clang-tidy check"
            )
            continue
        units.setdefault(entry["file"], f"canary for changed {config}")
    return units, notes


def is_coverable(path, repo_dir, compile_commands):
    """True when `select_translation_units` finds a translation unit for `path`.

    The same order of lookups: a source file itself, a header's sibling
    translation unit including it, or a translation unit reached through the
    header's includers.
    """

    def is_built(candidate):
        return (
            lookup_translation_unit(candidate, repo_dir, compile_commands)
            is not None
        )

    if path.endswith(SOURCE_SUFFIXES):
        return is_built(path)
    if covering_sibling(path, repo_dir, is_built) is not None:
        return True
    return bool(find_includers(path, repo_dir, 1, is_built))


def select_translation_units(
    changed, repo_dir, compile_commands, config_changed=(), pinned=()
):
    """Pick the translation units to analyze for `changed`.

    Returns `(translation_units, notes)`: the selected paths as the compilation
    database spells them, and human-readable notes about what the selection
    could and could not cover. When `config_changed` is not empty, the
    translation units of `canary_translation_units` are selected as well.

    The canaries and the translation units selected for the paths of `changed`
    that are also in `pinned` are kept ahead of `MAX_TRANSLATION_UNITS`: they
    validate a change to the configuration or to this module, and a large
    accompanying source change must not crowd them out. Only the remaining
    budget is spent on the other affected translation units.
    """
    selected = {}
    pinned_units = set()
    notes = []
    unbuilt = []
    uncovered_headers = []

    def lookup(path):
        return lookup_translation_unit(path, repo_dir, compile_commands)

    def add(path, reason, pin=False):
        entry = lookup(path)
        if entry is None:
            return False
        selected.setdefault(entry["file"], reason)
        if pin:
            pinned_units.add(entry["file"])
        return True

    canaries, canary_notes = canary_translation_units(
        config_changed, repo_dir, compile_commands
    )
    for unit, reason in canaries.items():
        selected.setdefault(unit, reason)
        pinned_units.add(unit)
    notes.extend(canary_notes)

    for path in changed:
        pin = path in pinned
        if path.endswith(SOURCE_SUFFIXES):
            if not add(path, f"changed: {path}", pin):
                unbuilt.append(path)
            continue

        sibling = covering_sibling(
            path, repo_dir, lambda candidate: lookup(candidate) is not None
        )
        if sibling is not None:
            add(sibling, f"sibling of changed {path}", pin)
            continue

        includers = find_includers(
            path,
            repo_dir,
            MAX_INCLUDERS_PER_HEADER,
            lambda includer: lookup(includer) is not None,
        )
        covered = [
            inc for inc in includers if add(inc, f"includes changed {path}", pin)
        ]
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

    mandatory = sorted(pinned_units)
    others = sorted(set(selected) - pinned_units)
    budget = max(MAX_TRANSLATION_UNITS - len(mandatory), 0)
    if len(others) > budget:
        notes.append(
            f"{len(mandatory) + len(others)} translation units are affected, "
            f"analyzing {len(mandatory) + budget} of them; the full clang-tidy "
            f"check covers the rest"
        )
        others = others[:budget]
    return sorted(mandatory + others), notes


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
        if match and match.group("level") in ("fatal error", "error", "warning"):
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
    the compilation database - which is this. So is a nonzero return code that no
    error diagnostic explains, such as `Error: no checks enabled.`: clang-tidy
    exits nonzero only for errors, as warnings are not turned into errors here.
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
    if returncode != 0 and not any(
        diagnostic["level"] in ERROR_LEVELS
        for diagnostic in parse_diagnostics(output)
    ):
        return f"clang-tidy exited with code {returncode} without reporting an error"
    return ""


def relevant_diagnostics(
    diagnostics, changed_real_paths, report_all=False, generated=None
):
    """The diagnostics this check reports: those in the files the change touches.

    That includes the headers regenerated from the changed generator inputs
    (`generated`, a `GeneratedHeaders`). A compiler error is reported wherever it
    occurs, whatever its check name (see `ERROR_LEVELS`) - it means the
    translation unit failed to compile, so silence about it would be a false
    green. With `report_all`, every diagnostic is reported
    (see `run_one`).
    """
    relevant = []
    for diagnostic in diagnostics:
        if report_all or diagnostic["level"] in ERROR_LEVELS:
            relevant.append(diagnostic)
        elif diagnostic["file"]:
            path = os.path.realpath(diagnostic["file"])
            if path in changed_real_paths or (
                generated is not None and generated.contains(path)
            ):
                relevant.append(diagnostic)
    return relevant


def timeout_output(timeout):
    """The partial output of a timed-out run, as text.

    `TimeoutExpired` carries the captured streams as bytes even when the run
    was given `text=True`.
    """
    return "".join(
        stream.decode("utf-8", errors="replace") if isinstance(stream, bytes) else stream
        for stream in (timeout.stdout, timeout.stderr)
        if stream
    )


def run_one(
    translation_unit,
    tidy,
    db_dir,
    filter_regex,
    log_dir,
    changed_real_paths,
    repo_dir,
    is_canary=False,
    generated=None,
):
    """Analyze one translation unit and turn the outcome into a `Result`.

    A canary translation unit is analyzed the way the full check analyzes it:
    with the `HeaderFilterRegex` of `.clang-tidy` instead of `filter_regex`, and
    with every diagnostic reported, including those in the headers it includes.
    `master` is clean under the full check, so a diagnostic anywhere in a canary
    is one a changed configuration introduced - and a newly enabled check that
    fires only in a header must not stay green here.
    """
    stop_watch = Utils.Stopwatch()
    name = os.path.relpath(translation_unit, repo_dir)
    log_file = f"{log_dir}/{Utils.normalize_string(name)}.log"
    command = [tidy, "-p", db_dir, "--quiet", NO_WARNINGS_AS_ERRORS]
    if not is_canary:
        command.append(f"--header-filter={filter_regex}")
    command.append(translation_unit)

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
        output = timeout_output(timeout)
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
        relevant = relevant_diagnostics(
            parse_diagnostics(output),
            changed_real_paths,
            report_all=is_canary,
            generated=generated,
        )
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


def order_depends_targets(entries, build_dir):
    """The `cmake_object_order_depends_target_<target>` phonies of `entries`.

    cmake gives every target such a phony, which its object files order-depend
    on: it stands for everything the target compiles against.
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


# The rules cmake gives the edges that compile or link a target. Every other
# non-phony edge is a custom command: a code generator, or a utility target.
_COMPILE_OR_LINK_RULE_RE = re.compile(
    r"^[A-Za-z0-9]+_(?:COMPILER|STATIC_LIBRARY_LINKER|SHARED_LIBRARY_LINKER"
    r"|MODULE_LINKER|EXECUTABLE_LINKER)__"
)


def ninja_query(targets, build_dir):
    """The rule and the direct inputs of each of `targets`, from `ninja -t query`.

    Returns `{target: (rule, inputs)}`, with `rule` None for a source file.
    """
    output = subprocess.run(
        ["ninja", "-C", build_dir, "-t", "query", *targets],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    nodes = {}
    target = None
    section = None
    for line in output.splitlines():
        if not line.startswith(" "):
            target = line[: -len(":")]
            nodes[target] = (None, [])
            section = None
        elif line.startswith("  input: "):
            nodes[target] = (line[len("  input: ") :], [])
            section = "input"
        elif not line.startswith("    "):
            # `outputs:`, `validations:` and whatever else follows the inputs.
            section = None
        elif section == "input":
            path = line.strip()
            for marker in ("|| ", "| "):
                if path.startswith(marker):
                    path = path[len(marker) :]
                    break
            nodes[target][1].append(path)
    missing = sorted(set(targets) - set(nodes))
    if missing:
        raise RuntimeError(f"ninja did not describe: {', '.join(missing)}")
    return nodes


def generated_prerequisites(entries, build_dir):
    """The ninja targets that produce the sources `entries` are built against.

    The `cmake_object_order_depends_target_*` phonies of the targets owning
    `entries` cannot be built as they are: through `add_dependencies` they can
    reach the object files of other targets - `unit_tests_dbms` depends on the
    objects of every microbenchmark - and compiling an object in the tidy build
    runs clang-tidy on it, with no changed-file filter and with
    `WarningsAsErrors`. So this walks the graph below those phonies, does not
    descend into the edges that compile or link, and returns the custom commands
    it reaches: the code generators, which is what the translation units need.
    A generator built from the tree (`protoc`, `llvm-tblgen`) is still compiled,
    because it has to run; `assert_no_clang_tidy` checks that it is not analyzed.
    """
    seen = {}
    frontier = order_depends_targets(entries, build_dir)
    while frontier:
        nodes = ninja_query(frontier, build_dir)
        seen.update(nodes)
        following = set()
        for rule, inputs in nodes.values():
            if rule is None or _COMPILE_OR_LINK_RULE_RE.match(rule):
                continue
            following.update(path for path in inputs if path not in seen)
        frontier = sorted(following)
    return sorted(
        target
        for target, (rule, _) in seen.items()
        if rule is not None
        and rule != "phony"
        and not _COMPILE_OR_LINK_RULE_RE.match(rule)
    )


def assert_no_clang_tidy(targets, build_dir):
    """Raise if building `targets` would run clang-tidy.

    cmake runs clang-tidy through `cmake -E __run_co_compile --tidy=...`. Such a
    run would analyze a file the change did not touch, with no diagnostic filter,
    so it must not happen while generating sources.
    """
    commands = subprocess.run(
        ["ninja", "-C", build_dir, "-t", "commands", *targets],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    analyzed = [command for command in commands.splitlines() if "--tidy=" in command]
    if analyzed:
        raise RuntimeError(
            f"Generating the sources would run clang-tidy {len(analyzed)} time(s), "
            f"the first: {analyzed[0]}"
        )


def changed_files_from_ci(info):
    """The change's files, from the workflow data - or from the pull request locally.

    Raises rather than returning nothing when the workflow data has no changed
    files: with an empty list there is nothing to analyze, and a check that
    analyzes nothing is a green one that guards nothing.
    """
    # Locally the files are fetched the way `store_data.py` fetches them for the
    # workflow, so that the old path of a rename is a changed file here as well.
    changed_files = (
        GH.get_changed_files(strict=True)
        if info.is_local_run
        else info.get_changed_files()
    )
    if changed_files is None:
        raise RuntimeError(
            "The workflow data holds no changed files, so there is nothing to "
            "select for the limited clang-tidy check"
        )
    return changed_files


def self_test(repo_dir, build_dir, compile_commands):
    """Check the selection and filtering of this module on representative cases.

    Run when this module changes (see `SELF_PATH`): each case is a property the
    ordinary path relies on and that a regression in this module would silently
    break - a changed header is covered by translation units of the build, a
    generator input is covered through the consumers of its generated header,
    the header filter matches the changed header even by a non-canonical path,
    and diagnostics are parsed and attributed to the right files.
    """
    stop_watch = Utils.Stopwatch()
    failures = []

    def expect(condition, description):
        if not condition:
            failures.append(description)

    header = SELF_TEST_CHANGED_HEADER
    includers = find_includers(
        header,
        repo_dir,
        MAX_INCLUDERS_PER_HEADER,
        lambda path: lookup_translation_unit(path, repo_dir, compile_commands)
        is not None,
    )
    expect(
        includers and all(path.endswith(SOURCE_SUFFIXES) for path in includers),
        f"`find_includers` found no translation unit covering {header}: {includers}",
    )
    units, notes = select_translation_units([header], repo_dir, compile_commands)
    expect(
        units and not notes,
        f"`select_translation_units` did not cover {header}: {units}, {notes}",
    )

    # The search for includers is not cut off at some depth: a header reaching
    # its only built translation unit through several other headers is covered.
    distant_file = os.path.normpath(f"{repo_dir}/{SELF_TEST_DISTANT_TRANSLATION_UNIT}")
    expect(
        os.path.isfile(distant_file),
        f"{SELF_TEST_DISTANT_TRANSLATION_UNIT} does not exist",
    )
    units, notes = select_translation_units(
        [SELF_TEST_DISTANT_HEADER],
        repo_dir,
        {distant_file: {"file": distant_file}},
    )
    expect(
        units == [distant_file] and not notes,
        f"`select_translation_units` did not cover {SELF_TEST_DISTANT_HEADER} "
        f"through {SELF_TEST_DISTANT_TRANSLATION_UNIT}: {units}, {notes}",
    )

    # A same-stem source that does not include the changed header is not its
    # coverage: the header is covered through its actual includer instead.
    sibling_header = SELF_TEST_UNRELATED_SIBLING_HEADER
    sibling_units = {
        os.path.normpath(f"{repo_dir}/{path}"): {
            "file": os.path.normpath(f"{repo_dir}/{path}")
        }
        for path in (SELF_TEST_UNRELATED_SIBLING, SELF_TEST_UNRELATED_SIBLING_INCLUDER)
    }
    expect(
        all(os.path.isfile(path) for path in sibling_units),
        f"{', '.join(sibling_units)} do not all exist",
    )
    units, notes = select_translation_units([sibling_header], repo_dir, sibling_units)
    expect(
        units
        == [os.path.normpath(f"{repo_dir}/{SELF_TEST_UNRELATED_SIBLING_INCLUDER}")]
        and not notes,
        f"`select_translation_units` did not cover {sibling_header} through "
        f"{SELF_TEST_UNRELATED_SIBLING_INCLUDER} alone: {units}, {notes}",
    )

    # The sources of targets that never enable clang-tidy are neither selected
    # nor used to cover a changed header.
    expect(
        not analyzable_changed_files([SELF_TEST_DISTANT_TRANSLATION_UNIT], repo_dir)
        and lookup_translation_unit(
            SELF_TEST_DISTANT_TRANSLATION_UNIT, repo_dir, compile_commands
        )
        is None,
        f"{SELF_TEST_DISTANT_TRANSLATION_UNIT} of a target without clang-tidy "
        f"would be analyzed",
    )

    # A template of a translation unit is covered by the generated translation unit.
    template = SELF_TEST_SOURCE_TEMPLATE
    expect(
        changed_generator_inputs([template]) == [template],
        f"{template} is not recognized as a generator input",
    )
    expect(
        template_translation_units([template], build_dir, compile_commands),
        f"No translation unit generated from {template} is in the compilation database",
    )

    # A changed C++ fragment is covered through the translation units including it.
    fragment = SELF_TEST_CHANGED_FRAGMENT
    expect(
        analyzable_changed_files([fragment], repo_dir) == [fragment],
        f"{fragment} is not selected as a changed file",
    )
    units, notes = select_translation_units([fragment], repo_dir, compile_commands)
    expect(
        units and not notes,
        f"`select_translation_units` did not cover {fragment}: {units}, {notes}",
    )

    # The canaries survive the truncation of a selection exceeding
    # `MAX_TRANSLATION_UNITS` with the ordinary units alone.
    canaries = {
        lookup_translation_unit(canary, repo_dir, compile_commands)["file"]
        for canary in CANARY_TRANSLATION_UNITS
    }
    many = sorted(
        os.path.relpath(unit, repo_dir)
        for unit in compile_commands
        if unit not in canaries and is_analyzed_path(os.path.relpath(unit, repo_dir))
    )[: MAX_TRANSLATION_UNITS + 1]
    units, _ = select_translation_units(
        many, repo_dir, compile_commands, config_changed=[".clang-tidy"]
    )
    expect(
        len(units) == MAX_TRANSLATION_UNITS and canaries <= set(units),
        f"Truncating {len(many)} changed translation units dropped a canary "
        f"or kept {len(units)} units",
    )

    # A directory-local configuration is validated on a translation unit of its
    # own subtree, which the global canaries are not part of.
    for dir_path, _, file_names in sorted(os.walk(f"{repo_dir}/src")):
        if ".clang-tidy" not in file_names:
            continue
        config = os.path.relpath(f"{dir_path}/.clang-tidy", repo_dir)
        local_units, local_notes = canary_translation_units(
            [config], repo_dir, compile_commands
        )
        expect(
            changed_tidy_config_files([config]) == [config]
            and len(local_units) == 1
            and all(
                unit.startswith(f"{os.path.normpath(dir_path)}/") for unit in local_units
            )
            and not local_notes,
            f"The changed {config} is not validated on a translation unit beneath "
            f"it: {local_units}, {local_notes}",
        )

    generator_input = SELF_TEST_GENERATOR_INPUT
    expect(
        changed_generator_inputs([generator_input]) == [generator_input],
        f"{generator_input} is not recognized as a generator input",
    )
    consumers = generated_header_consumers(
        [generator_input],
        repo_dir,
        MAX_INCLUDERS_PER_HEADER,
        lambda path: is_coverable(path, repo_dir, compile_commands),
    )
    expect(
        consumers,
        f"`generated_header_consumers` found no file including the headers "
        f"generated from {generator_input}",
    )
    # A submodule bump is covered through every coverable file directly
    # including one of the submodule's public headers, not through a few
    # representative ones.
    surface_header = "parquet/file_reader.h"
    surface = {
        includer
        for includer, spelling in include_index(repo_dir).get(
            os.path.basename(surface_header), ()
        )
        if spelling == surface_header
        and is_coverable(includer, repo_dir, compile_commands)
    }
    submodule_consumers = set(
        generated_header_consumers(
            ["contrib/arrow"],
            repo_dir,
            MAX_INCLUDERS_PER_HEADER,
            lambda path: is_coverable(path, repo_dir, compile_commands),
        )
    )
    expect(
        surface and surface <= submodule_consumers,
        f"`generated_header_consumers` for contrib/arrow misses files including "
        f"<{surface_header}>: {sorted(surface - submodule_consumers)}",
    )
    # A `contrib` template of the same name generates an unrelated header: it
    # must not be attributed to the one generated from `generator_input`.
    contrib_template = "contrib/llvm-project-cmake/config/config.h.in"
    expect(
        not set(generated_headers(contrib_template))
        & set(generated_headers(generator_input)),
        f"{contrib_template} is mapped to the headers generated from {generator_input}",
    )

    # The targets that opt out of clang-tidy are found, and their files are
    # neither selected nor used to cover a changed header.
    antlr_dir = "src/Parsers/Prometheus/grammar"
    antlr_source = f"{antlr_dir}/generated/antlr4_grammars/PromQLParser.cpp"
    expect(
        antlr_dir in tidy_disabled_dirs(repo_dir),
        f"{antlr_dir} is not recognized as opting out of clang-tidy: "
        f"{tidy_disabled_dirs(repo_dir)}",
    )
    expect(
        not analyzable_changed_files([antlr_source], repo_dir)
        and lookup_translation_unit(antlr_source, repo_dir, compile_commands) is None,
        f"{antlr_source} of a target opting out of clang-tidy would be analyzed",
    )

    # LLVM regular expressions are POSIX extended ones; the filter uses only
    # constructs that Python's `re` interprets the same way.
    header_name = os.path.basename(header)
    header_filter = re.compile(header_filter_regex([header]))
    for path in (
        f"{repo_dir}/{header}",
        f"{repo_dir}/base/base/../base/{header_name}",
        header_name,
    ):
        expect(header_filter.search(path), f"The header filter does not match {path}")
    for path in (f"{repo_dir}/base/base/x{header_name}", f"{repo_dir}/{header}pp"):
        expect(not header_filter.search(path), f"The header filter matches {path}")
    expect(
        not re.search(header_filter_regex([]), f"{repo_dir}/{header}"),
        "The header filter for no changed headers matches a header",
    )

    changed_path = os.path.realpath(f"{repo_dir}/{header}")
    other_path = os.path.realpath(f"{repo_dir}/base/base/wide_integer.h")
    generated_path = os.path.realpath(f"{build_dir}/src/Common/config.h")
    output = "\n".join(
        [
            f"{changed_path}:1:1: warning: changed [bugprone-test]",
            "   1 | quoted source line",
            f"{changed_path}:1:1: note: a note of the warning above",
            f"{other_path}:2:2: warning: unchanged [bugprone-test]",
            f"{other_path}:3:3: error: does not compile [clang-diagnostic-error]",
            f"{other_path}:5:5: error: promoted [clang-diagnostic-unused-variable]",
            f"{generated_path}:4:4: warning: regenerated [bugprone-test]",
            "4 warnings and 2 errors generated.",
            "error: unattributed [clang-diagnostic-error]",
        ]
    )
    diagnostics = parse_diagnostics(output)
    expect(
        [d["message"].split(" [")[0] for d in diagnostics]
        == [
            "changed",
            "unchanged",
            "does not compile",
            "promoted",
            "regenerated",
            "unattributed",
        ],
        f"`parse_diagnostics` split the output wrongly: {diagnostics}",
    )
    expect(
        diagnostics and "a note of the warning above" in diagnostics[0]["text"],
        "`parse_diagnostics` did not attach a note to its diagnostic",
    )
    generated = GeneratedHeaders(
        names=generated_headers(generator_input),
        build_dir=build_dir,
        full_filter=config_header_filter_regex(repo_dir),
    )
    relevant = relevant_diagnostics(diagnostics, {changed_path}, generated=generated)
    expect(
        [d["message"].split(" [")[0] for d in relevant]
        == ["changed", "does not compile", "promoted", "regenerated", "unattributed"],
        f"`relevant_diagnostics` selected the wrong diagnostics: {relevant}",
    )
    expect(
        len(relevant_diagnostics(diagnostics, set(), report_all=True))
        == len(diagnostics),
        "`relevant_diagnostics` with `report_all` dropped diagnostics",
    )

    expect(
        not infrastructure_error(output, 1),
        "A compile error is taken for an infrastructure error",
    )
    expect(
        infrastructure_error("error: unknown check 'foo'", 1),
        "clang-tidy refusing to run is not an infrastructure error",
    )
    expect(
        infrastructure_error("", -9),
        "A killed clang-tidy is not an infrastructure error",
    )
    expect(
        infrastructure_error("Error: no checks enabled.", 1),
        "clang-tidy enabling no checks is not an infrastructure error",
    )
    expect(
        infrastructure_error(
            f"{changed_path}:1:1: warning: changed [bugprone-test]", 1
        ),
        "A nonzero exit explained by no error is not an infrastructure error",
    )
    expect(
        not infrastructure_error(
            f"{changed_path}:1:1: warning: changed [bugprone-test]", 0
        ),
        "A warning is taken for an infrastructure error",
    )

    expect(
        timeout_output(
            subprocess.TimeoutExpired("clang-tidy", 1, output=b"out\n", stderr=b"err")
        )
        == "out\nerr"
        and timeout_output(subprocess.TimeoutExpired("clang-tidy", 1, stderr=b"err"))
        == "err",
        "`timeout_output` did not decode the partial output of a timeout",
    )

    return Result(
        name="Self-test",
        status=Result.Status.FAIL if failures else Result.Status.OK,
        start_time=stop_watch.start_time,
        duration=stop_watch.duration,
        info="\n".join(failures),
    )


def run(changed_files, repo_dir, build_dir, temp_dir):
    """Run the limited clang-tidy check and report it as a single `Result`."""
    stop_watch = Utils.Stopwatch()
    changed = analyzable_changed_files(changed_files, repo_dir)
    config_changed = changed_tidy_config_files(changed_files)
    generator_inputs = changed_generator_inputs(changed_files)
    print(f"Changed files clang-tidy analyzes: {changed}")
    print(f"Changed clang-tidy configuration files: {config_changed}")
    print(f"Changed inputs of code generators: {generator_inputs}")
    if not changed and not config_changed and not generator_inputs:
        return Result.create_from(
            name=RESULT_NAME,
            status=Result.Status.SKIPPED,
            stopwatch=stop_watch,
            info="The change touches no C or C++ file that clang-tidy analyzes",
        )

    compile_commands = load_compile_commands(build_dir, repo_dir)
    self_results = []
    # The files analyzed to validate a change to this module, kept ahead of the
    # truncation in `select_translation_units`.
    pinned = set()
    if SELF_PATH in config_changed:
        self_results.append(self_test(repo_dir, build_dir, compile_commands))
        changed = sorted(set(changed) | {SELF_TEST_CHANGED_HEADER})
        generator_inputs = sorted(set(generator_inputs) | {SELF_TEST_GENERATOR_INPUT})
        pinned = {SELF_TEST_CHANGED_HEADER} | set(
            generated_header_consumers(
                [SELF_TEST_GENERATOR_INPUT],
                repo_dir,
                MAX_INCLUDERS_PER_HEADER,
                lambda path: is_coverable(path, repo_dir, compile_commands),
            )
        )
        print(
            f"This module changed, also analyzing as changed: "
            f"{SELF_TEST_CHANGED_HEADER}, {SELF_TEST_GENERATOR_INPUT}"
        )
    consumers = generated_header_consumers(
        generator_inputs,
        repo_dir,
        MAX_INCLUDERS_PER_HEADER,
        lambda path: is_coverable(path, repo_dir, compile_commands),
    )
    print(f"Files including headers generated from changed inputs: {consumers}")
    templates = [path for path in generator_inputs if is_source_template(path)]
    header_inputs = [path for path in generator_inputs if not is_source_template(path)]
    # Spelled relative to the repository, like the other changed files, so that
    # they are looked up, selected and reported the same way.
    regenerated_units = [
        os.path.relpath(unit, repo_dir)
        for unit in template_translation_units(templates, build_dir, compile_commands)
    ]
    print(f"Translation units generated from changed templates: {regenerated_units}")
    # The consumers and the regenerated translation units are selected and
    # reported as if they had changed: `master` is clean under the full check,
    # so a diagnostic located in them is one the regenerated file introduced.
    changed = sorted(set(changed) | set(consumers) | set(regenerated_units))
    translation_units, notes = select_translation_units(
        changed, repo_dir, compile_commands, config_changed, pinned
    )
    if header_inputs and not consumers:
        notes.append(
            "No translation unit in the build covers a file including the headers "
            "generated from these changed inputs, so they are covered only by the "
            "full clang-tidy check: " + ", ".join(header_inputs)
        )
    if templates and not regenerated_units:
        notes.append(
            "No translation unit in the build is generated from these changed "
            "templates, so they are covered only by the full clang-tidy check: "
            + ", ".join(templates)
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

    prerequisites = generated_prerequisites(entries, build_dir)
    generate_results = []
    # `ninja` without targets builds everything, so it is not run when the units
    # need nothing generated.
    if prerequisites:
        assert_no_clang_tidy(prerequisites, build_dir)
        generate = Result.from_commands_run(
            name="Generate sources",
            command="ninja " + " ".join(shlex.quote(target) for target in prerequisites),
            workdir=build_dir,
            with_log=True,
        )
        generate_results.append(generate)
        if not generate.is_ok():
            return Result.create_from(
                name=RESULT_NAME,
                results=self_results + generate_results,
                stopwatch=stop_watch,
                info=notes,
            )

    tidy = clang_tidy_binary(build_dir)
    # The headers regenerated from the changed inputs are filtered in by name,
    # like the changed headers: a diagnostic in a regenerated header is one the
    # change introduced, and the full check reports it.
    generated = GeneratedHeaders(
        names={
            header for path in generator_inputs for header in generated_headers(path)
        },
        build_dir=build_dir,
        full_filter=config_header_filter_regex(repo_dir),
    )
    filter_regex = header_filter_regex(
        [path for path in changed if path.endswith(HEADER_SUFFIXES)]
        + sorted(generated.names)
    )
    # The full check never reports a finding located in a fragment, so only the
    # compile errors of the translation units including it are reported.
    changed_real_paths = {
        os.path.realpath(f"{repo_dir}/{path}")
        for path in changed
        if not is_fragment_path(path)
    }
    canary_units, _ = canary_translation_units(
        config_changed, repo_dir, compile_commands
    )
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
                    unit in canary_units,
                    generated,
                ),
                translation_units,
            )
        )

    return Result.create_from(
        name=RESULT_NAME,
        results=self_results + generate_results + results,
        stopwatch=stop_watch,
        info=notes,
    )
