"""
Regression tests for the codegen_select_fuzzer grammar's rebuild dependencies.

The grammar is generated at build time from codegen.dict, which
tests/fuzz/generate_source_dict.sh derives by grepping source trees. The
generator applies no suffix filter, so every file under a scanned tree is an
input, and an incremental build only regenerates when a file CMake lists as a
dependency changes. Two sets therefore have to agree:

  - the trees and files generate_source_dict.sh scans;
  - the dependency set src/Parsers/fuzzers/codegen_fuzzer/CMakeLists.txt builds
    for the codegen.dict command.

If the dependency set is the narrower one, editing a carrier it omits leaves
codegen.dict untouched and codegen_select_fuzzer keeps fuzzing with a grammar
that no longer matches the sources. src/Functions/stl.hpp is such a carrier:
a `*.h` / `*.cpp` glob does not match a `.hpp` file.

The comparison is over resolved file paths rather than the two lists of
patterns, because a pattern list can agree while the sets it expands to do not.
Every assertion has a mutation arm that narrows one side and requires the
disagreement to be seen.
"""

import fnmatch
import os
import re
import shutil
import subprocess
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
_CODEGEN_CMAKE = os.path.join(
    _REPO, "src/Parsers/fuzzers/codegen_fuzzer/CMakeLists.txt"
)
_GENERATOR = os.path.join(_REPO, "tests/fuzz/generate_source_dict.sh")

# The CMake variable holding the generated dictionary's source dependencies.
_DEPS_VAR = "CODEGEN_DICT_SCANNED_SOURCES"

# A tracked file under a scanned tree whose suffix is neither .h nor .cpp, and
# which carries a token the generator extracts. It stands in for the whole class
# a suffix-filtered glob drops.
_UNSUFFIXED_CARRIER = "src/Functions/stl.hpp"


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _cmake_statements(text):
    """Whole parenthesis-balanced CMake statements, comments removed.

    A statement spans lines, so the variable name and the paths assigned to it
    are usually on different ones.
    """
    body = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )
    statements, depth, current = [], 0, []
    for char in body:
        if char == "(":
            if depth == 0:
                # The command name is the token immediately before the paren.
                current = ["".join(current).split()[-1] if current else ""]
            depth += 1
        if depth:
            current.append(char)
        if char == ")":
            depth -= 1
            if depth == 0:
                statements.append("".join(current))
                current = []
        elif depth == 0:
            current.append(char)
    return [s.strip() for s in statements]


def _dependency_statements(cmake_text):
    """The statements that build the dependency variable."""
    return [
        s
        for s in _cmake_statements(cmake_text)
        if _DEPS_VAR in s and s.split("(", 1)[0] in ("file", "list")
    ]


def _resolve_dependencies(cmake_text, repo=_REPO):
    """The dependency set as files, expanding the globs the way CMake does.

    `test_python_glob_resolution_matches_cmake` pins this against cmake itself
    where cmake is available; cmake is not installed in this job's image.
    """
    files = set()
    for statement in _dependency_statements(cmake_text):
        command = statement.split("(", 1)[0]
        paths = [
            p.replace("${ClickHouse_SOURCE_DIR}", repo)
            for p in re.findall(r'"([^"]*)"', statement)
        ]
        if command == "file":
            assert "GLOB_RECURSE" in statement, (
                "only GLOB_RECURSE is resolved here; a plain GLOB would not "
                f"descend and this resolution would be wrong: {statement}"
            )
            for pattern in paths:
                base, tail = pattern.rsplit("/", 1)
                for directory, _, names in os.walk(base):
                    files.update(
                        os.path.join(directory, name)
                        for name in names
                        if fnmatch.fnmatch(name, tail)
                    )
        else:
            files.update(p for p in paths if p.startswith(repo))
    return files


def _scanned_trees(generator_text):
    """The `$SOURCE_ROOT` paths the generator's SCANNED_TREES array names."""
    array = re.search(r"^SCANNED_TREES=\((.*?)^\)", generator_text, re.M | re.S)
    assert array, "SCANNED_TREES not found; the generator's scan set moved"
    entries = re.findall(r'"\$SOURCE_ROOT/([^"]+)"', array.group(1))
    assert entries, "SCANNED_TREES parsed empty"
    return entries


def _resolve_scanned(generator_text, repo=_REPO):
    """The generator's scan set as files.

    The generator greps the trees whole, with no `--include`, so every file
    under them is an input.
    """
    files = set()
    for entry in _scanned_trees(generator_text):
        path = os.path.join(repo, entry)
        if os.path.isdir(path):
            for directory, _, names in os.walk(path):
                files.update(os.path.join(directory, name) for name in names)
        else:
            files.add(path)
    return files


def _relative(paths):
    return sorted(os.path.relpath(p, _REPO) for p in paths)


class TestScannedSourcesMatchTheGeneratorsScanSet:
    def test_the_two_sets_are_equal(self):
        scanned = _resolve_scanned(_read(_GENERATOR))
        declared = _resolve_dependencies(_read(_CODEGEN_CMAKE))
        assert scanned, "the generator's scan set resolved empty"
        assert _relative(scanned - declared) == [], (
            "the generator reads these but an incremental build does not "
            "regenerate codegen.dict when they change"
        )
        assert _relative(declared - scanned) == [], (
            "these are dependencies of codegen.dict but the generator never "
            "reads them, so editing one rebuilds the grammar for nothing"
        )

    def test_the_generator_applies_no_suffix_filter(self):
        # The premise of comparing whole trees: with an --include the scan set
        # would be narrower than the tree and the equality above would be wrong.
        assert "--include" not in _read(_GENERATOR)

    def test_an_unsuffixed_carrier_is_a_dependency(self):
        # The concrete case: a `*.h` / `*.cpp` glob omits a `.hpp` carrier.
        carrier = os.path.join(_REPO, _UNSUFFIXED_CARRIER)
        assert os.path.exists(carrier), (
            f"{_UNSUFFIXED_CARRIER} is gone; pick another tracked non-.h/.cpp "
            "file under a scanned tree for this assertion"
        )
        assert carrier in _resolve_dependencies(_read(_CODEGEN_CMAKE))

    def test_every_path_the_generator_reads_is_a_dependency(self):
        # The scan set is not all of it: CommonParsers.h and the curated
        # old.dict are read too, and are named individually in DEPENDS.
        generator = _read(_GENERATOR)
        trees = _scanned_trees(generator)
        outside = sorted(
            {
                ref
                for ref in re.findall(r"\$SOURCE_ROOT/([A-Za-z0-9_./-]+)", generator)
                if not any(ref == t or ref.startswith(t + "/") for t in trees)
            }
        )
        assert outside, "no reads outside the scanned trees; this arm is vacuous"
        command = next(
            s
            for s in _cmake_statements(_read(_CODEGEN_CMAKE))
            if s.startswith("add_custom_command(") and "generate_source_dict.sh" in s
        )
        named = set(re.findall(r"ClickHouse_SOURCE_DIR\}/([A-Za-z0-9_./-]+)", command))
        assert [ref for ref in outside if ref not in named] == []


class TestNarrowingTheDependencySetIsDetected:
    """Mutation arms. Without them the equality above could hold vacuously."""

    @staticmethod
    def _mutate(replacement):
        text = _read(_CODEGEN_CMAKE)
        mutated = replacement(text)
        assert mutated != text, "the mutation did not change the file"
        return mutated

    def test_reverting_the_glob_to_suffixes_is_detected(self):
        # The form this list had before it was widened.
        trees = (
            "Functions",
            "AggregateFunctions",
            "TableFunctions",
            "DataTypes",
            "Formats",
        )
        widened = "".join(
            '    "${ClickHouse_SOURCE_DIR}/src/%s/*"\n' % tree for tree in trees
        )
        suffixed = "".join(
            '    "${ClickHouse_SOURCE_DIR}/src/%s/*.%s"\n' % (tree, suffix)
            for tree in trees
            for suffix in ("h", "cpp")
        )
        mutated = self._mutate(lambda t: t.replace(widened, suffixed))
        missing = _resolve_scanned(_read(_GENERATOR)) - _resolve_dependencies(mutated)
        assert missing, "narrowing the glob to *.h/*.cpp went unnoticed"
        assert os.path.join(_REPO, _UNSUFFIXED_CARRIER) in missing

    def test_dropping_a_scanned_tree_is_detected(self):
        mutated = self._mutate(
            lambda t: t.replace('    "${ClickHouse_SOURCE_DIR}/src/Formats/*"\n', "")
        )
        assert _resolve_scanned(_read(_GENERATOR)) - _resolve_dependencies(mutated)

    def test_dropping_the_individually_listed_files_is_detected(self):
        # The two files that are not trees reach the set through list(APPEND).
        mutated = self._mutate(
            lambda t: re.sub(r"list\(APPEND %s.*?\)\n" % _DEPS_VAR, "", t, flags=re.S)
        )
        missing = _resolve_scanned(_read(_GENERATOR)) - _resolve_dependencies(mutated)
        assert _relative(missing) == [
            "src/Processors/Transforms/WindowTransform.cpp",
            "src/Storages/ObjectStorage/StorageObjectStorageDefinitions.h",
        ]

    def test_a_tree_added_only_to_the_generator_is_detected(self):
        # The other direction: the generator grows a scan root and the
        # dependency set does not follow.
        generator = _read(_GENERATOR)
        mutated = generator.replace(
            '    "$SOURCE_ROOT/src/Formats"\n',
            '    "$SOURCE_ROOT/src/Formats"\n    "$SOURCE_ROOT/src/Storages/MergeTree"\n',
            1,
        )
        assert mutated != generator
        assert _resolve_scanned(mutated) - _resolve_dependencies(_read(_CODEGEN_CMAKE))


class TestTheDictionaryTracksAnUnsuffixedCarrier:
    """Run the generator, so the token-level consequence is measured.

    The assertions above are about two sets of paths. This one closes the loop:
    a name added to a carrier a suffix-filtered glob would drop does reach the
    dictionary, so omitting it from the dependencies loses a real token.
    """

    @staticmethod
    def _generate(source_root, output):
        return subprocess.run(
            [_GENERATOR, source_root, output],
            capture_output=True,
            text=True,
            check=False,
            timeout=600,
        )

    @pytest.fixture(scope="class")
    def arms(self, tmp_path_factory):
        # A copy of what the generator reads, so the repository is never
        # written to. Only the scanned trees plus the two files named
        # individually in DEPENDS are needed.
        root = tmp_path_factory.mktemp("scan_root")
        tree = root / "tree"
        for entry in _scanned_trees(_read(_GENERATOR)) + [
            "src/Parsers/CommonParsers.h",
            "tests/fuzz/dictionaries",
        ]:
            source = os.path.join(_REPO, entry)
            destination = tree / entry
            os.makedirs(destination.parent, exist_ok=True)
            if os.path.isdir(source):
                shutil.copytree(source, destination)
            else:
                shutil.copy(source, destination)

        carrier = tree / _UNSUFFIXED_CARRIER
        assert carrier.exists(), f"{_UNSUFFIXED_CARRIER} was not copied"
        control = str(root / "control.dict")
        assert self._generate(str(tree), control).returncode == 0

        # Add a token only this carrier can supply.
        with open(carrier, "a", encoding="utf-8") as f:
            f.write(
                "\nnamespace DB { struct FunctionCodegenDepsProbe "
                '{ static constexpr auto name = "codegenDepsProbeToken"; }; }\n'
            )
        treatment = str(root / "treatment.dict")
        assert self._generate(str(tree), treatment).returncode == 0
        return _read(control).splitlines(), _read(treatment).splitlines()

    def test_the_added_token_reaches_the_dictionary(self, arms):
        _, treatment = arms
        assert '"codegenDepsProbeToken"' in treatment

    def test_the_control_arm_does_not_carry_it(self, arms):
        # Without this the assertion above would pass against a generator that
        # emits the token for some other reason.
        control, treatment = arms
        assert '"codegenDepsProbeToken"' not in control
        assert len(treatment) == len(control) + 1


class TestPythonGlobResolutionIsFaithful:
    """`_resolve_dependencies` stands in for cmake, so pin it against cmake.

    cmake is not in this job's image, so this is skipped there; it still runs
    for anyone working on the resolution locally, which is when it is wrong.
    """

    @staticmethod
    def _cmake_resolve(cmake_text):
        statements = _dependency_statements(cmake_text)
        with tempfile.TemporaryDirectory() as directory:
            project = os.path.join(directory, "project")
            os.makedirs(project)
            output = os.path.join(directory, "resolved.txt")
            with open(
                os.path.join(project, "CMakeLists.txt"), "w", encoding="utf-8"
            ) as f:
                f.write("cmake_minimum_required(VERSION 3.20)\nproject(x NONE)\n")
                f.write('set(ClickHouse_SOURCE_DIR "%s")\n' % _REPO)
                for statement in statements:
                    f.write(statement + "\n")
                f.write('string(JOIN "\\n" _joined ${%s})\n' % _DEPS_VAR)
                f.write('file(WRITE "%s" "${_joined}\\n")\n' % output)
            configured = subprocess.run(
                ["cmake", "-S", project, "-B", os.path.join(directory, "build")],
                capture_output=True,
                text=True,
                check=False,
            )
            assert configured.returncode == 0, configured.stderr
            return {line for line in _read(output).splitlines() if line}

    @pytest.mark.skipif(not shutil.which("cmake"), reason="cmake is not installed")
    def test_python_glob_resolution_matches_cmake(self):
        text = _read(_CODEGEN_CMAKE)
        assert self._cmake_resolve(text) == _resolve_dependencies(text)

    @pytest.mark.skipif(not shutil.which("cmake"), reason="cmake is not installed")
    def test_it_also_matches_on_a_narrowed_list(self):
        # Agreement on one input can be luck; agreement has to survive the
        # shapes the arms above construct.
        text = _read(_CODEGEN_CMAKE).replace(
            '    "${ClickHouse_SOURCE_DIR}/src/Formats/*"\n', ""
        )
        assert self._cmake_resolve(text) == _resolve_dependencies(text)


class TestCodegenCMakeIsInTheJobDigest:
    """This suite reads the codegen CMakeLists, so `CI Tests` must hash it.

    `CI Tests` is cache-gated: a commit that only narrows the dependency list
    takes a cache hit from an older run and the assertions above never execute
    on the commit that broke them - which is exactly the regression they exist
    to catch.
    """

    @staticmethod
    def _include_paths():
        from ci.defs.job_configs import JobConfigs

        return JobConfigs.ci_tests.digest_config.include_paths

    @staticmethod
    def _uncovered(path, include_paths):
        normalized = [os.path.normpath(p.removeprefix("./")) for p in include_paths]
        return not any(
            path == entry or path.startswith(entry + os.sep) for entry in normalized
        )

    def test_the_codegen_cmake_is_covered(self):
        relative = os.path.relpath(_CODEGEN_CMAKE, _REPO)
        assert not self._uncovered(relative, self._include_paths()), (
            f"{relative} is read by this suite but is not in the CI Tests "
            "digest, so narrowing it would take a cache hit"
        )

    def test_dropping_the_entry_uncovers_it(self):
        # Mutation arm: the assertion above must be able to fail.
        relative = os.path.relpath(_CODEGEN_CMAKE, _REPO)
        include_paths = self._include_paths()
        reduced = [p for p in include_paths if self._uncovered(relative, [p])]
        assert len(reduced) < len(include_paths), (
            "no entry covers the codegen CMakeLists, so this arm has nothing "
            f"to remove: {include_paths}"
        )
        assert self._uncovered(relative, reduced)
