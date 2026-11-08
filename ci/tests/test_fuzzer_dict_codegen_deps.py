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

# The chain from the scanned sources to the grammar the fuzzer embeds: each of
# these is produced by an add_custom_command that depends on the previous one.
_DICT = "codegen.dict"
_GRAMMAR = "clickhouse.g"
_GENERATED_SOURCE = "out.cpp"

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


# The keywords that end one argument section of add_custom_command and begin
# the next.
_SECTION_KEYWORDS = (
    "OUTPUT",
    "COMMAND",
    "DEPENDS",
    "COMMENT",
    "VERBATIM",
    "WORKING_DIRECTORY",
    "MAIN_DEPENDENCY",
    "IMPLICIT_DEPENDS",
    "BYPRODUCTS",
)


def _section(statement, keyword):
    """The arguments of one section, so OUTPUT is not read as far as COMMAND."""
    found = re.search(r"\b%s\b(.*)$" % keyword, statement, re.S)
    if not found:
        return []
    others = "|".join(k for k in _SECTION_KEYWORDS if k != keyword)
    tail = re.split(r"\b(?:%s)\b" % others, found.group(1))[0]
    return [token for token in tail.replace(")", " ").split() if token]


def _custom_command(cmake_text, output):
    """The add_custom_command statement producing `output`."""
    matching = [
        s
        for s in _cmake_statements(cmake_text)
        if s.startswith("add_custom_command(")
        and any(output in arg for arg in _section(s, "OUTPUT"))
    ]
    assert len(matching) == 1, f"expected one command producing {output}: {matching}"
    return matching[0]


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
        # From the DEPENDS section, so naming a file in COMMAND does not count:
        # only DEPENDS makes a file trigger a rebuild.
        depends = " ".join(
            _section(_custom_command(_read(_CODEGEN_CMAKE), _DICT), "DEPENDS")
        )
        named = set(re.findall(r"ClickHouse_SOURCE_DIR\}/([A-Za-z0-9_./-]+)", depends))
        assert [ref for ref in outside if ref not in named] == []


class TestTheDependencySetReachesTheGrammar:
    """A correct dependency set is inert unless the rebuild chain consumes it.

    Populating the variable and wiring it up are separate: the assertions above
    hold with the variable never referenced by any command, which is a state
    where an edited carrier regenerates nothing.
    """

    # Each output and a dependency that must reach it. The data edges carry the
    # sources through to the grammar; the script edges are what make an edit to
    # a generator rerun the step it drives.
    _EDGES = (
        (_DICT, "${%s}" % _DEPS_VAR),
        (_GRAMMAR, _DICT),
        (_GENERATED_SOURCE, _GRAMMAR),
        (_GRAMMAR, "update.sh"),
        (_GRAMMAR, "clickhouse-template.g"),
        (_GENERATED_SOURCE, "gen.py"),
    )

    @pytest.mark.parametrize("output,dependency", _EDGES, ids=lambda v: v.strip("${}"))
    def test_the_edge_is_declared(self, output, dependency):
        depends = _section(_custom_command(_read(_CODEGEN_CMAKE), output), "DEPENDS")
        assert any(dependency in argument for argument in depends), (
            f"the command producing {output} does not depend on {dependency}, so "
            "an incremental build reuses a stale one"
        )

    @pytest.mark.parametrize("output,dependency", _EDGES, ids=lambda v: v.strip("${}"))
    def test_removing_the_edge_is_detected(self, output, dependency):
        # Mutation arm, one per edge: the assertion above must be able to fail.
        text = _read(_CODEGEN_CMAKE)
        statement = _custom_command(text, output)
        stripped = "\n".join(
            line for line in statement.splitlines() if dependency not in line
        )
        assert stripped != statement, f"{dependency} is not on a line of its own"
        mutated = text.replace(statement, stripped)
        depends = _section(_custom_command(mutated, output), "DEPENDS")
        assert not any(dependency in argument for argument in depends)

    def test_the_generator_is_a_dependency_of_the_dictionary(self):
        # The script is an input like the sources it reads: editing an
        # extraction pass must regenerate the dictionary too.
        depends = " ".join(
            _section(_custom_command(_read(_CODEGEN_CMAKE), _DICT), "DEPENDS")
        )
        assert os.path.relpath(_GENERATOR, _REPO) in depends

    def test_the_grammar_is_built_from_the_generated_dictionary(self):
        # The command has to consume the generated dictionary, not the
        # binary-derived all.dict, which does not exist at build time.
        command = " ".join(
            _section(_custom_command(_read(_CODEGEN_CMAKE), _GRAMMAR), "COMMAND")
        )
        assert _DICT in command
        assert "all.dict" not in command

    @pytest.mark.parametrize(
        "output,script",
        [
            (_GRAMMAR, "update.sh"),
            (_GRAMMAR, "clickhouse-template.g"),
            (_GENERATED_SOURCE, "gen.py"),
        ],
    )
    def test_a_copied_script_is_depended_on_where_it_is_run(self, output, script):
        # These are configure_file'd into the binary directory and the commands
        # run there, so the copy is the file the build compares timestamps
        # against; depending on the source path would not rerun the step.
        text = _read(_CODEGEN_CMAKE)
        assert re.search(
            r'configure_file\(\s*"\$\{CURRENT_DIR_IN_SOURCES\}/%s"\s*'
            r'"\$\{CURRENT_DIR_IN_BINARY\}/%s"'
            % (re.escape(script), re.escape(script)),
            text,
        ), f"{script} is no longer copied into the binary directory"
        depends = _section(_custom_command(text, output), "DEPENDS")
        assert any(
            "${CURRENT_DIR_IN_BINARY}/%s" % script == argument.strip('"')
            for argument in depends
        ), f"the command producing {output} must depend on the copy of {script}"


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
