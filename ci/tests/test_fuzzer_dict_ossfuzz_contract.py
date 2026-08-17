"""
Regression tests for the OSS-Fuzz fuzzer-dictionary contract.

OSS-Fuzz (google/oss-fuzz projects/clickhouse/build.sh) does not use
tests/fuzz/build.sh. It configures with -DENABLE_FUZZING=1, builds each fuzzer
individually (`ninja -j N $FUZZER_TARGET`), and then, under `#!/bin/bash -eu`,
runs `cp $SRC/ClickHouse/tests/fuzz/*.dict $OUT/`. Two consequences pin the
design these tests protect:

  - the copy reads the *source* tree, not the build output, so staging a
    dictionary into $OUT does not satisfy it;
  - `fuzzers` is an aggregate target that a single-target build never reaches,
    so its POST_BUILD step cannot be what produces the dictionary.

all.dict is therefore generated at *configure* time, into the source tree,
where a per-target build and the OSS-Fuzz glob both find it. Under `-eu` an
unmatched glob aborts the whole build, so losing that wiring breaks the
official OSS-Fuzz build rather than merely degrading a dictionary.

Each test carries a mutation arm: the assertion has to fail when the wiring it
describes is removed, otherwise it would pass against a tree that has lost it.
"""

import os
import re
import subprocess
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` does `from praktika import ...`, so `ci/` itself must be on the
# path for `import praktika` to resolve.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
_ROOT_CMAKE = os.path.join(_REPO, "CMakeLists.txt")
_BUILD_SH = os.path.join(_REPO, "tests/fuzz/build.sh")
_GENERATOR = os.path.join(_REPO, "tests/fuzz/generate_source_dict.sh")
_GITIGNORE = os.path.join(_REPO, ".gitignore")

# What OSS-Fuzz's .options files ask libFuzzer to load.
_DICT_NAME = "all.dict"


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _fuzzer_block(cmake_text):
    """The root CMakeLists.txt `if (FUZZER)` block, verbatim.

    Scoping the assertions to this block is what makes them meaningful: the
    generation must be reached by a fuzzing configure, not merely mentioned
    somewhere in a 1000-line file.
    """
    lines = cmake_text.splitlines(True)
    start = next(i for i, l in enumerate(lines) if l.startswith("if (FUZZER)"))
    end = next(i for i, l in enumerate(lines[start:], start) if l.startswith("endif()"))
    return "".join(lines[start:end])


def _generation_commands(block):
    """Every command in the block that runs the source-dictionary generator."""
    return [l.strip() for l in block.splitlines() if "generate_source_dict.sh" in l]


class TestConfigureTimeGeneration:
    """The dictionary must be produced by configure, not by a build step."""

    def test_generator_runs_at_configure_time(self):
        # execute_process runs while CMake is configuring, so the dictionary
        # exists before ninja is invoked for any target.
        block = _fuzzer_block(_read(_ROOT_CMAKE))
        generation = _generation_commands(block)
        assert generation, (
            "the root CMakeLists.txt `if (FUZZER)` block must generate the fuzzer "
            "dictionary; without it OSS-Fuzz's `cp tests/fuzz/*.dict` aborts"
        )
        assert re.search(
            r"execute_process\s*\(\s*COMMAND[^)]*generate_source_dict\.sh", block, re.S
        ), "the generator must run via execute_process (configure time), not as a build step"

    def test_generation_is_not_attached_to_the_fuzzers_aggregate_target(self):
        # A POST_BUILD step of `fuzzers` never fires for `ninja <one>_fuzzer`,
        # which is the only way OSS-Fuzz builds.
        block = _fuzzer_block(_read(_ROOT_CMAKE))
        for command in _generation_commands(block):
            assert "add_custom_command" not in command, (
                "the dictionary generation must not hang off the `fuzzers` target: "
                "OSS-Fuzz builds targets individually and would never run it"
            )
            assert "POST_BUILD" not in command

    def test_writes_into_the_source_tree(self):
        # OSS-Fuzz copies $SRC/ClickHouse/tests/fuzz/*.dict; a build-directory
        # output would leave that glob unmatched.
        block = _fuzzer_block(_read(_ROOT_CMAKE))
        generation = _generation_commands(block)
        assert any(
            f"CMAKE_SOURCE_DIR}}/tests/fuzz/{_DICT_NAME}" in c for c in generation
        ), f"the dictionary must be written to the source tree's tests/fuzz/{_DICT_NAME}"
        assert not any("CMAKE_BINARY_DIR}/tests/fuzz" in c for c in generation)

    def test_failure_to_generate_is_fatal(self):
        # An empty or missing dictionary surfaces far away, as an unmatched glob
        # inside OSS-Fuzz's build script, so fail where the cause is.
        block = _fuzzer_block(_read(_ROOT_CMAKE))
        assert re.search(
            r"if\s*\(NOT\s+\w+\s+EQUAL\s+0\)", block
        ), "a failed generation must abort the configure"
        assert "FATAL_ERROR" in block


class TestBuildShConsumesRatherThanRegenerates:
    """tests/fuzz/build.sh must copy the current dictionary, not make a second one."""

    def test_copies_the_dict_glob(self):
        assert re.search(
            r"^cp \$SRC/tests/fuzz/\*\.dict \$OUT/$", _read(_BUILD_SH), re.M
        ), "build.sh must stage tests/fuzz/*.dict into $OUT for the .options files"

    def test_does_not_generate_its_own_copy(self):
        # update_dict.sh overwrites the source-derived fallback with the
        # binary-derived dictionary; regenerating here would discard it.
        assert "generate_source_dict.sh" not in _read(_BUILD_SH)


class TestDictionaryIsGeneratedNotCommitted:
    def test_dict_is_ignored(self):
        assert f"/tests/fuzz/{_DICT_NAME}" in _read(_GITIGNORE), (
            "the generated dictionary must be ignored so a fuzzing configure "
            "does not dirty the working tree"
        )

    def test_dict_is_not_committed(self):
        tracked = subprocess.run(
            ["git", "ls-files", f"tests/fuzz/{_DICT_NAME}"],
            cwd=_REPO,
            capture_output=True,
            text=True,
            check=False,
        )
        assert tracked.stdout.strip() == "", (
            "all.dict must not be committed: it is derived from the sources or "
            "from a release binary"
        )

    def test_options_files_reference_the_generated_dict(self):
        # These are the consumers that make the glob load-bearing.
        options_dir = os.path.join(_REPO, "tests/fuzz")
        referencing = [
            name
            for name in sorted(os.listdir(options_dir))
            if name.endswith(".options")
            and f"dict = {_DICT_NAME}" in _read(os.path.join(options_dir, name))
        ]
        assert referencing, (
            "no .options file references the dictionary; if that is intentional "
            "the generation and this contract can go too"
        )


class TestGeneratorProducesAUsableDictionary:
    """The generator is the whole mechanism, so exercise it for real."""

    @pytest.fixture(scope="class")
    def generated(self, tmp_path_factory):
        out = tmp_path_factory.mktemp("dict") / _DICT_NAME
        subprocess.run(
            [_GENERATOR, _REPO, str(out)],
            check=True,
            capture_output=True,
            text=True,
            timeout=600,
        )
        return out

    def test_output_is_non_empty(self, generated):
        assert generated.stat().st_size > 0
        assert len(generated.read_text(encoding="utf-8").splitlines()) > 100

    def test_output_is_libfuzzer_dictionary_format(self, generated):
        # libFuzzer needs one quoted token per line; an unquoted line makes it
        # reject the whole dictionary.
        bad = [
            line
            for line in generated.read_text(encoding="utf-8").splitlines()
            if line and not re.fullmatch(r'".*"', line)
        ]
        assert bad == [], f"non-dictionary lines: {bad[:5]}"

    def test_satisfies_the_ossfuzz_copy(self, generated, tmp_path):
        # The end-to-end assertion: OSS-Fuzz's own command, under the same
        # `-eu` it runs with.
        src = tmp_path / "tests" / "fuzz"
        src.mkdir(parents=True)
        (src / _DICT_NAME).write_bytes(generated.read_bytes())
        out = tmp_path / "out"
        out.mkdir()
        copied = subprocess.run(
            ["bash", "-eu", "-c", f"cp {src}/*.dict {out}/"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert copied.returncode == 0, copied.stderr

    def test_ossfuzz_copy_fails_without_the_dictionary(self, tmp_path):
        # Mutation arm for the test above: with no dictionary the same command
        # must abort, which is exactly the OSS-Fuzz breakage being fixed. If
        # both arms passed, the assertion above would prove nothing.
        src = tmp_path / "tests" / "fuzz"
        src.mkdir(parents=True)
        (src / "lexer_fuzzer.options").write_text("[libfuzzer]\n")
        out = tmp_path / "out"
        out.mkdir()
        copied = subprocess.run(
            ["bash", "-eu", "-c", f"cp {src}/*.dict {out}/"],
            capture_output=True,
            text=True,
            check=False,
        )
        assert copied.returncode != 0
        assert list(out.iterdir()) == []


class TestSuiteInputsAreInTheJobDigest:
    """`CI Tests` is cache-gated, so an input outside its digest is unguarded.

    A commit touching only such a file takes a cache hit from an older run, and the
    suite above never executes on the commit that broke it.
    """

    # From the module's own constants, so a new input is covered automatically.
    _READS = [_ROOT_CMAKE, _BUILD_SH, _GENERATOR, _GITIGNORE]

    @staticmethod
    def _include_paths():
        from ci.defs.job_configs import JobConfigs

        return JobConfigs.ci_tests.digest_config.include_paths

    @staticmethod
    def _uncovered(paths, include_paths):
        normalized = [
            os.path.normpath(os.path.join(_REPO, p.removeprefix("./")))
            for p in include_paths
        ]
        return [
            os.path.relpath(path, _REPO)
            for path in paths
            if not any(
                path == entry or path.startswith(entry + os.sep) for entry in normalized
            )
        ]

    def _all_reads(self):
        fuzz_dir = os.path.join(_REPO, "tests/fuzz")
        return self._READS + [
            os.path.join(_REPO, "tests/fuzz/dictionaries/old.dict"),
            *(
                os.path.join(fuzz_dir, name)
                for name in sorted(os.listdir(fuzz_dir))
                if name.endswith(".options")
            ),
        ]

    def test_every_input_this_suite_reads_is_covered(self):
        reads = self._all_reads()
        assert len(reads) > len(self._READS), "the .options glob found nothing"
        assert self._uncovered(reads, self._include_paths()) == []

    def test_dropping_the_fuzz_directory_uncovers_them(self):
        # Mutation arm: without it the assertion above would pass against a
        # digest that had lost the coverage.
        include_paths = self._include_paths()
        reduced = [p for p in include_paths if p != "./tests/fuzz/"]
        assert len(reduced) < len(include_paths), (
            "'./tests/fuzz/' is not in include_paths, so this arm has nothing to "
            f"remove and cannot prove anything: {include_paths}"
        )
        assert self._uncovered(self._all_reads(), reduced) != []
