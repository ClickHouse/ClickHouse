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


def _generation_statements(block):
    """Whole CMake statements naming the generator, not just the matching line.

    A statement spans lines, so the command that names the generator and the
    `add_custom_command(... POST_BUILD` that introduces it are usually on
    different ones. Matching per line cannot see the difference.
    """
    # Comments cannot open a statement and would otherwise be glued onto the
    # front of the next one.
    text = "\n".join(
        line for line in block.splitlines() if not line.lstrip().startswith("#")
    )
    statements, depth, current = [], 0, []
    for char in text:
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
    return [s.strip() for s in statements if "generate_source_dict.sh" in s]


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
        # which is the only way OSS-Fuzz builds. Checked per statement, and over
        # all of them, so neither a multi-line form nor a build-step copy added
        # beside the configure-time one passes.
        statements = _generation_statements(_fuzzer_block(_read(_ROOT_CMAKE)))
        assert len(statements) == 1, (
            "the generator must be invoked exactly once in the `if (FUZZER)` "
            f"block, at configure time: {statements}"
        )
        assert statements[0].startswith("execute_process("), statements[0]
        for statement in statements:
            assert "add_custom_command" not in statement, (
                "the dictionary generation must not hang off the `fuzzers` target: "
                "OSS-Fuzz builds targets individually and would never run it"
            )
            assert "POST_BUILD" not in statement

    @pytest.mark.parametrize(
        "form",
        [
            'add_custom_command(TARGET fuzzers POST_BUILD\n    COMMAND "$'
            '{CMAKE_SOURCE_DIR}/tests/fuzz/generate_source_dict.sh" a b\n    VERBATIM)',
            "add_custom_command(TARGET fuzzers POST_BUILD COMMAND "
            "${CMAKE_SOURCE_DIR}/tests/fuzz/generate_source_dict.sh a b VERBATIM)",
        ],
        ids=["multi-line", "single-line"],
    )
    def test_a_build_step_generator_is_rejected(self, form):
        # Mutation arm. The multi-line case is the one a per-line check misses:
        # POST_BUILD and the generator name sit on different lines.
        block = _fuzzer_block(_read(_ROOT_CMAKE)) + "\n" + form + "\n"
        statements = _generation_statements(block)
        assert len(statements) == 2, statements
        assert any("POST_BUILD" in s for s in statements)

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

    @staticmethod
    def _dictionary_commands(body):
        """Non-comment lines that touch a dictionary file or a generator."""
        return [
            line.strip()
            for line in body.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
            if ".dict" in line or "_dict.sh" in line
        ]

    def test_only_copies_the_dictionary(self):
        # Any write here replaces the binary-derived dictionary the nightly job
        # installed, whichever generator produces it, so the contract is
        # positive: one copy into $OUT and nothing else.
        touching = self._dictionary_commands(_read(_BUILD_SH))
        assert touching == ["cp $SRC/tests/fuzz/*.dict $OUT/"], (
            "build.sh must only copy the current dictionary into $OUT, never "
            f"regenerate or write one: {touching}"
        )

    @pytest.mark.parametrize(
        "regeneration",
        [
            "bash $SRC/tests/fuzz/update_dict.sh",
            "$SRC/tests/fuzz/generate_source_dict.sh $SRC $SRC/tests/fuzz/all.dict",
            'clickhouse local -q "SELECT name FROM system.functions" > '
            "$SRC/tests/fuzz/all.dict",
        ],
        ids=["binary-derived", "source-derived", "inlined"],
    )
    def test_a_regenerating_build_sh_is_rejected(self, regeneration):
        # Mutation arm, one per way the regression can come back: naming no
        # single script is not the property being asserted.
        mutated = _read(_BUILD_SH).replace(
            "cp $SRC/tests/fuzz/*.dict $OUT/",
            f"{regeneration}\ncp $SRC/tests/fuzz/*.dict $OUT/",
        )
        assert self._dictionary_commands(mutated) != ["cp $SRC/tests/fuzz/*.dict $OUT/"]


class TestGeneratorRejectsUnsupportedShellTooling:
    """The generator is run by configure, so it must fail loudly, not quietly.

    It needs `mapfile` and GNU `grep -z`; without them the extraction yields a
    short dictionary, which is not a configure error and would only surface much
    later as the nightly coverage check.
    """

    def test_shebang_resolves_bash_through_path(self):
        # /bin/bash is 3.2 on macOS, so pinning it defeats an installed newer
        # bash that is ahead on PATH.
        assert _read(_GENERATOR).startswith("#!/usr/bin/env bash\n")

    def test_both_prerequisites_are_checked_before_first_use(self):
        lines = _read(_GENERATOR).splitlines()

        def first(predicate):
            return next(i for i, line in enumerate(lines) if predicate(line))

        bash_guard = first(lambda l: "BASH_VERSINFO" in l)
        grep_guard = first(lambda l: "grep -qzE" in l)
        assert bash_guard < first(lambda l: l.startswith("mapfile "))
        assert grep_guard < first(lambda l: "grep -rhoz" in l)

    def test_a_shell_without_gnu_grep_is_rejected(self, tmp_path):
        # Mutation arm, run rather than read: a grep rejecting -z stands in for
        # BSD grep, and the generator must exit non-zero instead of proceeding.
        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        fake_grep = fake_bin / "grep"
        fake_grep.write_text(
            "#!/usr/bin/env bash\n"
            'for a in "$@"; do case "$a" in -*z*) exit 2;; esac; done\n'
            'exec /usr/bin/grep "$@"\n'
        )
        fake_grep.chmod(0o755)
        env = dict(os.environ, PATH=f"{fake_bin}:{os.environ['PATH']}")
        result = subprocess.run(
            ["bash", _GENERATOR, _REPO, str(tmp_path / "out.dict")],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        assert result.returncode != 0
        assert "GNU grep" in result.stderr
        assert not (tmp_path / "out.dict").exists()


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


class TestFuzzersBuildDigestCoversItsOwnInputs:
    """`Build (arm_fuzzers)` stages inputs the shared build digest does not cover.

    Its POST_BUILD step runs tests/fuzz/build.sh, which packs the .options files,
    the dictionary and seed corpora repacked from tests/queries/0_stateless into
    the artifact. Without those paths in the digest, a commit changing the
    dictionary wiring or the corpus takes a cache hit and every consumer -
    NightlyFuzzers included - reuses a stale ARM_FUZZERS artifact.
    """

    _REQUIRED = ["./tests/fuzz/", "./tests/queries/0_stateless/"]

    @staticmethod
    def _fuzzers_job():
        from ci.defs.defs import BuildTypes
        from ci.defs.job_configs import JobConfigs

        return next(
            j
            for j in JobConfigs.special_build_jobs
            if j.parameter == BuildTypes.ARM_FUZZERS
        )

    def test_staged_inputs_are_in_the_digest(self):
        include_paths = self._fuzzers_job().digest_config.include_paths
        missing = [p for p in self._REQUIRED if p not in include_paths]
        assert (
            missing == []
        ), f"Build (arm_fuzzers) stages these but does not hash them: {missing}"

    def test_the_shared_build_digest_does_not_already_cover_them(self):
        # Negative control: the assertion above holds trivially if the shared
        # digest carries these paths, so the per-job widening proves nothing.
        from ci.defs.job_configs import build_digest_config

        overlap = [p for p in self._REQUIRED if p in build_digest_config.include_paths]
        assert overlap == [], (
            "the shared build digest now covers these, so the widening above is "
            f"redundant and this guard proves nothing: {overlap}"
        )

    def test_submodule_hashing_is_preserved(self):
        # Replacing the digest config drops fields that are not carried over,
        # and the fuzzers build needs contrib pinned like every other build.
        assert self._fuzzers_job().digest_config.with_git_submodules

    def test_only_the_fuzzers_build_is_widened(self):
        # The widening is per-job on purpose: adding these to the shared digest
        # would invalidate every unrelated build on a corpus change.
        from ci.defs.defs import BuildTypes
        from ci.defs.job_configs import JobConfigs

        widened = [
            j.parameter
            for j in JobConfigs.special_build_jobs
            if any(p in j.digest_config.include_paths for p in self._REQUIRED)
        ]
        assert widened == [BuildTypes.ARM_FUZZERS], widened
