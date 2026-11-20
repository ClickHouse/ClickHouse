"""
Tests for `Utils.traverse_paths` exclude semantics
(`ci/praktika/utils.py`).

`traverse_paths` collects the files a job digest is computed over
(`Digest.calc_job_digest`) and the files the style check walks. Its
include paths may be globs, and the workflow's own example config writes
glob excludes (`./**/*.pyc` in `ci/praktika/cache.py`), so an exclude must
be matched the way the other digest-path matchers in this codebase match
(`Job.CacheDigestConfig.is_excluded`, `Job.Config.is_affected_by`): an
exact path, a directory prefix on a path boundary, or a glob. Plain
`startswith` matching silently ignores every glob entry and also swallows
sibling directories that merely share a name prefix.

Glob matching uses `PurePosixPath.full_match`, so `**` spans any number of
directories while a plain `*` stays within a single path segment: an
exclude never reaches past the segment it names and so never drops an
included file that the pattern does not actually cover.

`not_exists_ok` must reach `traverse_path`: a caller that passes it has
already decided a missing include path is not an error.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.utils import Utils


@pytest.fixture
def tree(tmp_path, monkeypatch):
    inc = tmp_path / "inc"
    (inc / "sub").mkdir(parents=True)
    (inc / "sub2").mkdir()
    for name in ("a.py", "b.pyc"):
        (inc / name).touch()
    (inc / "sub" / "c.pyc").touch()
    (inc / "sub" / "d.py").touch()
    (inc / "sub2" / "e.py").touch()
    monkeypatch.chdir(tmp_path)
    return tmp_path


class TestExcludeMatching:
    def test_glob_exclude_matches_at_any_depth(self, tree):
        res = Utils.traverse_paths(["./inc"], ["./**/*.pyc"])
        assert sorted(res) == ["./inc/a.py", "./inc/sub/d.py", "./inc/sub2/e.py"]

    def test_glob_exclude_is_anchored_like_a_path(self, tree):
        res = Utils.traverse_paths(["./inc"], ["./inc/*.pyc"])
        assert "./inc/b.pyc" not in res
        assert "./inc/sub/c.pyc" in res

    def test_literal_directory_exclude_still_works(self, tree):
        res = Utils.traverse_paths(["./inc"], ["./inc/sub"])
        assert sorted(res) == ["./inc/a.py", "./inc/b.pyc", "./inc/sub2/e.py"]

    def test_directory_exclude_does_not_swallow_a_name_prefix_sibling(self, tree):
        res = Utils.traverse_paths(["./inc"], ["./inc/sub"])
        assert "./inc/sub2/e.py" in res

    def test_exact_file_exclude(self, tree):
        res = Utils.traverse_paths(["./inc"], ["./inc/a.py"])
        assert "./inc/a.py" not in res
        assert "./inc/sub/d.py" in res


@pytest.fixture
def generated_tree(tmp_path, monkeypatch):
    # inc/a/generated/foo.py         -> ** nonempty, *.py one segment
    # inc/a/generated/nested/live.py -> *.py must NOT cross the `nested` dir
    # inc/generated/cache.py         -> leading ** matches zero directories
    # inc/generated/a/b/cache.py     -> trailing ** matches many directories
    files = [
        "inc/a/generated/foo.py",
        "inc/a/generated/nested/live.py",
        "inc/generated/cache.py",
        "inc/generated/a/b/cache.py",
    ]
    for rel in files:
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.touch()
    monkeypatch.chdir(tmp_path)
    return tmp_path


class TestRecursiveGlobExcludeDoesNotOverreach:
    def test_trailing_wildcard_stays_within_one_segment(self, generated_tree):
        # `*.py` must match only files directly under a `generated` dir, so the
        # deeper `nested/live.py` stays included - dropping it would leave the
        # job digest unchanged after editing a file that should invalidate it.
        res = Utils.traverse_paths(["./inc"], ["./inc/**/generated/*.py"])
        assert "./inc/a/generated/foo.py" not in res
        assert "./inc/a/generated/nested/live.py" in res

    def test_double_star_matches_zero_and_many_directories(self, generated_tree):
        # Both `**` components must be independently satisfiable: the leading one
        # by zero dirs, the trailing one by several.
        res = Utils.traverse_paths(["./inc"], ["./inc/**/generated/**/cache.py"])
        assert "./inc/generated/cache.py" not in res
        assert "./inc/generated/a/b/cache.py" not in res


class TestNotExistsOk:
    def test_missing_include_is_empty_not_an_assert(self, tree):
        assert (
            Utils.traverse_paths(["./does_not_exist"], [], not_exists_ok=True) == []
        )

    def test_missing_include_still_asserts_by_default(self, tree):
        with pytest.raises(AssertionError):
            Utils.traverse_paths(["./does_not_exist"], [])
