"""
Tests for `Utils.traverse_paths` exclude semantics
(`ci/praktika/utils.py`).

`traverse_paths` collects the files a job digest is computed over
(`Digest.calc_job_digest`) and the files the style check walks. Its
include paths may be globs, and the workflow's own example config writes
glob excludes (`./**/*.pyc` in `ci/praktika/cache.py`), so an exclude must
be matched the way the other digest-path matchers in this codebase match
(`Job.CacheDigestConfig.is_excluded`, `workflow_hooks/filter_job.py`): an
exact path, a directory prefix on a path boundary, or a glob. Plain
`startswith` matching silently ignores every glob entry and also swallows
sibling directories that merely share a name prefix.

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


class TestNotExistsOk:
    def test_missing_include_is_empty_not_an_assert(self, tree):
        assert (
            Utils.traverse_paths(["./does_not_exist"], [], not_exists_ok=True) == []
        )

    def test_missing_include_still_asserts_by_default(self, tree):
        with pytest.raises(AssertionError):
            Utils.traverse_paths(["./does_not_exist"], [])
