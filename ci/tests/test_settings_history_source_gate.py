"""
Tests for the source-file gate of the `settings_changes_history` style check in
`ci/jobs/check_style.py`.

A change that touches no C++ source besides `src/Core/SettingsChangesHistory.cpp` cannot have
changed any setting's compiled default, so it is a historical correction and must not be forced
into the current version block. As soon as any other source file changed, the rule is enforced -
including for files that are not the setting declarations themselves, because a default can come
from a constant defined elsewhere (`src/Core/Defines.h` and friends).
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `praktika` is imported as a top-level module by the style-check job.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs import check_style


REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
HISTORY = "src/Core/SettingsChangesHistory.cpp"


class _FakeInfo:
    def __init__(self, kv):
        self._kv = kv

    def __call__(self):
        return self

    def get_kv_data(self):
        return self._kv


def _run(monkeypatch, kv):
    monkeypatch.setattr(check_style, "Info", _FakeInfo(kv))
    monkeypatch.chdir(REPO_ROOT)
    return check_style.check_settings_changes_history()


def _kv(changed_files, changed_settings):
    return {
        "changed_files": changed_files,
        "settings_history_changed_settings": changed_settings,
    }


def test_history_only_change_is_a_correction_and_is_allowed(monkeypatch):
    # Only the history file changed: no compiled default can have changed here.
    kv = _kv([HISTORY], [{"namespace": "Session", "name": "no_such_setting_at_all"}])
    assert _run(monkeypatch, kv) == ""


def test_history_plus_unrelated_non_source_change_is_still_a_correction(monkeypatch):
    kv = _kv(
        [HISTORY, "docs/en/operations/settings/settings.md", "ci/jobs/check_style.py"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    assert _run(monkeypatch, kv) == ""


def test_default_defined_by_a_constant_outside_the_declaration_files_is_enforced(
    monkeypatch,
):
    # src/Core/Defines.h holds constants such as `DEFAULT_INSERT_BLOCK_SIZE` that several
    # settings use as their default, so a change there must not bypass the current-block rule.
    kv = _kv(
        [HISTORY, "src/Core/Defines.h"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    error = _run(monkeypatch, kv)
    assert "no_such_setting_at_all" in error


def test_build_definition_config_template_is_enforced(monkeypatch):
    # Compile definitions such as `CLICKHOUSE_CLOUD` and `ENABLE_DISTRIBUTED_CACHE` select
    # settings defaults at build time; they are carried by config templates like
    # src/Common/config.h.in, so a change there must not bypass the current-block rule.
    kv = _kv(
        [HISTORY, "src/Common/config.h.in"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    assert "no_such_setting_at_all" in _run(monkeypatch, kv)


def test_build_definition_cmake_files_are_enforced(monkeypatch):
    for build_file in ("CMakeLists.txt", "src/CMakeLists.txt", "cmake/limit_jobs.cmake"):
        kv = _kv(
            [HISTORY, build_file],
            [{"namespace": "Session", "name": "no_such_setting_at_all"}],
        )
        assert "no_such_setting_at_all" in _run(monkeypatch, kv), build_file


def test_declaration_file_change_is_enforced(monkeypatch):
    kv = _kv(
        [HISTORY, "src/Core/Settings.cpp"],
        [{"namespace": "Session", "name": "no_such_setting_at_all"}],
    )
    assert "no_such_setting_at_all" in _run(monkeypatch, kv)


def test_no_history_change_at_all_is_skipped(monkeypatch):
    assert _run(monkeypatch, _kv(["src/Core/Settings.cpp"], [])) == ""
