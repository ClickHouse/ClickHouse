from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    s = p.read_text()
    if new in s:
        return
    count = s.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one anchor, found {count}: {old[:120]!r}")
    p.write_text(s.replace(old, new, 1))


replace_once(
    "src/Core/Settings.cpp",
    '''    DECLARE(String, workload, "default", R"(
Name of workload to be used to access resources
)", 0) \\
    DECLARE(Milliseconds, workload_admission_timeout_ms, 0, R"(''',
    '''    DECLARE(String, workload, "default", R"(
Name of workload to be used to access resources
)", 0) \\
    DECLARE(String, refresh_workload, "", R"(
Workload used for refreshable materialized view admission and execution. Specify it in the stored SELECT settings. A non-empty value overrides `workload` for the complete refresh, including its SELECT and internal operations, without changing the workload of the CREATE or ALTER query.

An empty value preserves the existing `workload` behavior. This setting has no effect on ordinary query execution. QUERY admission for refreshes also requires `use_query_slot_to_refresh_materialized_view` and a configured QUERY resource.
)", 0) \\
    DECLARE(Milliseconds, workload_admission_timeout_ms, 0, R"(''',
)

history = Path("src/Core/SettingsChangesHistory.cpp")
s = history.read_text()
entry = '            {"refresh_workload", "", "", "New setting to choose the workload for refreshable materialized view execution independently of the creating query."},\n'
if entry not in s:
    marker = '        addSettingsChanges(settings_changes_history, "26.9",\n        {\n'
    count = s.count(marker)
    if count != 1:
        raise RuntimeError(f"SettingsChangesHistory.cpp: expected one 26.9 anchor, found {count}")
    s = s.replace(marker, marker + entry, 1)
    history.write_text(s)
