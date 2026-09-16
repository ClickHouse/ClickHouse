#!/usr/bin/env bash
set -euo pipefail

git remote add upstream https://github.com/ClickHouse/ClickHouse.git
git fetch --no-tags --depth=700 upstream master
git merge --no-commit --no-ff -X theirs upstream/master

python3 - <<'PY'
from pathlib import Path


def ensure_before(path, marker, anchor, insertion):
    p = Path(path)
    text = p.read_text()
    if marker in text:
        return
    if text.count(anchor) != 1:
        raise SystemExit(f"expected one anchor in {path}, got {text.count(anchor)}")
    p.write_text(text.replace(anchor, insertion + anchor, 1))


ensure_before(
    "src/Core/ServerSettings.cpp",
    "use_query_slot_to_refresh_materialized_view",
    '    DECLARE(Bool, cpu_slot_preemption, true, R"(\n',
    '''    DECLARE(Bool, use_query_slot_to_refresh_materialized_view, false, R"(
When enabled, refreshable materialized views request a query slot before starting refresh execution. The stored SELECT setting `refresh_workload` selects the refresh workload; when empty, the existing `workload` setting is used. This requires a configured QUERY resource for that workload. While queued, the refresh reports `WaitingForResource` in `system.view_refreshes` without occupying a background execution worker.

Disabled by default for compatibility: existing refreshes continue to bypass QUERY admission, while their SELECT workload still applies to execution. Requires a server restart.
)", 0) \\
''',
)

ensure_before(
    "src/Core/Settings.cpp",
    "DECLARE(String, refresh_workload",
    '    DECLARE(Milliseconds, workload_admission_timeout_ms, 0, R"(\n',
    '''    DECLARE(String, refresh_workload, "", R"(
Workload used for refreshable materialized view admission and execution. Specify it in the stored SELECT settings. A non-empty value overrides `workload` for the complete refresh, including its SELECT and internal operations, without changing the workload of the CREATE or ALTER query.

An empty value preserves the existing `workload` behavior. This setting has no effect on ordinary query execution. QUERY admission for refreshes also requires `use_query_slot_to_refresh_materialized_view` and a configured QUERY resource.
)", 0) \\
''',
)

path = Path("src/Core/SettingsChangesHistory.cpp")
text = path.read_text()
if '"refresh_workload"' not in text:
    anchor = '''        addSettingsChanges(settings_changes_history, "26.9",
        {
'''
    if text.count(anchor) != 1:
        raise SystemExit(f"expected one 26.9 anchor, got {text.count(anchor)}")
    text = text.replace(
        anchor,
        anchor + '            {"refresh_workload", "", "", "New setting to choose the workload for refreshable materialized view execution independently of the creating query."},\n',
        1,
    )
    path.write_text(text)

ensure_before(
    "src/Parsers/ParserCreateQuery.cpp",
    "### Workload Scheduling {#refresh-workload-scheduling}",
    "### Refresh Settings {#refresh-settings}\n",
    '''### Workload Scheduling {#refresh-workload-scheduling}

Use `refresh_workload` in the stored SELECT settings to choose the workload for refresh admission and execution:

```sql
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 MINUTE
ENGINE = MergeTree ORDER BY tuple()
AS SELECT * FROM source SETTINGS refresh_workload = 'refreshes';
```

`refresh_workload` takes effect only for the refresh. The ordinary `workload` setting still applies to the CREATE or ALTER query. When both are present, `refresh_workload` overrides `workload` throughout refresh execution, including nested SELECT settings and internal operations. The stored query definition is unchanged. An empty `refresh_workload` preserves the existing `workload` behavior.

When the server setting `use_query_slot_to_refresh_materialized_view` is enabled and a `QUERY` resource is configured for the effective refresh workload, the refresh requests a query slot before starting execution. Admission and execution use the same workload. A queued refresh reports `WaitingForResource` in `system.view_refreshes` without occupying a background execution worker. Stopping, pausing or dropping the view cancels pending admission.

The server setting is disabled by default to preserve existing scheduling behavior. With it disabled, or without a `QUERY` resource for the workload, refreshes do not wait for a query slot. Workload classification for CPU and I/O is independent of this setting.

''',
)
PY

git diff --check
git config user.name clickhouse-gh
git config user.email clickhouse-gh@users.noreply.github.com
git add -A
git commit -m "Merge current master into RMV workload PR"
git push origin HEAD:agent/refreshable-mv-workload
