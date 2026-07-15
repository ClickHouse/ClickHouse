"""
Guard test for the storage placement of the MinIO webhook log tables created in
ClickHouseProc.create_minio_log_tables (ci/jobs/scripts/clickhouse_proc.py).

Background
----------
`system.minio_audit_logs` and `system.minio_server_logs` are diagnostic tables
that capture the MinIO audit/server webhook stream during a run. They are plain
`ENGINE = MergeTree` tables, so on s3 storage runs they would inherit the
overridden default merge_tree policy (s3_storage_policy_for_merge_tree_by_default
.xml sets <merge_tree><storage_policy>s3</storage_policy>) and live ON S3. That is
doubly bad for a table that records S3 activity:

  * every audit-event insert writes parts to S3, which generates more audit
    events - a feedback loop that inflates the table, and
  * the post-run `select * ... into outfile` dump in dump_system_tables reads it
    all back from S3. On amd_tsan the JSON-typed audit table grew to ~700k rows /
    ~1.5 GB and the dump exceeded DUMP_SYSTEM_TABLE_TIMEOUT, turning the
    "Scraping system tables" step red (observed on master, e.g. commit 961ded3).

The fix pins both tables to the local `default` policy with an explicit
`SETTINGS storage_policy = 'default'`. `default` is a local policy on every
stateless config (nothing remaps it), so the dump reads locally and the audit
insert path no longer feeds itself over S3. This test guards against a future
edit dropping that setting and silently putting the tables back on S3.
"""

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SRC = _REPO_ROOT / "ci" / "jobs" / "scripts" / "clickhouse_proc.py"


def _create_statements(src):
    # Every `CREATE TABLE system.minio_*_logs ...` SQL string built in the
    # source, up to the closing double quote of the clickhouse-client --query.
    return re.findall(r"CREATE TABLE system\.minio_\w+_logs[^\"]*", src)


def test_minio_log_tables_are_pinned_to_local_storage():
    src = _SRC.read_text()
    statements = _create_statements(src)
    # Both the audit and server log tables must be created.
    assert len(statements) == 2, (
        f"expected 2 minio log table CREATE statements, found {len(statements)}: {statements}"
    )
    for stmt in statements:
        # The captured text is a Python source fragment, so the SQL string
        # quotes are backslash-escaped (\\'default\\'); drop the backslashes to
        # compare against the SQL as clickhouse-client will see it.
        sql = stmt.replace("\\", "")
        # A CREATE that ends at `ORDER BY tuple()` with no storage_policy would
        # inherit the s3-overridden default on s3 runs. Require the local pin
        # right after the ORDER BY, so the placement is explicit and local.
        assert re.search(r"ORDER BY tuple\(\)\s+SETTINGS\s+storage_policy = 'default'", sql), (
            "minio log table must be pinned to the local 'default' storage policy "
            f"so its dump is not read back from S3; got: {sql}"
        )
