#!/usr/bin/env python3
"""
Test cases for report queries: ``commit_sha``, ``head_ref``, and ``expect``.

``expect`` is the exact set of ``test_name`` values the query must return (order
ignored). Use an empty ``expect`` tuple when it must return no failures.

Requires ``testflows`` and ``clickhouse_driver``. Run:

  pip install testflows
  python .github/actions/create_workflow_report/test_report_queries.py

Set ``CHECKS_DATABASE_HOST``, ``CLICKHOUSE_TEST_STAT_LOGIN``,
``CLICKHOUSE_TEST_STAT_PASSWORD``.

To capture ``expect`` for a commit:

  python3 -c "
  import os
  from clickhouse_driver import Client
  from create_workflow_report import get_checks_fails
  c = Client(host=os.environ['CHECKS_DATABASE_HOST'],
             user=os.environ['CLICKHOUSE_TEST_STAT_LOGIN'],
             password=os.environ['CLICKHOUSE_TEST_STAT_PASSWORD'],
             port=9440, secure='y', verify=False, settings={'use_numpy': True})
  df = get_checks_fails(c, 'COMMIT_SHA', 'HEAD_REF')
  print(repr(tuple(sorted(df['test_name'].tolist()))))
  "
"""

import os

import pandas as pd
from clickhouse_driver import Client
from testflows.core import *

from create_workflow_report import get_checks_fails


def check_result_matches_expect(df: pd.DataFrame, expect: list[str]) -> None:
    """Result ``test_name`` values must match ``expect`` exactly (as a set)."""
    if "test_name" not in df.columns and not df.empty:
        fail(f"DataFrame has no test_name column: {df.columns.tolist()}")
    actual = set(df["test_name"].tolist()) if not df.empty else set()
    required = set(expect)
    if actual != required:
        fail(f"test results mismatch: got {sorted(actual)}; expected {sorted(required)}")


@TestOutline(Scenario)
@Examples(
    "case_id commit_sha head_ref expect",
    [
        (
            "tests_passing_no_reruns",
            "088fb0351680a67f6f34b4ad56ca12030012c919",
            "qa/update-broken-tests",
            (),
        ),
        (
            "stress_startup_failed_no_reruns",
            "33fd024890a401d30d85b3fad656f9deba916cbc",
            "antalya-25.8",
            (
                "Cannot start clickhouse-server",
                "Server failed to start (see application_errors.txt and clickhouse-server.clean.log)",
            ),
        ),
        (
            "stateless_and_integration_failed_no_reruns",
            "4edabbad8665e1c727bbd7c891e0cbcd81aefd56",
            "antalya-25.8",
            (
                "test_dns_cache/test.py::test_user_access_ip_change[node5]",
                "01042_check_query_and_last_granule_size",
            ),
        ),
        (
            "stateless_fail_in_teardown_no_reruns",
            "0cd90a87ab7b2ad83d24df87d96af5d3de7858c2",
            "feature/antalya-26.1/json_part2",
            (
                "00411_long_accurate_number_comparison_int4",
                "03572_export_merge_tree_part_limits_and_table_functions",
                "Exception in test runner",
                "Some queries hung",
            ),
        ),
        (
            "stateless_and_integration_passed_after_reruns",
            "49bb3f7beb5e6e424a1e94c749478fd23a8e6196",
            "antalya-25.8",
            (),
        ),
        (
            "stress_passed_after_reruns",
            "51762a72207f3d4bcee51a0a78912a8b2cbb1bb5",
            "antalya-25.8",
            (),
        ),
        # Post Hooks (later stopwatch) used to set the check_start_time anchor and hide
        # Integration FAILs, e.g. AzureQueue on https://github.com/Altinity/ClickHouse/pull/2107.
        (
            "integration_fail_hidden_by_post_hooks_anchor",
            "6185add695010b15869b3dc1bcb9c417621b36bc",
            "bump/antalya-26.6/26.6.2.81",
            (
                "00071_merge_tree_optimize_aio",
                "01167_isolation_hermitage",
                "01509_parallel_quorum_insert_no_replicas_long",
                "03752_attach_as_replicated_transaction_metadata",
                "04033_tpc_ds_q20",
                "04033_tpc_ds_q39",
                "04033_tpc_ds_q58",
                "04033_tpc_ds_q71",
                "Unknown error",
                "test_storage_s3_queue/test_0.py::test_move_after_processing[another_bucket-AzureQueue]",
            ),
        ),
    ],
)
def test_checks_fails_query(self, case_id, commit_sha, head_ref, expect):
    """Test checks fails query for one commit."""
    with Given("DB Client is configured"):
        host = os.getenv("CHECKS_DATABASE_HOST")
        user = os.getenv("CLICKHOUSE_TEST_STAT_LOGIN")
        password = os.getenv("CLICKHOUSE_TEST_STAT_PASSWORD")
        if not all([host, user, password]):
            skip(
                "Set CHECKS_DATABASE_HOST, CLICKHOUSE_TEST_STAT_LOGIN, "
                "CLICKHOUSE_TEST_STAT_PASSWORD"
            )
        client = Client(
            host=host,
            user=user,
            password=password,
            port=9440,
            secure="y",
            verify=False,
            settings={"use_numpy": True},
        )

    with When(
        f"I call get_checks_fails for commit_sha={commit_sha!r} head_ref={head_ref!r}"
    ):
        df = get_checks_fails(client, commit_sha, head_ref)

    with Then("result test_name set equals expect"):
        check_result_matches_expect(df, list(expect))


@Name("test report queries")
@TestModule
def test_report_queries(self):
    Scenario(run=test_checks_fails_query, flags=TE)


if main():
    test_report_queries()
