#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-replicated-database, no-flaky-check
# no-fasttest: TPC-DS tables use web disk (S3) which is not available in fasttest.
# no-random-settings: random session_timezone, query_plan_join_swap_table, etc. change query results.
# no-replicated-database: the `datasets` database is not created in DatabaseReplicated mode.
# no-flaky-check: TPC-DS queries are too expensive for thread fuzzer.
# Known issue: Memory Limit Exceeded with reasonable amount of memory. Enable the test once the issue is resolved or memory is optimised.

echo "SKIP"
