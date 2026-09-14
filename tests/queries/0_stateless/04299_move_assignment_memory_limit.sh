#!/usr/bin/env bash
# Tags: long, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Reproducer from a fuzzer/stress run (https://github.com/ClickHouse/ClickHouse/pull/106096).
# A UNION query unites query plans via QueryPlan::unitePlans, which calls
# QueryPlanResourceHolder::append; append inserts into memory-tracking containers and so can throw
# MEMORY_LIMIT_EXCEEDED. append (and the BlockIO / QueryPipeline / QueryPlanResourceHolder move
# assignments) must not be noexcept, otherwise that throw crosses a noexcept boundary and terminates
# the server. Inject a small memory-tracker fault probability with every allocation tracked
# (max_untracked_memory = 0) and loop, so a fault eventually lands inside append. With the bug the
# server terminates and clickhouse-test fails the run; without it the queries just fail with
# MEMORY_LIMIT_EXCEEDED (swallowed below) and the server stays up.
#
# The fault probability is deliberately small: a large one makes the query fail early, before it
# reaches append. Against a buggy build this crashed within a few dozen iterations.

query="SELECT DISTINCT 1025, toFixedString('%', 1048576), 100 UNION ALL SELECT 9223372036854775807, '\0', materialize(toLowCardinality(1025)) SETTINGS extremes = 1 FORMAT Null"

for _ in {1..500}; do
    $CLICKHOUSE_CLIENT --memory_tracker_fault_probability=0.001 --max_untracked_memory=0 --query="$query" >/dev/null 2>&1 ||:
done
