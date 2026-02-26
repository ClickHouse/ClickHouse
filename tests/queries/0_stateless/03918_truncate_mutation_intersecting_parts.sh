#!/usr/bin/env bash
# Tags: no-parallel, no-ordinary-database, no-async-insert

# This test reproduces a LOGICAL_ERROR that causes server crash
# The bug: TRUNCATE in transaction doesn't create empty covering parts,
# leading to zombie parts resurrection that can intersect with mutation parts

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_intersecting_mutation SYNC"

# Create table
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE test_intersecting_mutation (id UInt64, value String)
ENGINE = MergeTree() ORDER BY id
"

# Insert data and optimize (not in transaction to avoid ABORTED error)
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_intersecting_mutation VALUES (1, 'a')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_intersecting_mutation VALUES (2, 'b')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_intersecting_mutation VALUES (3, 'c')"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE test_intersecting_mutation FINAL"


# TRUNCATE in transaction
${CLICKHOUSE_CLIENT} --multiquery --query "
BEGIN TRANSACTION;
TRUNCATE TABLE test_intersecting_mutation;
COMMIT;
"

# DETACH and ATTACH to resurrect zombie parts
${CLICKHOUSE_CLIENT} --query "DETACH TABLE test_intersecting_mutation"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE test_intersecting_mutation"

# Create mutation in transaction - mutation will apply to zombie parts
${CLICKHOUSE_CLIENT} --multiquery --query "
BEGIN TRANSACTION;
ALTER TABLE test_intersecting_mutation UPDATE value = 'mutated' WHERE 1 SETTINGS mutations_sync=2;
COMMIT;
"

# DETACH and ATTACH again to check intersecting
${CLICKHOUSE_CLIENT} --query "DETACH TABLE test_intersecting_mutation"

${CLICKHOUSE_CLIENT} --query "ATTACH TABLE test_intersecting_mutation"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test_intersecting_mutation SYNC"
