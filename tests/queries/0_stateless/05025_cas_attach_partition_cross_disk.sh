#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# `ATTACH PARTITION FROM` across disks clones the part through `freezeRemote`, and a
# content-addressed destination models a part as ONE atomic unit: N files, one manifest, one ref.
# Without a single transaction every file autocommits as its own one-file manifest against the same
# ref, so two of them resolve the ref as absent and the loser hits the unique-ref guard -- the very
# first attach fails.
#
# The two tables must be on DIFFERENT disks: the clone path is chosen by `on_same_disk`, and a
# same-disk attach goes through `freeze`, which already has the transaction branch.
#
# Leg 2 is the same-pool content-addressed case. Its row-level result verifies that the destination
# resolves the shared content correctly without relying on a particular physical-publication branch.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every table this test creates, including leg 3's, so a run interrupted mid-script can be repeated.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_plain;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS dst_cas;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_cas;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS dst_cas_same_pool;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS src_plain_repl SYNC;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS dst_cas_repl SYNC;"

# ---------------------------------------------------------------- leg 1: local -> content-addressed

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE src_plain (k UInt32, v String)
ENGINE = MergeTree ORDER BY k PARTITION BY k
SETTINGS disk = disk(
    type = local,
    name = '05025_plain',
    path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_05025_plain/');"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dst_cas (k UInt32, v String)
ENGINE = MergeTree ORDER BY k PARTITION BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '05025_dst',
    name = '05025_cas_dst',
    path = '05025_cas_dst_pool/');"

${CLICKHOUSE_CLIENT} --query "INSERT INTO src_plain SELECT number % 2, toString(number) FROM numbers(64);"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE dst_cas ATTACH PARTITION 1 FROM src_plain;"

# Completeness, not mere presence: a partially published part would satisfy a bare count().
${CLICKHOUSE_CLIENT} --query "SELECT 'leg1', count(), sum(k), uniqExact(v) FROM dst_cas;"

# A detach/attach round trip reads the part back from its manifest rather than from whatever the
# writing session still had warm.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE dst_cas DETACH PARTITION 1;"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE dst_cas ATTACH PARTITION 1;"
${CLICKHOUSE_CLIENT} --query "SELECT 'leg1_roundtrip', count(), sum(k) FROM dst_cas;"

# ------------------------------------------- leg 2: content-addressed -> content-addressed, one pool

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE src_cas (k UInt32, v String)
ENGINE = MergeTree ORDER BY k PARTITION BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '05025_shared_a',
    name = '05025_cas_shared_a',
    path = '05025_cas_shared_pool/');"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dst_cas_same_pool (k UInt32, v String)
ENGINE = MergeTree ORDER BY k PARTITION BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '05025_shared_b',
    name = '05025_cas_shared_b',
    path = '05025_cas_shared_pool/');"

${CLICKHOUSE_CLIENT} --query "INSERT INTO src_cas SELECT number % 2, toString(number) FROM numbers(64);"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE dst_cas_same_pool ATTACH PARTITION 1 FROM src_cas;"

${CLICKHOUSE_CLIENT} --query "SELECT 'leg2', count(), sum(k), uniqExact(v) FROM dst_cas_same_pool;"

# ------------------------------------------------ leg 3: replicated, local -> content-addressed

# The replicated ATTACH is a DIFFERENT shape and reaches the same function: of the replicated clone
# sites only the ATTACH branch passes `must_on_same_disk=false`, and its clone params set
# `metadata_version_to_write`, so after the transaction commits the caller writes
# `metadata_version.txt` separately -- a repoint of an already-published part rather than a file
# inside the clone. REPLACE on a replicated table is same-disk only and cannot reach this path.

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE src_plain_repl (k UInt32, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05025_src_repl', 'r1')
ORDER BY k PARTITION BY k
SETTINGS disk = disk(
    type = local,
    name = '05025_plain_repl',
    path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_DATABASE}_05025_plain_repl/');"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dst_cas_repl (k UInt32, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05025_dst_repl', 'r1')
ORDER BY k PARTITION BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '05025_dst_repl',
    name = '05025_cas_dst_repl',
    path = '05025_cas_dst_repl_pool/');"

${CLICKHOUSE_CLIENT} --query "INSERT INTO src_plain_repl SELECT number % 2, toString(number) FROM numbers(64);"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE dst_cas_repl ATTACH PARTITION 1 FROM src_plain_repl;"
${CLICKHOUSE_CLIENT} --query "SELECT 'leg3', count(), sum(k), uniqExact(v) FROM dst_cas_repl;"

# The metadata-version repoint lands on a committed part, so the part must still read after a
# detach/attach round trip -- that is what proves the repoint did not corrupt the published ref.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE dst_cas_repl DETACH PARTITION 1;"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE dst_cas_repl ATTACH PARTITION 1;"
${CLICKHOUSE_CLIENT} --query "SELECT 'leg3_roundtrip', count(), sum(k) FROM dst_cas_repl;"

${CLICKHOUSE_CLIENT} --query "DROP TABLE src_plain;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE dst_cas;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE src_cas;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE dst_cas_same_pool;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE src_plain_repl SYNC;"
${CLICKHOUSE_CLIENT} --query "DROP TABLE dst_cas_repl SYNC;"
${CLICKHOUSE_CLIENT} --query "SELECT 'dropped_ok';"
