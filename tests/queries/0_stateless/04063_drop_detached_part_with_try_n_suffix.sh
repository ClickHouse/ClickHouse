#!/usr/bin/env bash
# Tags: no-object-storage, no-shared-merge-tree

# Regression test for parsing detached part names with the _tryN suffix.
# The bug caused BAD_DATA_PART_NAME when trying to drop such parts with zero-copy
# replication, and made DROP DETACHED PARTITION ALL skip them silently everywhere
# (the unparsable name excluded them from the partition-level drop).
#
# The leftover "_tryN" directories are produced here with a filesystem-level "cp -r" of the
# original detached part directory. On object storage this only copies the local metadata, so all
# copies alias the same remote blobs; dropping the copies then removes blobs the original still
# references, and the subsequent "ATTACH PARTITION ALL" fails because the data is gone. The bug
# under test (detached part name parsing) is storage-agnostic, so the test runs on local disk only.
#
# The "no-object-storage" tag only skips the test in regular object-storage runs (clickhouse-test
# with "--s3-storage"). The stress test does not pass that flag yet still randomly makes an object
# storage policy the MergeTree default, so the table is pinned to the built-in local "default"
# storage policy below to keep "cp -r" safe regardless of the server's default policy.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_04063_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (n UInt64)
    ENGINE = MergeTree ORDER BY n
    SETTINGS storage_policy = 'default'
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} VALUES (1), (42)"

# Determine the actual part name. The first allocated block number is not
# necessarily 0 (e.g. under a Replicated database it comes from a Keeper
# counter), so the name must not be hardcoded.
PART=$(${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}' AND active
    LIMIT 1
")

# Detach the part
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DETACH PART '${PART}'"

# Get the path to the detached directory (parent of the part directory)
PART_PATH=$(${CLICKHOUSE_CLIENT} --query "
    SELECT path FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    LIMIT 1
")
DETACHED_DIR=$(dirname "${PART_PATH}")

# Create leftover copies of the detached part covering the suffix variants:
#   *_try1                       - single digit
#   covered-by-broken_*_try1     - with a known prefix
#   *_try100                     - multiple digits (must be accepted)
#   *_try                        - no digits (must NOT be treated as a tryN suffix)
for name in "${PART}_try1" "covered-by-broken_${PART}_try1" "${PART}_try100" "${PART}_try"; do
    cp -r "${DETACHED_DIR}/${PART}" "${DETACHED_DIR}/${name}"
done

# List detached parts - should see all of them (part name normalized for stable output)
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    ORDER BY name
" | sed "s/${PART}/PART/g"

# Drop the detached parts with _tryN suffix - this used to fail with BAD_DATA_PART_NAME
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DROP DETACHED PART 'covered-by-broken_${PART}_try1'
    SETTINGS allow_drop_detached = 1
"

${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DROP DETACHED PART '${PART}_try1'
    SETTINGS allow_drop_detached = 1
"

# A multi-digit suffix must also be droppable
${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DROP DETACHED PART '${PART}_try100'
    SETTINGS allow_drop_detached = 1
"

# The original part and the malformed "_try" directory (not a tryN suffix) should remain
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    ORDER BY name
" | sed "s/${PART}/PART/g"

# ATTACH PARTITION ALL must not be broken by a leftover "_tryN" directory: it should
# attach the original part and silently ignore the suffixed leftovers.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} ATTACH PARTITION ALL"
${CLICKHOUSE_CLIENT} --query "SELECT n FROM ${TABLE} ORDER BY n"

# The malformed "_try" directory is not a valid attach candidate, so it stays detached
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    ORDER BY name
" | sed "s/${PART}/PART/g"

# DROP DETACHED PARTITION ALL must drop "_tryN" leftovers too. Before the fix they were
# skipped silently, because their directory name failed to parse (valid_name was false),
# so a partition-level drop could never remove them.
PART2=$(${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}' AND active
    LIMIT 1
")
${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${TABLE} DETACH PART '${PART2}'"
cp -r "${DETACHED_DIR}/${PART2}" "${DETACHED_DIR}/${PART2}_try7"

${CLICKHOUSE_CLIENT} --query "
    ALTER TABLE ${TABLE} DROP DETACHED PARTITION ALL
    SETTINGS allow_drop_detached = 1
"

# Everything except the malformed "_try" directory (not a tryN suffix) must be gone
${CLICKHOUSE_CLIENT} --query "
    SELECT name FROM system.detached_parts
    WHERE table = '${TABLE}' AND database = '${CLICKHOUSE_DATABASE}'
    ORDER BY name
" | sed "s/${PART2}/PART2/g; s/${PART}/PART/g"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
