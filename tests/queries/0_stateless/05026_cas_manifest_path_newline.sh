#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# A projection name is used verbatim as a part-relative directory, so a projection named with a newline
# puts a newline in a part-file path. `MergeTree` allows that, and a content-addressed disk has to carry it:
# the manifest writes each path twice -- escaped in its record line, and in an `Inline` entry's payload-zone
# banner -- and both spellings have to agree or the writer cannot read back what it just wrote.
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DISK_NAME="ca_05026_${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}"
POOL_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}_05026_${RANDOM}"
TABLE="t_05026_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE};"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE ${TABLE} (k UInt32, v String, PROJECTION \`p
q\` (SELECT v, count() GROUP BY v))
ENGINE = MergeTree ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '05026',
    name = '${DISK_NAME}',
    path = '${POOL_DIR}/');"

echo 'insert_ok'
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SELECT number, 'v' || (number % 3) FROM numbers(30);" \
    && echo 1

# The part is read back through the same manifest that was just written -- on the broken tree the `INSERT`
# never got this far, because the manifest it wrote could not be decoded.
echo 'rows'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE};"

# And the projection itself is usable, which is what a newline in its name must not prevent. The settings
# `optimize_use_projections` and `force_optimize_projection` are the assertion: without the latter, this
# same result comes from the main part and the query proves nothing about the projection at all.
echo 'projection_result'
${CLICKHOUSE_CLIENT} --query "SELECT v, count() FROM ${TABLE} GROUP BY v ORDER BY v
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;"

# No manifest body without a committed owner: the successful `INSERT` left nothing orphaned, and on the
# broken tree the failed one did -- that object wedged every later collection round.
${CLICKHOUSE_CLIENT} --query "SYSTEM CAS FSCK '${DISK_NAME}'" --format TSVWithNames \
    | awk -F'\t' 'NR==1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
                  { print "unreachable", $col["unreachable"]; print "dangling", $col["dangling"] }'

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE};"
${CLICKHOUSE_CLIENT} --query "SELECT 'dropped_ok';"
