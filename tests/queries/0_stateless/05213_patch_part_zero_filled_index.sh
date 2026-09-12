#!/usr/bin/env bash

# A patch part carries the index of the parts it patches in `source_parts.dat`. A zeroed block in that
# file used to parse as a valid index of format version `V1` with no source parts at all: the patch
# part loaded clean and Active, the acknowledged update was silently unapplied, and background cleanup
# then deleted the only copy of it because an empty index reports data version 0. The part is detached
# as broken now, so the corruption is reported and the rows stay recoverable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05213_patch_part_zero_filled_index"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    CREATE TABLE t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO t SELECT number, number FROM numbers(5000);
    UPDATE t SET v = v + 1000 WHERE id < 2500 SETTINGS enable_lightweight_update = 1;

    SELECT 'updated', sum(v) FROM t;
    SELECT 'active patch parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't' AND active AND startsWith(name, 'patch-');
" </dev/null

# The patch part survives with all its files at their recorded sizes, but the index has lost its content.
SOURCE_PARTS_FILE=$(find "${WORKING_DIR}" -name source_parts.dat | head -1)
dd if=/dev/zero of="${SOURCE_PARTS_FILE}" bs=1 count="$(stat -c%s "${SOURCE_PARTS_FILE}")" conv=notrunc 2>/dev/null

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    SELECT 'active patch parts after the zero fill', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't' AND active AND startsWith(name, 'patch-');
    SELECT 'detached patch parts', count(), any(reason) FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't';
" </dev/null

rm -rf "${WORKING_DIR}"

# A patch part that is not corrupted still loads and still applies after a restart.
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    CREATE TABLE t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

    INSERT INTO t SELECT number, number FROM numbers(5000);
    UPDATE t SET v = v + 1000 WHERE id < 2500 SETTINGS enable_lightweight_update = 1;
" </dev/null

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" --multiquery -q "
    SELECT 'reloaded', sum(v) FROM t;
    SELECT 'reloaded patch parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't' AND active AND startsWith(name, 'patch-');
    SELECT 'reloaded detached parts', count() FROM system.detached_parts
    WHERE database = currentDatabase() AND table = 't';
" </dev/null

rm -rf "${WORKING_DIR}"
