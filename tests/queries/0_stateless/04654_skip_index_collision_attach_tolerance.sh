#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# no-ordinary-database: ATTACH ... UUID requires an Atomic database.
# no-replicated-database: a Replicated database rewrites the UUID of an ATTACHed table.
# escape_index_filenames defaulted to false before 26.1, so a table created on an older version can
# already hold two skip indices that resolve to the same base stream name. CREATE rejects that pair
# now, but ATTACH must still load such a table: refusing it would turn a data bug into a startup
# failure. The check logs an error instead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A fresh UUID per run keeps this test safe to run in parallel with itself.
uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

definition="(k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
    INDEX \`a.dct\` w TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 0"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_attach_collide"

# CREATE is rejected.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_attach_collide ${definition}" 2>&1 \
    | grep -c -m1 "BAD_ARGUMENTS"

# ATTACH of the very same definition succeeds, and reports the collision as an error in the log.
${CLICKHOUSE_CLIENT} --send_logs_level=error -q "ATTACH TABLE t_attach_collide UUID '${uuid}' ${definition}" 2>&1 \
    | grep -c -m1 "Table definition is incorrect.*collision in file name skp_idx_a.dct"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_attach_collide"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_attach_collide"

# An inert index type writes no files, so it can never collide with anything and must not be
# reported. ATTACH is the only route that reaches an inert index: its validator rejects a
# non-attaching query outright. A non-inert index in the same shape is reported, which is what makes
# the negative assertion discriminating.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_inert"

inert_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
${CLICKHOUSE_CLIENT} --send_logs_level=error -q "
    ATTACH TABLE t_inert UUID '${inert_uuid}' (k UInt64, s String, w UInt64,
        INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
        INDEX \`a.dct\` w > 0 TYPE hypothesis GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 0
" 2>&1 | grep -c "collision in file name"

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_inert"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_inert"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_not_inert"

not_inert_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
${CLICKHOUSE_CLIENT} --send_logs_level=error -q "
    ATTACH TABLE t_not_inert UUID '${not_inert_uuid}' (k UInt64, s String, w UInt64,
        INDEX a(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
        INDEX \`a.dct\` w TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k SETTINGS escape_index_filenames = 0
" 2>&1 | grep -c -m1 "collision in file name"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_not_inert"

# An index whose description attach-mode validation tolerates but whose creator rejects must not stop
# the table from loading. `bloomFilterIndexValidator` gates its argument checks on `!attach`, while
# `bloomFilterIndexCreator` does an unconditional `safeGet<Float64>`, so an integer argument reaches the
# creator and throws BAD_GET. There is no colliding index here at all: the only reason this definition
# meets the check is that the check constructs every index.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_uncheckable"

uncheckable_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
attach_output=$(${CLICKHOUSE_CLIENT} --send_logs_level=warning -q "
    ATTACH TABLE t_uncheckable UUID '${uncheckable_uuid}' (k UInt64, w UInt64,
        INDEX i w TYPE bloom_filter(1) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k
" 2>&1)

# The creator's error is reported and the index is skipped, not propagated.
echo "${attach_output}" | grep -c -m1 "Skipping index 'i' of type 'bloom_filter'"
echo "${attach_output}" | grep -c -m1 "Bad get: has UInt64, requested Float64"
# The table loaded and is queryable.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_uncheckable'"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_uncheckable"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_uncheckable"

# The same definition via CREATE is still rejected: there the validator's `!attach` branch fires, so
# tolerating construction failure on ATTACH does not let a malformed new definition through.
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_uncheckable (k UInt64, w UInt64,
        INDEX i w TYPE bloom_filter(1) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k
" 2>&1 | grep -c -m1 "BAD_ARGUMENTS"

# Both bases are long enough to be replaced by a hash, so the reported name is the hashed one and the
# hint about `replace_long_file_name_to_hash` is appended. The expected hash is derived here rather
# than hardcoded, the way 02869_insert_filenames_collisions does it for columns.
long_name="a_text_index_with_a_deliberately_very_long_name"
expected_hash=$(${CLICKHOUSE_CLIENT} -q \
    "SELECT lower(hex(reverse(CAST(sipHash128('skp_idx_${long_name}.dct'), 'FixedString(16)'))))")

hashed_error=$(${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE t_hashed (k UInt64, s String, w UInt64,
        INDEX \`${long_name}\`(s) TYPE text(tokenizer = ngrams(3)) GRANULARITY 1,
        INDEX \`${long_name}.dct\` w TYPE set(100) GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k
    SETTINGS escape_index_filenames = 0, replace_long_file_name_to_hash = 1, max_file_name_length = 42
" 2>&1)

echo "${hashed_error}" | grep -c -m1 "BAD_ARGUMENTS"
echo "${hashed_error}" | grep -c -m1 "collision in file name ${expected_hash}"
# Anchored on the hint sentence: the bare setting name also appears in the echoed query text.
echo "${hashed_error}" | grep -c -m1 "see setting 'replace_long_file_name_to_hash'"

# A table that already holds a colliding pair must stay alterable - ATTACH tolerating it and then
# freezing its schema would defeat the point - but only while the contested base keeps holding the
# same data files written by the same owners. The arms below pin both halves of that boundary.
attach_colliding () {
    local tbl="$1" indices="$2"
    local attach_uuid
    attach_uuid=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${tbl} SYNC"
    ${CLICKHOUSE_CLIENT} --send_logs_level=none -q "
        ATTACH TABLE ${tbl} UUID '${attach_uuid}' (k UInt64, s String, w UInt64, ${indices})
        ENGINE = MergeTree ORDER BY k
        SETTINGS escape_index_filenames = 0, allow_experimental_text_index_phrase_search = 1"
}

text_and="INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,"

# 1. An ALTER that touches no index at all is accepted.
attach_colliding t_alter_unrelated "${text_and} INDEX \`a.pos\` w TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_alter_unrelated ADD COLUMN zz UInt8" 2>&1 | grep -c "BAD_ARGUMENTS"

# 2. An ADD INDEX introducing a collision on a DIFFERENT base is still refused, so grandfathering is
# scoped to the contested base rather than being a table-wide bypass.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_alter_unrelated ADD INDEX \`a.pst\` w TYPE minmax GRANULARITY 1" 2>&1 \
    | grep -c -m1 "BAD_ARGUMENTS"

# 3. DROP INDEX of the colliding index still works: it is the repair path.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_alter_unrelated DROP INDEX \`a.pos\` SETTINGS mutations_sync = 2" 2>&1 \
    | grep -c "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "SELECT groupArray(name) FROM system.data_skipping_indices
    WHERE database = currentDatabase() AND table = 't_alter_unrelated'"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_alter_unrelated SYNC"

# 4. CLEAR INDEX keeps the index in metadata, so the collision survives into the post-ALTER metadata
# and this is accepted only because it is grandfathered.
attach_colliding t_alter_clear "${text_and} INDEX \`a.pos\` w TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_alter_clear CLEAR INDEX a SETTINGS mutations_sync = 2" 2>&1 \
    | grep -c "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_alter_clear SYNC"

# 5. TWO pre-existing colliding bases on one table: both must be recorded, so the collector cannot
# stop at the first collision it finds.
attach_colliding t_alter_two "${text_and}
    INDEX \`a.pos\` w TYPE minmax GRANULARITY 1,
    INDEX \`a.pst\` w TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_alter_two ADD COLUMN zz UInt8" 2>&1 | grep -c "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_alter_two SYNC"

# A projection is its own filename namespace, and grandfathering is per namespace for the same
# reason. It is not asserted here: a projection's parser accepts no INDEX clause, so its only indices
# are the implicit `auto_minmax_index_<column>` ones, which are minmax (a single `""` substream) over
# distinct column names and therefore always resolve to distinct bases. No colliding projection can
# be built, so an arm for it would pass without exercising anything.

# 6. The same-name TYPE CHANGE on the contested base. The owner names are unchanged, but `minmax`
# writes `.idx2` while `set` writes `.idx`, which is the data file the text index's positional
# substream already writes - the shape that aborts a merge. It must be ONE statement: split in two,
# the DROP completes first and the ADD is refused as an ordinary new claimant instead, which does not
# exercise the grandfathering key at all.
attach_colliding t_alter_retype "${text_and} INDEX \`a.pos\` w TYPE minmax GRANULARITY 1"
${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE t_alter_retype DROP INDEX \`a.pos\`, ADD INDEX \`a.pos\` w TYPE set(100) GRANULARITY 1" 2>&1 \
    | grep -c -m1 "BAD_ARGUMENTS"
# On the very same table an unrelated ALTER must still succeed, else an arm that refuses everything
# (because grandfathering never fires at all) would read as green.
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_alter_retype ADD COLUMN zz UInt8" 2>&1 | grep -c "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_alter_retype SYNC"

# Turning escaping off makes a previously clean pair collide. The pre-existing map is resolved with
# the OLD setting, so this collision is new and still refused.
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_alter_unescape SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_alter_unescape (k UInt64, s String, w UInt64,
    INDEX a(s) TYPE text(tokenizer = ngrams(3), support_phrase_search = 1) GRANULARITY 1,
    INDEX \`a.pos\` w TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k
    SETTINGS escape_index_filenames = 1, allow_experimental_text_index_phrase_search = 1"
${CLICKHOUSE_CLIENT} -q "ALTER TABLE t_alter_unescape MODIFY SETTING escape_index_filenames = 0" 2>&1 \
    | grep -c -m1 "BAD_ARGUMENTS"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_alter_unescape SYNC"
