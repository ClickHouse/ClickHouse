#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-parallel-replicas, no-object-storage, no-replicated-database, no-shared-merge-tree, no-async-insert
# why: a with_column_ids table must refuse to load when column_ids.json is
# incomplete or absent -- auto-rebuild is unsafe because DROP + re-ADD makes
# on-disk files indistinguishable from their column ID alone.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_column_ids=1"

# Every `columns.txt` token must own a stream file named after it -- not a bijection, since a
# metadata-only DROP leaves an orphan stream no slot names.  Tables inspected this way must pin
# `replace_long_file_name_to_hash = 0`: randomized `max_file_name_length = 0` hashes every name.
slots_own_streams() {
    local dir=$1
    local missing
    missing=$(comm -23 \
        <(sed -n 's/^`\([^`]*\)`.*/\1/p' "${dir}columns.txt" | sort) \
        <(find "$dir" -maxdepth 1 -name '*.bin' -printf '%f\n' | sed 's/\.bin$//' | sort))
    [ -z "$missing" ] || { echo "slots with no stream: $missing"; false; }
}

data_path() { $CLIENT --query "SELECT data_paths[1] FROM system.tables WHERE database = currentDatabase() AND name = '$1'"; }

# Scenario 1: the mapping is present but misses an entry for a column that still exists in
# metadata.  The refused table stays detached -- the test's database is dropped at the end.
$CLIENT --query "CREATE TABLE t_corrupt_mapping (a UInt32, b String, c Float64) ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"

echo "INSERT INTO t_corrupt_mapping VALUES (1, 'x', 1.5)" | $CLIENT

table_dir=$(data_path t_corrupt_mapping)
$CLIENT --query "DETACH TABLE t_corrupt_mapping SYNC"

# The whole mapping, minus `c`.
printf '%s' '{"active":true,"next_column_id":0,"mapping":{"a":"a","b":"b"}}' > "${table_dir}column_ids.json"

$CLIENT --query "ATTACH TABLE t_corrupt_mapping" 2>&1 \
    | grep -q "missing entries for column" && echo "incomplete_mapping_refused" || echo "incomplete_mapping_loaded"

# Scenario 2: column_ids.json is ENTIRELY absent (deleted out-of-band).  The table must refuse
# to load rather than silently loading with no mapping and returning defaults for every
# non-identity column.  What refuses is the part, not the setting: a part written under column
# IDs cannot be read without the mapping, and that error fails the load instead of detaching
# the part as broken.
$CLIENT --query "CREATE TABLE t_missing_mapping (a UInt32, b String, c Float64) ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"

# DROP + re-ADD gives `c` the numeric column ID "1": its data lives in `1.bin`
# and is only reachable through column_ids.json.
echo "INSERT INTO t_missing_mapping VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_missing_mapping DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_missing_mapping ADD COLUMN c Float64"
echo "INSERT INTO t_missing_mapping VALUES (2, 'y', 9.9)" | $CLIENT

table_dir=$(data_path t_missing_mapping)
$CLIENT --query "DETACH TABLE t_missing_mapping SYNC"

rm -f "${table_dir}column_ids.json"

$CLIENT --query "ATTACH TABLE t_missing_mapping" 2>&1 \
    | grep -q "has no active column-ID mapping" && echo "absent_mapping_refused" || echo "absent_mapping_loaded"

# Discriminator independent of stderr: after a refusal the table must NOT be present;
# the buggy silent load leaves it present.
loaded=$($CLIENT --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_missing_mapping'")
echo "table_loaded_after_fileless_attach=${loaded}"

# Scenario 3: turning serialization_info_version to 'with_column_ids' on an existing table must
# create column_ids.json in the same commit -- committing the setting without a mapping bricks
# the table at ATTACH.  The flip is behind the same experimental gate as CREATE.
$CLIENT --query "CREATE TABLE t_settings_enable (a UInt32, b String) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0"
echo "INSERT INTO t_settings_enable VALUES (1, 'x')" | $CLIENT

$CLICKHOUSE_CLIENT --query "ALTER TABLE t_settings_enable MODIFY SETTING serialization_info_version = 'with_column_ids'" 2>&1 \
    | grep -q "allow_experimental_column_ids" && echo "flip_requires_experimental_flag" || echo "flip_missed_experimental_gate"

# The rejected flip may not have committed anything: the table still reloads.
$CLIENT --query "DETACH TABLE t_settings_enable SYNC"
$CLIENT --query "ATTACH TABLE t_settings_enable"
$CLIENT --query "SELECT a, b FROM t_settings_enable ORDER BY a"

$CLIENT --query "ALTER TABLE t_settings_enable MODIFY SETTING serialization_info_version = 'with_column_ids'"
table_dir=$(data_path t_settings_enable)
if [ -f "${table_dir}column_ids.json" ]; then
    echo "mapping_created_by_settings_flip"
else
    echo "mapping_missing_after_settings_flip"
fi

# The flip must leave the table loadable: a committed setting with no mapping bricks it at ATTACH.
$CLIENT --query "DETACH TABLE t_settings_enable SYNC"
$CLIENT --query "ATTACH TABLE t_settings_enable"

# Scenario 4: `columns.txt` declares its format, and a part that declares column IDs cannot be
# adopted by a table with no mapping.  Without the declaration the two forms are byte-identical,
# so the destination reads the IDs as column names and every non-identity column silently comes
# back as DEFAULT.
#
# `min_bytes_for_full_part_storage = 0` keeps `columns.txt` a standalone file: under
# Packed storage it lives inside one blob and cannot be read or moved on its own.
$CLIENT --query "CREATE TABLE t_fmt_ids (a UInt32, b String, c Float64) ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         replace_long_file_name_to_hash = 0"
$CLIENT --query "CREATE TABLE t_fmt_plain (a UInt32, b String, c Float64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         replace_long_file_name_to_hash = 0"

# DROP + re-ADD gives `c` the numeric column ID "1", so the part's columns.txt token
# for `c` is not its logical name and the destination cannot guess it.
echo "INSERT INTO t_fmt_ids VALUES (1, 'x', 1.5)" | $CLIENT
$CLIENT --query "ALTER TABLE t_fmt_ids DROP COLUMN c"
$CLIENT --query "ALTER TABLE t_fmt_ids ADD COLUMN c Float64"
echo "INSERT INTO t_fmt_ids VALUES (2, 'y', 9.9)" | $CLIENT
echo "INSERT INTO t_fmt_plain VALUES (1, 'x', 1.5)" | $CLIENT

ids_part=$($CLIENT --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_fmt_ids' AND active AND name LIKE 'all_2%' LIMIT 1")
plain_part=$($CLIENT --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_fmt_plain' AND active LIMIT 1")
echo "column-ids part: $(head -1 "${ids_part}columns.txt")"
slots_own_streams "${ids_part}" && echo "ids part slots own streams" || echo "ids part slot/stream desync"
echo "plain part: $(head -1 "${plain_part}columns.txt")"

plain_table_dir=$(data_path t_fmt_plain)
mkdir -p "${plain_table_dir}detached"
cp -r "${ids_part%/}" "${plain_table_dir}detached/all_2_2_0"

$CLIENT --query "ALTER TABLE t_fmt_plain ATTACH PART 'all_2_2_0'" 2>&1 \
    | grep -q "was written under column IDs" && echo "refused_column_ids_part" || echo "adopted_column_ids_part"
echo "rows after refusal: $($CLIENT --query "SELECT count() FROM t_fmt_plain")"

# Scenario 5: `serialization_info_version` is projection-overridable, so a projection part's own
# version does not decide how its streams are named -- the PARENT table's mapping, threaded into
# the writer, does.  The header must therefore describe what was written, not what the setting
# asked for.  (The opposite override, a projection asking for 'with_column_ids' on a table with no
# mapping, is rejected at DDL time -- see 04669_column_ids_projection_setting_gate.)
$CLIENT --query "CREATE TABLE t_proj_with_mapping (a UInt32, c UInt32) ENGINE = MergeTree ORDER BY a
SETTINGS serialization_info_version = 'with_column_ids', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0"
# ADD COLUMN gives `d` a numeric ID, so its stream name differs from its logical name.
$CLIENT --query "ALTER TABLE t_proj_with_mapping ADD COLUMN d UInt32"
$CLIENT --query "ALTER TABLE t_proj_with_mapping ADD PROJECTION p (SELECT d, sum(c) GROUP BY d)
WITH SETTINGS (serialization_info_version = 'with_types')"
echo "INSERT INTO t_proj_with_mapping (a, c, d) VALUES (1, 10, 7)" | $CLIENT

part=$($CLIENT --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't_proj_with_mapping' AND active LIMIT 1")
grep -q '`d`' "${part}p.proj/columns.txt" && echo "proj_columns_txt_uses_logical_name" || echo "proj_columns_txt_uses_column_id"
echo "parent part: $(head -1 "${part}columns.txt")"
echo "projection part: $(head -1 "${part}p.proj/columns.txt")"
# CHECK TABLE resolves every columns.txt slot to its stream files, so a header that disagreed
# with the slots below it fails here.
$CLIENT --query "CHECK TABLE t_proj_with_mapping SETTINGS check_query_single_value_result = 1"

$CLIENT --query "DETACH TABLE t_proj_with_mapping SYNC"
$CLIENT --query "ATTACH TABLE t_proj_with_mapping"
$CLIENT --query "SELECT d, sum(c) FROM t_proj_with_mapping GROUP BY d ORDER BY d SETTINGS force_optimize_projection = 1"
