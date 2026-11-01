#!/usr/bin/env bash
# Tags: zookeeper
# RENAME DATABASE must reject a target name that would leave a table undroppable, because the
# dropped-metadata filename metadata_dropped/{db}.{table}.{uuid}.sql has to fit the filesystem
# limit. Two holes are covered: the check used to sit inside the dependency-check guard, so
# check_table_dependencies=0 skipped it; and detached tables were never checked at all, which is
# reachable at default settings because a database whose tables are all detached has an empty
# attached-tables map.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT_NO_DEPS="$CLICKHOUSE_CLIENT --check_table_dependencies=0"

# Build a database name of an exact escaped length. CLICKHOUSE_DATABASE is
# test_<12 alternating lowercase letters and digits>, so every byte is a word character and the
# escaped length equals the character count. The unique prefix is what keeps this parallel-safe.
pad() { printf '%*s' "$1" '' | tr ' ' d; }
mkdb() { echo "${CLICKHOUSE_DATABASE}_$1$(pad $(($2 - ${#CLICKHOUSE_DATABASE} - 1 - ${#1})))"; }

# Report a rename outcome by name. A zero exit status is the only thing that means accepted, so a
# client killed mid-query cannot be recorded as a successful rename and a server warning emitted
# during a rename that did succeed cannot be mistaken for an error. Rejection additionally has to
# name the length guard, so an unrelated failure stays -unexpected-error and reddens the reference.
rename_outcome() {
    local label="$1" query="$2" client="$3" out rc
    out=$($client -q "$query" 2>&1); rc=$?
    if [ "$rc" == "0" ]; then echo "$label-accepted"
    elif echo "$out" | grep -q 'ARGUMENT_OUT_OF_BOUND'; then echo "$label-rejected"
    else echo "$label-unexpected-error"; fi
}

# After a rejected rename the catalog must be untouched: the target name must not exist and the
# source must still be addressable under its old name. These two see the catalog rename
# (updateDatabaseName / database_name = new_name); recover_after_reject below sees the on-disk
# half, which happens earlier and is invisible here. Both counts come from one scan, so a
# diagnostic that carries no tab leaves the second field empty and cannot spell the expected line.
reject_postcondition() {
    local label="$1" src="$2" target="$3" row target_present source_ok
    row=$($CLICKHOUSE_CLIENT -q "SELECT countIf(name = '$target'), countIf(name = '$src') FROM system.databases" 2>&1)
    IFS=$'\t' read -r target_present source_ok <<< "$row"
    echo "$label-postcondition target=$target_present source=$source_ok"
}

# After a rejected rename the documented recovery path must still work: renaming the source to a
# short name. The check runs before the metadata moveFile, so the source's metadata/<name>.sql is
# still where the catalog says it is. A check relocated below that moveFile throws the same
# ARGUMENT_OUT_OF_BOUND after the file has already been renamed away, and only this assertion
# notices -- the catalog columns above cannot, because the catalog rename happens later still.
recover_after_reject() {
    local label="$1" src="$2" out rc
    out=$($CLICKHOUSE_CLIENT -q "RENAME DATABASE \`$src\` TO \`${src}_ok\`" 2>&1); rc=$?
    if [ "$rc" == "0" ]; then echo "$label-recoverable"
    else echo "$label-recovery-failed rc=$rc"; fi
}

# Report whether a table could really be dropped. The success marker is emitted only on a zero
# exit status, so a drop that fails without printing a diagnostic, or with one that carries no
# error code, cannot be laundered into a success.
drop_outcome() {
    local label="$1" table="$2" out rc
    out=$($CLICKHOUSE_CLIENT -q "DROP TABLE $table SYNC" 2>&1); rc=$?
    if [ "$rc" == "0" ]; then echo "$label"
    else echo "$label-failed rc=$rc"; fi
}

# Setup is batched into one round trip per database. The reads below are deliberately NOT batched:
# a multi-statement query stops at its first failing statement, and three arms reject by design, so
# batching a read would drop the later oracles instead of reporting them.
new_db_with_table() {
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$1\` ENGINE = Atomic; CREATE TABLE \`$1\`.\`$2\` (c0 Int) ENGINE = MergeTree() ORDER BY tuple()"
}

# The replica-status row a Replicated DDL returns is not part of what is asserted. A failure here
# still surfaces, because the rename arm then reports -unexpected-error.
new_replicated_db_with_table() {
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$1\` ENGINE = Replicated('/clickhouse/databases/test/$1', 's1', 'r1');
        CREATE TABLE \`$1\`.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple()" >/dev/null
}

# Same, with the table detached: the detached arms need exactly this state before the rename.
new_db_with_detached_table() {
    $CLICKHOUSE_CLIENT -q "CREATE DATABASE \`$1\` ENGINE = Atomic; CREATE TABLE \`$1\`.t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple(); DETACH TABLE \`$1\`.t0"
}

# When using s3_plain_rewriteable as a db disk, minio doesn't allow the path segment to have
# more than 255 characters, and these database names produce segments close to that limit.
# Refer: https://github.com/minio/minio/blob/ddd9a84cd769e6bed67f5fe860f8f3c7527a6971/cmd/xl-storage.go#L154-L167
# The probe is fail-closed: only 0 and 1 are answers, so a broken query reports a marker that is
# absent from the reference instead of being taken for "supported".
probe=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name='disk_db_remote' AND type = 'ObjectStorage' AND object_storage_type='S3' AND metadata_type='PlainRewritable'" 2>&1)
probe_rc=$?
probe=$(printf '%s' "$probe" | tr -d '[:space:]')
if [ "$probe_rc" != "0" ] || { [ "$probe" != "0" ] && [ "$probe" != "1" ]; }; then
    echo "capability-probe-failed rc=$probe_rc out=$probe"
    exit 0
fi
if [ "$probe" != "0" ]; then
    echo "@@SKIP@@: database disk is s3_plain_rewriteable, database names would exceed the object path segment limit"
    exit 0
fi

# At 214 the prefix alone leaves a table-name limit of 0 (213 is the first such length).
# 211 is the largest one that still admits a 2-character table name.
# 200 leaves a limit of 13, which separates a 2-character from a 20-character table name.
long_a=$(mkdb a 214); long_b=$(mkdb b 214); long_c=$(mkdb c 214); long_d=$(mkdb d 214)
edge_a=$(mkdb e 211); edge_b=$(mkdb f 211)
mid_a=$(mkdb g 200);  mid_b=$(mkdb h 200)
repl_long=$(mkdb i 214); repl_edge=$(mkdb j 211)
long_tbl=ttttttttttttttttttt0

# Each arm gets its own database, all prefixed with CLICKHOUSE_DATABASE, and teardown is deferred
# to one batch at the end. Dropping per arm would cost more round trips than every assertion in
# this test put together, and no other test asserts a database count.

# 1. The bug: with dependency checks off the length check was skipped entirely.
src="${CLICKHOUSE_DATABASE}_s1"
new_db_with_table "$src" t0
rename_outcome deps-off-into-long "RENAME DATABASE \`$src\` TO \`$long_a\`" "$CLIENT_NO_DEPS"
reject_postcondition deps-off-into-long "$src" "$long_a"
recover_after_reject deps-off-into-long "$src"

# 2. Control: the guard-TRUE path was already correct and must stay so.
src="${CLICKHOUSE_DATABASE}_s2"
new_db_with_table "$src" t0
rename_outcome deps-on-into-long "RENAME DATABASE \`$src\` TO \`$long_b\`" "$CLICKHOUSE_CLIENT"

# 3. Control against over-rejection: at 211 the limit is 2 and the table name is 2 characters,
#    so the dropped-metadata name lands exactly on the limit. A flag-everything fix reddens
#    here. The table is then dropped, which is what proves an accepted rename really leaves it
#    droppable rather than only reporting success.
src="${CLICKHOUSE_DATABASE}_s3"
new_db_with_table "$src" t0
rename_outcome deps-off-accept "RENAME DATABASE \`$src\` TO \`$edge_a\`" "$CLIENT_NO_DEPS"
drop_outcome droppable-after-accept "\`$edge_a\`.t0"

# 4/5. The discriminating pair: the same target database length, accepted or rejected only by
#      table name length (the limit at 200 is 13). No length-blind rule satisfies both rows.
src="${CLICKHOUSE_DATABASE}_s4"
new_db_with_table "$src" "$long_tbl"
rename_outcome deps-off-boundary-reject "RENAME DATABASE \`$src\` TO \`$mid_a\`" "$CLIENT_NO_DEPS"

src="${CLICKHOUSE_DATABASE}_s5"
new_db_with_table "$src" t0
rename_outcome deps-off-boundary-accept "RENAME DATABASE \`$src\` TO \`$mid_b\`" "$CLIENT_NO_DEPS"

# 6. A detached table is absent from the attached-tables map, so it was never checked. ATTACH
#    does not re-check the length and binds the table to the new database name, so its DROP
#    then builds the oversized metadata_dropped path.
src="${CLICKHOUSE_DATABASE}_s6"
new_db_with_detached_table "$src"
rename_outcome detached-into-long "RENAME DATABASE \`$src\` TO \`$long_c\`" "$CLIENT_NO_DEPS"
reject_postcondition detached-into-long "$src" "$long_c"
# The detached table must also still be bound to the OLD database name. That binding is what the
# snapshot rewrite would corrupt, and the two counts above cannot see it.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.detached_tables WHERE database = '$src' AND table = 't0'"
recover_after_reject detached-into-long "$src"

# 7. The same at DEFAULT settings: the attached-tables map is empty, so the pre-existing
#    dependency-guarded loop had nothing to iterate and the rename was accepted even with
#    check_table_dependencies left at its default.
src="${CLICKHOUSE_DATABASE}_s7"
new_db_with_detached_table "$src"
rename_outcome detached-into-long-deps-on "RENAME DATABASE \`$src\` TO \`$long_d\`" "$CLICKHOUSE_CLIENT"

# 8. Control: the detached check must not over-reject either, and the table stays droppable
#    after being reattached under the new database name.
src="${CLICKHOUSE_DATABASE}_s8"
new_db_with_detached_table "$src"
rename_outcome detached-accept "RENAME DATABASE \`$src\` TO \`$edge_b\`" "$CLIENT_NO_DEPS"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE \`$edge_b\`.t0"
drop_outcome detached-droppable-after-accept "\`$edge_b\`.t0"

# 9/10. A Replicated database delegates renameDatabase to DatabaseAtomic before it writes the
#       new name to ZooKeeper, so it is covered by the same check. Both directions are pinned
#       because the delegation is what makes that true. Each arm gets its own database: on a
#       binary where the reject arm is wrongly accepted, a shared source would already have
#       been renamed away and the accept arm would report an unrelated error instead of its
#       own outcome.
new_replicated_db_with_table "${CLICKHOUSE_DATABASE}_r9"
rename_outcome replicated-into-long "RENAME DATABASE \`${CLICKHOUSE_DATABASE}_r9\` TO \`$repl_long\`" "$CLIENT_NO_DEPS"
reject_postcondition replicated-into-long "${CLICKHOUSE_DATABASE}_r9" "$repl_long"
recover_after_reject replicated-into-long "${CLICKHOUSE_DATABASE}_r9"

new_replicated_db_with_table "${CLICKHOUSE_DATABASE}_r10"
rename_outcome replicated-accept "RENAME DATABASE \`${CLICKHOUSE_DATABASE}_r10\` TO \`$repl_edge\`" "$CLIENT_NO_DEPS"
drop_outcome replicated-droppable-after-accept "\`$repl_edge\`.t0"

# One batched teardown. SYNC everywhere so the Replicated databases release their ZooKeeper paths
# before the script exits. --ignore-error is what makes the batch safe: IF EXISTS only covers an
# absent database, and on a binary that wrongly accepted an oversized rename the DROP of that
# database really does fail, which would otherwise stop the batch and strand every name after it.
teardown=""
for db in "${CLICKHOUSE_DATABASE}_s1" "${CLICKHOUSE_DATABASE}_s1_ok" "$long_a" \
          "${CLICKHOUSE_DATABASE}_s2" "$long_b" \
          "${CLICKHOUSE_DATABASE}_s3" "$edge_a" \
          "${CLICKHOUSE_DATABASE}_s4" "$mid_a" \
          "${CLICKHOUSE_DATABASE}_s5" "$mid_b" \
          "${CLICKHOUSE_DATABASE}_s6" "${CLICKHOUSE_DATABASE}_s6_ok" "$long_c" \
          "${CLICKHOUSE_DATABASE}_s7" "$long_d" \
          "${CLICKHOUSE_DATABASE}_s8" "$edge_b" \
          "${CLICKHOUSE_DATABASE}_r9" "${CLICKHOUSE_DATABASE}_r9_ok" "$repl_long" \
          "${CLICKHOUSE_DATABASE}_r10" "$repl_edge"; do
    teardown="$teardown DROP DATABASE IF EXISTS \`$db\` SYNC;"
done
$CLICKHOUSE_CLIENT --ignore-error -q "$teardown" >/dev/null 2>&1

# Teardown of an oversized database fails on a binary without the fix, which is the bug itself.
# The outcome of every arm is reported on stdout and checked against the reference, so the exit
# status carries no signal of its own.
exit 0
