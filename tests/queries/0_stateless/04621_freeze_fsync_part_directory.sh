#!/usr/bin/env bash
# Tags: no-object-storage
# Tag no-object-storage: freeze on object storage does not fsync local directories

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ALTER TABLE ... FREEZE must fsync the snapshot's directories when fsync_part_directory is set,
# otherwise a power loss right after the acknowledgement erases the snapshot (only
# shadow/increment.txt survived before the fix). We observe the fsyncs issued by the FREEZE query
# via ProfileEvents in query_log: DirectorySync (directory entries) and FileSync (file contents).
#
# Both part storage layouts are exercised explicitly (Full and Packed have separate freeze
# overrides), pinning min_bytes_for_full_part_storage so the layout is not left to the CI setting
# randomizer:
#   * DirectorySync: the expected count is derived from the part's on-disk path depth so the test
#     asserts the *complete* chain -- the part dir plus every ancestor up to and including the disk
#     root -- rather than a lower bound that could pass while the disk root (which makes the
#     shadow/ entry durable) is skipped. A projection adds exactly one more synced directory.
#   * FileSync: Packed freeze rewrites data.packed (a fresh archive, not a hardlink), so its
#     contents must be fsynced too. We assert the FileSync delta between the setting-on and
#     setting-off runs equals the number of rewritten archives (1 without a projection, 2 with
#     one: the main part plus the projection). Full storage hardlinks its files and rewrites no
#     archive, so its FileSync delta is 0.

# $1 = fsync_part_directory, $2 = "proj" to add a projection, $3 = min_bytes_for_full_part_storage
# (0 forces Full storage, a large value forces Packed). Prints "<DirectorySync> <FileSync> <expected_dirsync>".
run_freeze() {
    local fpd=$1 proj=$2 min_full=$3
    local tag="${CLICKHOUSE_TEST_UNIQUE_NAME}_${fpd}_${proj}_${min_full}_${RANDOM}${RANDOM}"
    local projection=""
    [[ "$proj" == "proj" ]] && projection=", PROJECTION p (SELECT v, count() GROUP BY v)"
    $CLICKHOUSE_CLIENT -m -q "
        drop table if exists freeze_fsync;
        create table freeze_fsync (id UInt64, v UInt64${projection}) engine=MergeTree order by id
            settings fsync_part_directory = ${fpd}, min_bytes_for_full_part_storage = ${min_full};
        insert into freeze_fsync select number, number % 10 from numbers(1000);
    "
    # Expected number of synced directories = part-path depth relative to the disk root
    #   (e.g. store/<prefix>/<uuid>/<part>) + 2 for the shadow/<name> prefix the snapshot adds
    #   + 1 for the disk root itself, + 1 more when a projection subdir is present.
    local proj_extra=0
    [[ "$proj" == "proj" ]] && proj_extra=1
    local expected
    expected=$($CLICKHOUSE_CLIENT -q "
        WITH (SELECT path FROM system.disks WHERE name = 'default') AS root,
             (SELECT path FROM system.parts
              WHERE database = currentDatabase() AND table = 'freeze_fsync' AND active LIMIT 1) AS pp
        SELECT length(splitByChar('/', trim(BOTH '/' FROM replaceOne(pp, root, '')))) + 2 + 1 + ${proj_extra}
    ")
    $CLICKHOUSE_CLIENT --query_id "$tag" -q "alter table freeze_fsync freeze with name '${tag}'"
    $CLICKHOUSE_CLIENT -q "system flush logs query_log"
    $CLICKHOUSE_CLIENT --param_query_id "$tag" -q "
        select max(ProfileEvents['DirectorySync']), max(ProfileEvents['FileSync']), ${expected}
        from system.query_log
        where event_date >= yesterday() and event_time >= now() - 600
          and current_database = currentDatabase()
          and query_id = {query_id:String}
          and type = 'QueryFinish'
        format TSV;
    "
    $CLICKHOUSE_CLIENT -q "drop table freeze_fsync"
}

# Two storage layouts, each with its own freeze override: Full (min_bytes = 0) and
# Packed (min_bytes above the tiny part size). Both must behave identically for directories.
for storage in "full 0" "packed 1000000000"; do
    read -r name min_full <<< "$storage"

    read -r on_ds on_fs exp_plain <<< "$(run_freeze 1 plain "$min_full")"
    read -r on_proj_ds on_proj_fs exp_proj <<< "$(run_freeze 1 proj "$min_full")"
    read -r off_ds off_fs _ <<< "$(run_freeze 0 plain "$min_full")"
    read -r off_proj_ds off_proj_fs _ <<< "$(run_freeze 0 proj "$min_full")"

    # Setting on: the whole ancestor chain up to (and including) the disk root must be synced, so the
    # observed count equals the path-derived expected count exactly (not merely a lower bound).
    if [[ "$on_ds" -eq "$exp_plain" ]]; then
        echo "${name} on: full ancestor chain synced"
    else
        echo "${name} on: full ancestor chain synced FAILED (DirectorySync=$on_ds, expected $exp_plain)"
    fi

    # A projection adds exactly one more synced directory (its <name>.proj subdir).
    if [[ "$on_proj_ds" -eq "$exp_proj" ]]; then
        echo "${name} proj: projection subtree synced"
    else
        echo "${name} proj: projection subtree synced FAILED (DirectorySync=$on_proj_ds, expected $exp_proj)"
    fi

    # With fsync_part_directory = 0 (the default) behavior is unchanged: no directory fsync. Both
    # layouts are checked, so gating the part dir while syncing projection subdirs unconditionally
    # cannot pass.
    if [[ "$off_ds" -eq 0 && "$off_proj_ds" -eq 0 ]]; then
        echo "${name} off: DirectorySync = 0"
    else
        echo "${name} off: DirectorySync = $off_ds (expected 0), with projection = $off_proj_ds (expected 0)"
    fi

    # Content durability: Packed freeze rewrites data.packed, so enabling the setting must fsync one
    # more file (the rewritten archive) without a projection and two more with one (main + projection).
    # Full storage hardlinks its files and rewrites no archive, so its FileSync delta is 0.
    # The setting-off baseline is asserted too, not only the delta: a build that fsyncs the archive
    # unconditionally and adds the requested sync on top would show a correct delta off a wrong base.
    # Freeze itself writes only increment.txt, so the baseline is 1 for either layout.
    plain_delta=$((on_fs - off_fs))
    proj_delta=$((on_proj_fs - off_proj_fs))
    if [[ "$name" == "packed" ]]; then
        exp_plain_delta=1
        exp_proj_delta=2
    else
        exp_plain_delta=0
        exp_proj_delta=0
    fi
    if [[ "$off_fs" -eq 1 && "$off_proj_fs" -eq 1 ]]; then
        echo "${name} off content: baseline FileSync = 1"
    else
        echo "${name} off content: baseline FileSync = $off_fs / with projection $off_proj_fs (expected 1 and 1)"
    fi
    if [[ "$plain_delta" -eq "$exp_plain_delta" ]]; then
        echo "${name} content: data archive synced"
    else
        echo "${name} content: data archive synced FAILED (FileSync on=$on_fs off=$off_fs delta=$plain_delta, expected $exp_plain_delta)"
    fi
    if [[ "$proj_delta" -eq "$exp_proj_delta" ]]; then
        echo "${name} proj content: projection archive synced"
    else
        echo "${name} proj content: projection archive synced FAILED (FileSync on=$on_proj_fs off=$off_proj_fs delta=$proj_delta, expected $exp_proj_delta)"
    fi
done
