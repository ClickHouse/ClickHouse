#!/bin/bash
# Create a ClickHouse git worktree whose submodules share storage with the main checkout, or bring the submodules of an
# existing worktree to the commits that its index records. Never runs `git submodule update`, and never clones a
# submodule from the network when the main checkout has it.
#
# Usage:
#   worktree.sh create [--no-submodules] [--fresh-master] <main-repo> <worktree-path> <branch> [<start-point>]
#       Create <worktree-path> on <branch>. An existing local branch is checked out as it is. A new branch starts
#       from <start-point>; without it, from the current `master` of the main checkout, or with --fresh-master from
#       the fresh upstream of `master`. Fetch any other start point before, for example `origin/<branch>`.
#   worktree.sh update-submodules [--fresh-master] <worktree-path>
#       Set up or update the submodules of a worktree: after `create --no-submodules`, after a merge or a checkout that
#       moved pins, or after an interrupted setup. Existing submodule git data of the worktree is kept, with its local
#       branches, stashes and commits; missing git data is copied from the main checkout.
#
# The main checkout: by default, the script uses it as it is. It does not fetch the superproject, and it does not
# change the files of the main checkout. Only on request, with --fresh-master, it fetches the upstream of `master` and
# fast-forwards a clean `master` of the main checkout to it. A submodule of the main checkout then moves to its new pin
# only when nobody works on it: detached at the previous pin, without changes in tracked files. Otherwise the files
# stay as they are and the script says why: other sessions can use the main checkout.
#
# Network: without --fresh-master, the script fetches only submodule commits that no local git data has: from the URL
# in `.gitmodules`, by SHA, into the git data of the main checkout (`.git/modules/*`), where the next worktree finds
# them. A commit fetched by SHA is kept under `refs/pins/<sha>`, so that `git gc` does not remove it.
#
# Storage sharing:
#   - macOS (APFS): the submodule git data and the submodule files are clones (`cp -c`) of the main checkout. A clone
#     shares disk blocks until a file changes, so a new worktree costs about the superproject files plus the files that
#     differ between the pins.
#   - Linux: the submodule object files are hardlinks (`cp -al`). All other files of the git data are real copies, so
#     that a fetch in one worktree (`FETCH_HEAD`, reflogs, index) does not change the others. ext4 has no clones, so
#     the submodule files are checked out.
#
# Environment: SUBMODULE_CHECKOUT_WORKERS parallel checkout workers inside one submodule (default: 2 on macOS, where
# the clones leave little to check out; min(8, CPUs) on Linux); SUBMODULE_JOBS submodules processed at the same time
# (default: CPUs / SUBMODULE_CHECKOUT_WORKERS).

set -euo pipefail

# Submodules with the largest trees start first, so that they do not finish last.
HEAVY="contrib/llvm-project contrib/google-cloud-cpp contrib/aws contrib/openssl contrib/icu contrib/boost
contrib/rust_vendor contrib/sysroot contrib/grpc contrib/arrow contrib/curl contrib/rocksdb contrib/postgres
contrib/wasmtime"

# User configuration must not make git recurse into submodules: the script handles each submodule itself. A recursing
# checkout or merge would touch submodules before their git data exists, or move submodules of the main checkout that
# someone works on. Configuration from the environment overrides all configuration files.
n=${GIT_CONFIG_COUNT:-0}
export GIT_CONFIG_COUNT=$((n + 1)) "GIT_CONFIG_KEY_$n=submodule.recurse" "GIT_CONFIG_VALUE_$n=false"
unset n

# Written into the git data of a submodule when it is copied, removed after its first checkout succeeded. A retry
# finishes every submodule that still has it.
INCOMPLETE=worktree-sh-incomplete

TAB=$(printf '\t')
OS=$(uname -s)
if [ "$OS" = Darwin ]; then CPU_COUNT=$(sysctl -n hw.ncpu); else CPU_COUNT=$(nproc); fi

MAIN=""  # main checkout
GD=""    # its git dir
WT=""    # worktree
WGD=""   # git dir of the worktree, `$GD/worktrees/<name>`
CREATED=""  # what `create` added, for the message on failure
FRESH=0     # 1 with --fresh-master
FRESH_REF="" # the upstream of `master` that --fresh-master fetched
PHASE=""    # of `create`: "parent" until the superproject checkout finished, then "submodules"
STAGING=""  # staging directories that this run created, one per line; only these are ever deleted
STAGE=""    # the last one, set by new_staging
NL=$'\n'

die() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

# Creates a staging directory directly in git dir $1 and sets STAGE to it. The name `worktree-sh-staging.XXXXXX` is
# outside `modules/` and outside the worktree files, so no submodule git data and no tracked file can have it, and
# mktemp makes it unique. A complete result is moved from it into place with one rename.
new_staging()
{
    STAGE=$(mktemp -d "$1/worktree-sh-staging.XXXXXX")
    STAGING="$STAGING$STAGE$NL"
}

# On failure after `create` added the worktree, say what is left and how to continue. Nothing is removed automatically.
on_exit()
{
    local rc=$? d
    # The staging directories of this run hold only copies, or nothing after the rename.
    while IFS= read -r d
    do
        [ -z "$d" ] || rm -rf "$d"
    done <<< "$STAGING"
    if [ "$rc" != 0 ] && [ -n "$CREATED" ]
    then
        printf 'The worktree %s exists (%s).\n' "$WT" "$CREATED" >&2
        if [ "$PHASE" = parent ]
        then
            printf 'Its superproject checkout did not finish. Finish it with:\n' >&2
            printf '    git -C %s -c submodule.recurse=false checkout -f HEAD -- .\n' "$WT" >&2
            printf 'and then, for a build: %s update-submodules %s\n' "$0" "$WT" >&2
        else
            printf 'Finish its setup with: %s update-submodules %s\n' "$0" "$WT" >&2
        fi
        printf 'Or remove it with: git -C %s worktree remove --force %s\n' "$MAIN" "$WT" >&2
    fi
}
trap on_exit EXIT
note() { printf '[%4d s] %s\n' "$SECONDS" "$*"; }

usage()
{
    sed -n '/^# Usage:/,/^#$/p' "$0" | sed 's/^# \{0,1\}//' >&2
    exit 2
}

# Sets MAIN and GD from any path inside the main checkout or one of its worktrees.
locate_main()
{
    local common
    common=$(git -C "$1" rev-parse --path-format=absolute --git-common-dir) || die "not a git checkout: $1"
    [ "$(basename "$common")" = .git ] || die "unexpected git dir: $common"
    GD=$common
    MAIN=$(dirname "$common")
}

# The submodule setup copies the submodule git data of the main checkout, so the main checkout must have it.
require_main_stores()
{
    [ -d "$GD/modules" ] || die "the main checkout $MAIN has no submodules; run \`git submodule update --init\` there once"
}

# Sets WGD for the worktree WT and checks that it belongs to the main checkout.
locate_worktree_git_dir()
{
    WGD=$(git -C "$WT" rev-parse --absolute-git-dir)
    case $WGD in
        "$GD"/worktrees/*) ;;
        *) die "$WT is not a worktree of $MAIN (its git dir is $WGD)" ;;
    esac
}

# Prints "<sha>\t<path>\t<name>\t<url>" for every submodule in the index of checkout $1. The index is used, not HEAD,
# because during a merge HEAD still has the old pins.
gitlinks()
{
    awk -F '\t' '
        function unsafe(x) { return x == "" || x ~ /^[-\/]/ || x ~ /(^|\/)\.\.?(\/|$)/ }
        FNR == NR { if ($2 == "path") name_of[$3] = $1; else url[$1] = $3; next }
        {
            split($1, meta, " ")
            if (meta[3] != "0") { printf "FAILED: %s: unmerged submodule, resolve the conflict first\n", $2 > "/dev/stderr"; exit 1 }
            if (!($2 in name_of)) { printf "FAILED: %s: not in .gitmodules\n", $2 > "/dev/stderr"; exit 1 }
            # Names and paths become directories under the git dir and the worktree: no options, absolute paths, or
            # "." and ".." components (git itself rejects such names).
            n = name_of[$2]
            if (meta[2] !~ /^[0-9a-f]+$/ || unsafe($2) || unsafe(n)) {
                printf "FAILED: unsafe submodule: commit %s, path %s, name %s\n", meta[2], $2, n > "/dev/stderr"; exit 1
            }
            printf "%s\t%s\t%s\t%s\n", meta[2], $2, n, url[n]
        }' \
        <(git -C "$1" config --file .gitmodules --get-regexp '^submodule\..*\.(path|url)$' |
            awk '{ key = $1; sub(/^submodule\./, "", key); kind = key; sub(/^.*\./, "", kind); sub(/\.[^.]*$/, "", key)
                   printf "%s\t%s\t%s\n", key, kind, $2 }') \
        <(git -C "$1" ls-files -s | awk '$1 == "160000"')
}

# Prints the gitlink list $1 with the heavy submodules first.
heavy_first()
{
    awk -F '\t' -v heavy="$(tr '\n' ' ' <<< "$HEAVY")" '
        BEGIN { n = split(heavy, h, " "); for (i = 1; i <= n; ++i) rank[h[i]] = i }
        { printf "%d\t%s\n", ($2 in rank) ? rank[$2] : n + 1, $0 }' <<< "$1" |
        sort -t "$TAB" -k1,1n -s | cut -f2-
}

# Copies submodule git data $1 to $2, which must not exist, and writes the marker of the first checkout into it. See
# "Storage sharing" above. The copy is made in a staging directory and renamed to $2 only when it is complete: an
# interrupted copy must not look like existing git data.
copy_store()
{
    local stage
    new_staging "$WGD"
    stage=$STAGE/git
    if [ "$OS" = Darwin ]
    then
        cp -Rc "$1" "$stage"
    else
        cp -al "$1" "$stage"
        # Keep only the object files of this git data shared. `find` from inside it, and prune exactly ./objects:
        # `-path` tests the whole path, so a pattern like */objects/* also matches an `objects` directory above it.
        (cd "$stage" && find . -path ./objects -prune -o -type f -links +1 -exec sh -c '
            for f do cp -p "$f" "$f.unshare" && mv -f "$f.unshare" "$f" || exit 1; done' sh {} +)
    fi
    : > "$stage/$INCOMPLETE"
    mv "$stage" "$2"
}

# Runs an object-only git command (cat-file, fetch) on submodule git data $1. The work tree is set to the git data
# itself, because core.worktree can point at a directory that does not exist: git would refuse to run.
git_store()
{
    local store=$1
    shift
    git --git-dir="$store" --work-tree="$store" "$@"
}

# Fetches commit $3 from $2 (a URL, or the path of other git data) into submodule git data $1, keeps it under
# refs/pins/<sha>, and checks the result. The source can be shallow (`contrib/update-submodules.sh` uses --depth=1):
# --update-shallow moves the shallow boundary with the commit. Without it, git refuses the ref but still exits with 0,
# and the history of the commit is broken.
fetch_pin()
{
    git_store "$1" fetch -q --update-shallow -- "$2" "$3:refs/pins/$3" || die "fetch of $3 from $2 into $1"
    [ "$(git_store "$1" rev-parse -q --verify "refs/pins/$3^{commit}" || true)" = "$3" ] ||
        die "fetch of $3 from $2 into $1: the commit did not arrive"
    git_store "$1" rev-list --max-count=2 "$3" > /dev/null || die "fetch of $3 from $2 into $1: the history is broken"
}

# Points submodule git data $1 at the submodule files in $2. A few modules keep core.worktree in config.worktree.
set_module_worktree()
{
    local f found=0
    for f in "$1/config.worktree" "$1/config"
    do
        if [ -f "$f" ] && git config --file "$f" --get core.worktree > /dev/null
        then
            git config --file "$f" core.worktree "$2"
            found=1
        fi
    done
    [ "$found" = 1 ] || git config --file "$1/config" core.worktree "$2"
}

# Makes every commit of the gitlink list $1 available in the main git data. With $2 (the git dir of a worktree), commits
# that the git data of that worktree already has are skipped, for example a local commit in one of its submodules.
refresh_main_stores()
{
    local wgd=${2:-} sha path name url store up
    while IFS="$TAB" read -r sha path name url
    do
        if [ -n "$wgd" ] && [ -d "$wgd/modules/$name" ] &&
            git_store "$wgd/modules/$name" cat-file -e "$sha^{commit}" 2> /dev/null
        then
            continue
        fi
        # The URL comes from the branch of the worktree, which can be someone else's: allow only plain repository URLs.
        case $url in
            https://* | ssh://* | file://* | /* | git@[A-Za-z0-9]*:*) ;;
            *) die "$path: unsupported submodule URL: $url (allowed: https://, ssh://, git@host:path, file://, absolute path)" ;;
        esac
        store=$GD/modules/$name
        if [ ! -d "$store" ]
        then
            note "main git data: cloning new submodule $path"
            mkdir -p "$(dirname "$store")"
            # As in copy_store: clone into a staging directory, rename the git data when the clone is complete.
            new_staging "$GD"
            git clone -q --no-checkout --separate-git-dir="$STAGE/git" -- "$url" "$STAGE/files"
            # The same relative path that `git submodule update --init` writes, so that the main checkout can init it.
            up=$(printf '%s' "modules/$name" | awk -F / '{ for (i = 0; i <= NF; ++i) printf "../" }')
            git config --file "$STAGE/git/config" core.worktree "$up$path"
            mv "$STAGE/git" "$store"
        fi
        if ! git_store "$store" cat-file -e "$sha^{commit}" 2> /dev/null
        then
            note "main git data: fetching $path $sha from $url"
            fetch_pin "$store" "$url" "$sha"
        fi
    done <<< "$1"
}

# For --fresh-master: fetches the upstream of master and sets FRESH_REF to it. Then fast-forwards the main checkout and
# its submodules to it, when the main checkout is on a clean master. The user asked for a fresh master, so a missing
# upstream or a failed fetch fails the run.
refresh_main_worktree()
{
    local branch f upstream old_head old_list list sha path name url cur old_sha moved=0 left=""
    upstream=$(git -C "$MAIN" rev-parse --abbrev-ref --symbolic-full-name 'master@{upstream}' 2> /dev/null || true)
    [ -n "$upstream" ] || die "--fresh-master: master of the main checkout has no upstream"
    git -C "$MAIN" fetch -q --no-recurse-submodules "${upstream%%/*}" "${upstream#*/}" ||
        die "--fresh-master: the fetch of $upstream failed (see above)"
    FRESH_REF=$upstream
    note "main checkout: fetched $upstream at $(git -C "$MAIN" rev-parse --short=12 "$upstream")"
    branch=$(git -C "$MAIN" symbolic-ref -q --short HEAD || true)
    if [ "$branch" != master ]
    then
        note "main checkout: on '${branch:-detached HEAD}', not master; its files stay as they are"
        return 0
    fi
    for f in MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD rebase-merge rebase-apply
    do
        if [ -e "$GD/$f" ]
        then
            note "main checkout: $f exists; its files stay as they are"
            return 0
        fi
    done
    # Changes in submodules are checked per submodule below: on macOS, contrib/liburing and contrib/sysroot always
    # show changes. A staged gitlink is a change of the main checkout itself.
    if [ -n "$(git -C "$MAIN" status --porcelain --untracked-files=no --ignore-submodules=all)" ] ||
        ! git -C "$MAIN" diff --cached --quiet --ignore-submodules=none
    then
        note "main checkout: master has local changes; its files stay as they are"
        return 0
    fi
    if ! git -C "$MAIN" merge-base --is-ancestor HEAD "$upstream"
    then
        note "main checkout: master has commits that $upstream does not have; its files stay as they are"
        return 0
    fi
    old_head=$(git -C "$MAIN" rev-parse HEAD)
    if [ "$old_head" = "$(git -C "$MAIN" rev-parse "$upstream")" ]
    then
        note "main checkout: master is up to date with $upstream at ${old_head:0:12}"
        return 0
    fi
    old_list=$(gitlinks "$MAIN")
    # Without --no-overwrite-ignore, git replaces an ignored local file when the new commit tracks its path.
    if ! git -C "$MAIN" merge -q --no-progress --ff-only --no-overwrite-ignore "$upstream"
    then
        note "main checkout: the fast-forward to $upstream was refused (see above); its files stay as they are"
        return 0
    fi
    note "main checkout: master ${old_head:0:12} -> $(git -C "$MAIN" rev-parse --short=12 HEAD) ($upstream)"

    list=$(gitlinks "$MAIN")
    while IFS="$TAB" read -r sha path name url
    do
        [ -f "$MAIN/$path/.git" ] || continue  # not initialized in the main checkout
        old_sha=$(awk -F '\t' -v p="$path" '$2 == p { print $1 }' <<< "$old_list")
        [ "$old_sha" != "$sha" ] || continue  # the pin did not move
        cur=$(git -C "$MAIN/$path" rev-parse HEAD)
        [ "$cur" != "$sha" ] || continue
        # Move only a submodule that nobody works on: detached at the previous pin, without changes in tracked
        # files. An ignored local file that the new pin tracks also keeps it where it is (--no-overwrite-ignore).
        if git -C "$MAIN/$path" symbolic-ref -q HEAD > /dev/null || [ "$cur" != "$old_sha" ] ||
            ! git -C "$MAIN/$path" diff --quiet HEAD --
        then
            left="$left $path"
            continue
        fi
        refresh_main_stores "$(printf '%s\t%s\t%s\t%s' "$sha" "$path" "$name" "$url")"
        if git -C "$MAIN/$path" -c advice.detachedHead=false checkout -q --no-overwrite-ignore --detach "$sha"
        then
            moved=$((moved + 1))
        else
            left="$left $path"
        fi
    done <<< "$list"
    note "main checkout: $moved submodules moved to new pins"
    if [ -n "$left" ]
    then
        note "WARNING: main checkout: these submodules stay as they are (on a branch, not at the previous pin, or with" \
            "local changes):$left"
    fi
}

# Runs for one submodule in parallel with the others.
# Arguments: worktree, main checkout, OS, checkout workers, name of the marker file, git dir of the worktree, submodule
# git data, commit, path.
# shellcheck disable=SC2016 # Expanded by the inner shell.
WORKER='
    wt=$1 main=$2 os=$3 workers=$4 incomplete=$5 wgd=$6 store=$7 sha=$8 path=$9
    dst=$wt/$path
    marker=$store/$incomplete
    opts="-c advice.detachedHead=false -c checkout.workers=$workers -c checkout.thresholdForParallelism=100"
    opts="$opts -c index.threads=true -c core.fsync=none -c gc.auto=0"
    if [ -f "$marker" ] || [ ! -f "$dst/.git" ]
    then
        # The first checkout of this submodule, or the resume of an interrupted one: the marker is still there.
        if [ ! -f "$marker" ] && [ -d "$dst" ] && [ -n "$(ls -A "$dst")" ]
        then
            echo "FAILED: $path: not empty and not a submodule checkout" >&2
            exit 1
        fi
        : > "$marker" || exit 1
        if [ ! -d "$dst" ] || [ -z "$(ls -A "$dst")" ]
        then
            if [ "$os" = Darwin ] && [ -f "$main/$path/.git" ]
            then
                # Clone the files of the main checkout into a staging directory of this worker, give them the .git
                # file of this worktree, and move them into place only then: an interrupted clone must not leave the
                # .git file of the main checkout at the destination. Only this staging directory is ever deleted.
                stage=$(mktemp -d "$wgd/worktree-sh-staging.XXXXXX") || exit 1
                if cp -Rcp "$main/$path" "$stage/files" && printf "gitdir: %s\n" "$store" > "$stage/files/.git" &&
                    { [ ! -d "$dst" ] || rmdir "$dst"; } && mv "$stage/files" "$dst"
                then
                    rmdir "$stage"
                else
                    rm -rf "$stage"
                    echo "FAILED: $path: clone of the main checkout files" >&2
                    exit 1
                fi
            else
                mkdir -p "$dst" || exit 1
            fi
        fi
        printf "gitdir: %s\n" "$store" > "$dst/.git" || exit 1
        top=$(git -C "$dst" rev-parse --show-toplevel) || exit 1
        [ "$top" = "$dst" ] || { echo "FAILED: $path: its git data points at $top" >&2; exit 1; }
        if [ "$os" = Darwin ]
        then
            # Clones have new inodes. Refresh the stat data in the index, so that the checkout below writes only the
            # files that differ, and the other files keep the shared blocks.
            git -C "$dst" status --porcelain --untracked-files=no > /dev/null || exit 1
        fi
        # shellcheck disable=SC2086 # opts is a list of options.
        git -C "$dst" $opts checkout -q -f --detach "$sha" || { echo "FAILED: $path: checkout of $sha" >&2; exit 1; }
        rm -f "$marker" || exit 1
    else
        top=$(git -C "$dst" rev-parse --show-toplevel) || exit 1
        [ "$top" = "$dst" ] || { echo "FAILED: $path: its git data points at $top" >&2; exit 1; }
        [ "$(git -C "$dst" rev-parse HEAD)" != "$sha" ] || exit 0
        # No -f, and --no-overwrite-ignore: if local changes or ignored local files are in the way, fail and leave the
        # decision to the user.
        # shellcheck disable=SC2086
        git -C "$dst" $opts checkout -q --no-overwrite-ignore --detach "$sha" ||
            { echo "FAILED: $path: checkout of $sha; are local changes or ignored files in the way?" >&2; exit 1; }
    fi
'

# Materializes or updates every submodule of the gitlink list $1 in WT.
materialize()
{
    local workers jobs sha path name url
    if [ "$OS" = Darwin ]; then workers=${SUBMODULE_CHECKOUT_WORKERS:-2}; else workers=${SUBMODULE_CHECKOUT_WORKERS:-8}; fi
    [ "$workers" -le "$CPU_COUNT" ] || workers=$CPU_COUNT
    [ "$workers" -ge 1 ] || die "SUBMODULE_CHECKOUT_WORKERS must be positive"
    jobs=${SUBMODULE_JOBS:-$((CPU_COUNT / workers))}
    [ "$jobs" -ge 1 ] || jobs=1
    heavy_first "$1" |
        while IFS="$TAB" read -r sha path name url
        do
            printf '%s\0%s\0%s\0' "$WGD/modules/$name" "$sha" "$path"
        done |
        xargs -0 -n 3 -P "$jobs" sh -c "$WORKER" sh "$WT" "$MAIN" "$OS" "$workers" "$INCOMPLETE" "$WGD" ||
        die "some submodules were not set up (see the FAILED lines above)"
}

# Checks that every submodule of the gitlink list $1 is at its recorded commit, and reports modified submodules.
verify()
{
    local sha path name url cur bad="" n=0 dirty d left
    while IFS="$TAB" read -r sha path name url
    do
        n=$((n + 1))
        cur=$(git -C "$WT/$path" rev-parse HEAD 2> /dev/null || echo none)
        [ "$cur" = "$sha" ] && [ ! -e "$WGD/modules/$name/$INCOMPLETE" ] || bad="$bad $path"
    done <<< "$1"
    [ -z "$bad" ] || die "not at the recorded commit, or the first checkout did not finish:$bad"
    note "submodules: all $n at the recorded commits"
    left=""
    for d in "$GD"/worktree-sh-staging.* "$WGD"/worktree-sh-staging.*
    do
        [ -e "$d" ] || continue
        case "$NL$STAGING" in
            *"$NL$d$NL"*) continue ;;
        esac
        left="$left $d"
    done
    if [ -n "$left" ]
    then
        note "staging directories of an interrupted earlier run (only copies; remove them when no other run of this" \
            "script is active):$left"
    fi
    dirty=$(git -C "$WT" status --porcelain --ignore-submodules=untracked |
        awk -v paths="$(cut -f2 <<< "$1" | tr '\n' ' ')" '
            BEGIN { n = split(paths, p, " "); for (i = 1; i <= n; ++i) is_sub[p[i]] = 1 }
            ($2 in is_sub) { printf " %s", $2 }')
    if [ -n "$dirty" ]
    then
        note "submodules with modified files:$dirty"
        if [ "$OS" = Darwin ]
        then
            note "(on macOS, contrib/liburing and contrib/sysroot always show this: they have file names that differ only in case)"
        fi
    fi
}

# Checks, before any fetch or copy, that the files of each submodule of the gitlink list $1 are safe to touch. A checked
# out submodule must use the git data under this worktree. A directory without a checkout must be empty, unless an
# interrupted first checkout of this script left it (the marker). Otherwise the files are someone's work: stop, and
# change nothing.
check_destinations()
{
    local sha path name url dst store actual bad=""
    while IFS="$TAB" read -r sha path name url
    do
        dst=$WT/$path
        store=$WGD/modules/$name
        if [ -e "$dst/.git" ]
        then
            actual=$(git -C "$dst" rev-parse --absolute-git-dir 2> /dev/null) || actual=""
            if [ -z "$actual" ] || [ ! -d "$store" ] || [ "$(cd "$actual" && pwd -P)" != "$(cd "$store" && pwd -P)" ]
            then
                bad="$bad
    $path: checked out with git data '${actual:-unknown}', but its git data under this worktree is $store"
            fi
        elif [ -d "$dst" ] && [ -n "$(ls -A "$dst")" ] && [ ! -e "$store/$INCOMPLETE" ]
        then
            bad="$bad
    $path: has files, but is not a submodule checkout"
        fi
    done <<< "$1"
    [ -z "$bad" ] || die "these submodule directories were not set up by this script; nothing was changed:$bad"
}

# Sets up or updates every submodule of WT. Existing submodule git data of the worktree is kept: it can have local
# branches, stashes and commits that exist nowhere else. Missing git data is copied from the main checkout; only for
# the submodules of this worktree, because git data of other submodules would keep a core.worktree that points at the
# main checkout.
sync_submodules()
{
    local list sha path name url store copied=0 kept=0
    require_main_stores
    list=$(gitlinks "$WT")
    [ -n "$list" ] || die "no submodules recorded in $WT"
    check_destinations "$list"
    refresh_main_stores "$list" "$WGD"
    while IFS="$TAB" read -r sha path name url
    do
        store=$WGD/modules/$name
        if [ -d "$store" ]
        then
            kept=$((kept + 1))
        else
            mkdir -p "$(dirname "$store")"
            copy_store "$GD/modules/$name" "$store"
            copied=$((copied + 1))
        fi
        # The git data under this worktree belongs to it. This also repairs git data that points at another checkout.
        set_module_worktree "$store" "$WT/$path"
        if ! git_store "$store" cat-file -e "$sha^{commit}" 2> /dev/null
        then
            fetch_pin "$store" "$GD/modules/$name" "$sha"
        fi
    done <<< "$list"
    note "submodule git data: $copied copied from the main checkout, $kept kept"
    git -C "$WT" submodule --quiet init
    materialize "$list"
    note "submodule files: done"
    verify "$list"
}

cmd_create()
{
    local submodules=1 path branch start
    while [ $# -gt 0 ]
    do
        case $1 in
            --no-submodules) submodules=0; shift ;;
            --fresh-master) FRESH=1; shift ;;
            -*) usage ;;
            *) break ;;
        esac
    done
    [ $# -ge 3 ] && [ $# -le 4 ] || usage
    locate_main "$1"
    path=$2 branch=$3 start=${4:-}
    [ ! -e "$path" ] || die "$path exists"
    mkdir -p "$(dirname "$path")"
    WT=$(cd "$(dirname "$path")" && pwd -P)/$(basename "$path")
    if [ "$FRESH" = 1 ]; then refresh_main_worktree; fi
    if git -C "$MAIN" show-ref -q --verify "refs/heads/$branch"
    then
        [ -z "$start" ] || die "branch $branch exists; do not give a start point"
        git -C "$MAIN" worktree add -q --no-checkout "$WT" "$branch"
        CREATED="branch $branch"
    else
        if [ -z "$start" ]
        then
            if [ "$FRESH" = 1 ]; then start=$FRESH_REF; else start=master; fi
            git -C "$MAIN" rev-parse -q --verify "$start^{commit}" > /dev/null || die "start point $start does not exist"
            note "start point: $start at $(git -C "$MAIN" rev-parse --short=12 "$start")"
        fi
        git -C "$MAIN" worktree add -q --no-checkout -b "$branch" "$WT" "$start"
        CREATED="new branch $branch; delete it with: git -C $MAIN branch -D $branch"
    fi
    PHASE=parent
    git -C "$WT" -c checkout.workers=0 -c core.fsync=none -c gc.auto=0 checkout -q -f HEAD -- .
    PHASE=submodules
    note "worktree: $WT on $branch at $(git -C "$WT" rev-parse --short HEAD)"
    if [ "$submodules" = 0 ]
    then
        note "submodules: skipped; run \`$0 update-submodules $WT\` before a build"
        return 0
    fi
    locate_worktree_git_dir
    sync_submodules
}

cmd_update_submodules()
{
    if [ "${1:-}" = --fresh-master ]; then FRESH=1; shift; fi
    [ $# -eq 1 ] || usage
    locate_main "$1"
    WT=$(cd "$1" && pwd -P)
    [ "$(git -C "$WT" rev-parse --show-toplevel)" = "$WT" ] || die "give the top directory of the worktree"
    locate_worktree_git_dir
    if [ "$FRESH" = 1 ]; then refresh_main_worktree; fi
    sync_submodules
}

case ${1:-} in
    create) shift; cmd_create "$@" ;;
    update-submodules) shift; cmd_update_submodules "$@" ;;
    *) usage ;;
esac
