#!/bin/bash
# Create a ClickHouse git worktree whose submodules share storage with the main checkout, or bring the submodules of an
# existing worktree to the commits that its index records. Never runs `git submodule update`, and never clones a
# submodule from the network when the main checkout has it.
#
# Usage:
#   worktree.sh create [--no-submodules] <main-repo> <worktree-path> <branch> [<start-point>]
#       Create <worktree-path> on <branch>. An existing local branch is checked out as it is. A new branch needs
#       <start-point>, for example `origin/master` or `origin/<branch>`; fetch it before.
#   worktree.sh add-submodules <worktree-path>
#       Set up the submodules of a worktree that was created with --no-submodules.
#   worktree.sh update-submodules <worktree-path>
#       After a merge or a checkout moved submodule pins in the worktree, check out the recorded commits.
#
# Every mode first refreshes the main checkout:
#   - Its files and submodules are fast-forwarded to the upstream of `master`, but only when it is on a clean `master`.
#     Otherwise they stay as they are and the script says why: other sessions can use the main checkout.
#   - Its git data (`.git/modules/*`) gets every submodule commit that the worktree needs. The network is used only for
#     missing commits. A commit fetched by SHA is kept under `refs/pins/<sha>`, so that `git gc` does not remove it.
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

TAB=$(printf '\t')
OS=$(uname -s)
if [ "$OS" = Darwin ]; then CPU_COUNT=$(sysctl -n hw.ncpu); else CPU_COUNT=$(nproc); fi

MAIN=""  # main checkout
GD=""    # its git dir
WT=""    # worktree
WGD=""   # git dir of the worktree, `$GD/worktrees/<name>`

die() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
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
        FNR == NR { if ($2 == "path") name_of[$3] = $1; else url[$1] = $3; next }
        {
            split($1, meta, " ")
            if (meta[3] != "0") { printf "FAILED: %s: unmerged submodule, resolve the conflict first\n", $2 > "/dev/stderr"; exit 1 }
            if (!($2 in name_of)) { printf "FAILED: %s: not in .gitmodules\n", $2 > "/dev/stderr"; exit 1 }
            printf "%s\t%s\t%s\t%s\n", meta[2], $2, name_of[$2], url[name_of[$2]]
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

# Copies submodule git data $1 to $2, which must not exist. See "Storage sharing" above.
copy_store()
{
    if [ "$OS" = Darwin ]
    then
        cp -Rc "$1" "$2"
    else
        cp -al "$1" "$2"
        find "$2" -type f -links +1 ! -path '*/objects/*' -exec sh -c '
            for f do cp -p "$f" "$f.unshare" && mv -f "$f.unshare" "$f" || exit 1; done' sh {} +
    fi
}

# Runs an object-only git command (cat-file, fetch) on submodule git data $1. The work tree is set to the git data
# itself, because core.worktree can point at a directory that does not exist: git would refuse to run.
git_store()
{
    local store=$1
    shift
    git --git-dir="$store" --work-tree="$store" "$@"
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

# Makes the main git data contain every commit of the gitlink list $1.
refresh_main_stores()
{
    local sha path name url store tmp up
    while IFS="$TAB" read -r sha path name url
    do
        store=$GD/modules/$name
        if [ ! -d "$store" ]
        then
            note "main git data: cloning new submodule $path"
            mkdir -p "$(dirname "$store")"
            tmp=$(mktemp -d "$GD/worktree-sh.XXXXXX")
            git clone -q --no-checkout --separate-git-dir="$store" "$url" "$tmp/files"
            rm -rf "$tmp"
            # The same relative path that `git submodule update --init` writes, so that the main checkout can init it.
            up=$(printf '%s' "modules/$name" | awk -F / '{ for (i = 0; i <= NF; ++i) printf "../" }')
            git config --file "$store/config" core.worktree "$up$path"
        fi
        if ! git_store "$store" cat-file -e "$sha^{commit}" 2> /dev/null
        then
            note "main git data: fetching $path $sha"
            git_store "$store" fetch -q origin "$sha:refs/pins/$sha"
        fi
    done <<< "$1"
}

# Fast-forwards the main checkout and its submodules to the upstream of master, when it is on a clean master.
refresh_main_worktree()
{
    local branch f upstream old list sha path name url cur moved=0
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
    if [ -n "$(git -C "$MAIN" status --porcelain --untracked-files=no --ignore-submodules=all)" ]
    then
        note "main checkout: master has local changes; its files stay as they are"
        return 0
    fi
    upstream=$(git -C "$MAIN" rev-parse --abbrev-ref --symbolic-full-name '@{upstream}' 2> /dev/null || true)
    if [ -z "$upstream" ]
    then
        note "main checkout: master has no upstream; its files stay as they are"
        return 0
    fi
    git -C "$MAIN" fetch -q --no-recurse-submodules "${upstream%%/*}" "${upstream#*/}"
    if ! git -C "$MAIN" merge-base --is-ancestor HEAD "$upstream"
    then
        note "main checkout: master has commits that $upstream does not have; its files stay as they are"
        return 0
    fi
    old=$(git -C "$MAIN" rev-parse --short HEAD)
    if [ "$(git -C "$MAIN" rev-parse HEAD)" = "$(git -C "$MAIN" rev-parse "$upstream")" ]
    then
        note "main checkout: master is up to date with $upstream at $old"
    else
        git -C "$MAIN" merge -q --no-progress --ff-only "$upstream"
        note "main checkout: master $old -> $(git -C "$MAIN" rev-parse --short HEAD) ($upstream)"
    fi

    list=$(gitlinks "$MAIN")
    refresh_main_stores "$list"
    while IFS="$TAB" read -r sha path name url
    do
        [ -f "$MAIN/$path/.git" ] || continue  # not initialized in the main checkout
        cur=$(git -C "$MAIN/$path" rev-parse HEAD)
        [ "$cur" != "$sha" ] || continue
        if git -C "$MAIN/$path" -c advice.detachedHead=false checkout -q --detach "$sha"
        then
            moved=$((moved + 1))
        else
            note "WARNING: main checkout: $path stays at $cur, because the checkout of $sha failed (see above)"
        fi
    done <<< "$list"
    note "main checkout: $moved submodules moved to new pins"
}

# Runs for one submodule in parallel with the others.
# Arguments: worktree, main checkout, OS, checkout workers, submodule git data, commit, path.
# shellcheck disable=SC2016 # Expanded by the inner shell.
WORKER='
    wt=$1 main=$2 os=$3 workers=$4 store=$5 sha=$6 path=$7
    dst=$wt/$path
    opts="-c advice.detachedHead=false -c checkout.workers=$workers -c checkout.thresholdForParallelism=100"
    opts="$opts -c index.threads=true -c core.fsync=none -c gc.auto=0"
    if [ ! -f "$dst/.git" ]
    then
        if [ -d "$dst" ] && [ -n "$(ls -A "$dst")" ]
        then
            echo "FAILED: $path: not empty and not a submodule checkout" >&2
            exit 1
        fi
        if [ "$os" = Darwin ] && [ -f "$main/$path/.git" ]
        then
            if [ -d "$dst" ]; then rmdir "$dst" || exit 1; fi
            cp -Rcp "$main/$path" "$dst" || { echo "FAILED: $path: clone of the main checkout files" >&2; exit 1; }
            printf "gitdir: %s\n" "$store" > "$dst/.git" || exit 1
            # The clones have new inodes. Refresh the stat data in the index, so that the checkout below writes only
            # the files that differ, and the other files keep the shared blocks.
            git -C "$dst" status --porcelain --untracked-files=no > /dev/null || exit 1
        else
            mkdir -p "$dst" && printf "gitdir: %s\n" "$store" > "$dst/.git" || exit 1
        fi
        top=$(git -C "$dst" rev-parse --show-toplevel) || exit 1
        [ "$top" = "$dst" ] || { echo "FAILED: $path: its git data points at $top" >&2; exit 1; }
        # shellcheck disable=SC2086 # opts is a list of options.
        git -C "$dst" $opts checkout -q -f --detach "$sha" || { echo "FAILED: $path: checkout of $sha" >&2; exit 1; }
    else
        top=$(git -C "$dst" rev-parse --show-toplevel) || exit 1
        [ "$top" = "$dst" ] || { echo "FAILED: $path: its git data points at $top" >&2; exit 1; }
        [ "$(git -C "$dst" rev-parse HEAD)" != "$sha" ] || exit 0
        # No -f: if local changes are in the way, fail and leave the decision to the user.
        # shellcheck disable=SC2086
        git -C "$dst" $opts checkout -q --detach "$sha" ||
            { echo "FAILED: $path: checkout of $sha; are local changes in the way?" >&2; exit 1; }
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
        xargs -0 -n 3 -P "$jobs" sh -c "$WORKER" sh "$WT" "$MAIN" "$OS" "$workers" ||
        die "some submodules were not set up (see the FAILED lines above)"
}

# Checks that every submodule of the gitlink list $1 is at its recorded commit, and reports modified submodules.
verify()
{
    local sha path name url cur bad="" n=0 dirty
    while IFS="$TAB" read -r sha path name url
    do
        n=$((n + 1))
        cur=$(git -C "$WT/$path" rev-parse HEAD 2> /dev/null || echo none)
        [ "$cur" = "$sha" ] || bad="$bad $path"
    done <<< "$1"
    [ -z "$bad" ] || die "not at the recorded commit:$bad"
    note "submodules: all $n at the recorded commits"
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

# Sets up all submodules of WT, which has none yet.
setup_submodules()
{
    local list sha path name url
    locate_worktree_git_dir
    [ ! -e "$WGD/modules" ] || die "$WGD/modules exists: a partial setup. Remove that directory, then run add-submodules"
    list=$(gitlinks "$WT")
    [ -n "$list" ] || die "no submodules recorded in $WT"
    refresh_main_stores "$list"
    # Only the submodules of this worktree: git data of other submodules would keep a core.worktree that points at
    # the main checkout.
    while IFS="$TAB" read -r sha path name url
    do
        mkdir -p "$(dirname "$WGD/modules/$name")"
        copy_store "$GD/modules/$name" "$WGD/modules/$name"
        set_module_worktree "$WGD/modules/$name" "$WT/$path"
    done <<< "$list"
    note "submodule git data: copied from the main checkout"
    git -C "$WT" submodule --quiet init
    materialize "$list"
    note "submodule files: done"
    verify "$list"
}

cmd_create()
{
    local submodules=1 path branch start
    if [ "${1:-}" = --no-submodules ]; then submodules=0; shift; fi
    [ $# -ge 3 ] && [ $# -le 4 ] || usage
    locate_main "$1"
    path=$2 branch=$3 start=${4:-}
    [ ! -e "$path" ] || die "$path exists"
    mkdir -p "$(dirname "$path")"
    WT=$(cd "$(dirname "$path")" && pwd -P)/$(basename "$path")
    refresh_main_worktree
    if git -C "$MAIN" show-ref -q --verify "refs/heads/$branch"
    then
        [ -z "$start" ] || die "branch $branch exists; do not give a start point"
        git -C "$MAIN" worktree add -q --no-checkout "$WT" "$branch"
    else
        [ -n "$start" ] || die "branch $branch does not exist; give a start point"
        git -C "$MAIN" worktree add -q --no-checkout -b "$branch" "$WT" "$start"
    fi
    git -C "$WT" -c checkout.workers=0 -c core.fsync=none -c gc.auto=0 checkout -q -f HEAD -- .
    note "worktree: $WT on $branch at $(git -C "$WT" rev-parse --short HEAD)"
    if [ "$submodules" = 0 ]
    then
        note "submodules: skipped; run \`$0 add-submodules $WT\` before a build"
        return 0
    fi
    setup_submodules
}

cmd_add_submodules()
{
    [ $# -eq 1 ] || usage
    locate_main "$1"
    WT=$(cd "$1" && pwd -P)
    [ "$(git -C "$WT" rev-parse --show-toplevel)" = "$WT" ] || die "give the top directory of the worktree"
    refresh_main_worktree
    setup_submodules
}

cmd_update_submodules()
{
    local list sha path name url store
    [ $# -eq 1 ] || usage
    locate_main "$1"
    WT=$(cd "$1" && pwd -P)
    [ "$(git -C "$WT" rev-parse --show-toplevel)" = "$WT" ] || die "give the top directory of the worktree"
    locate_worktree_git_dir
    [ -d "$WGD/modules" ] || die "$WT has no submodules yet; run add-submodules"
    refresh_main_worktree
    list=$(gitlinks "$WT")
    [ -n "$list" ] || die "no submodules recorded in $WT"
    refresh_main_stores "$list"
    git -C "$WT" submodule --quiet init
    while IFS="$TAB" read -r sha path name url
    do
        store=$WGD/modules/$name
        if [ ! -d "$store" ]
        then
            note "new submodule $path: copying its git data from the main checkout"
            mkdir -p "$(dirname "$store")"
            copy_store "$GD/modules/$name" "$store"
        fi
        # The git data under this worktree belongs to it. This also repairs git data that points at another checkout.
        set_module_worktree "$store" "$WT/$path"
        if ! git_store "$store" cat-file -e "$sha^{commit}" 2> /dev/null
        then
            git_store "$store" fetch -q "$GD/modules/$name" "$sha:refs/pins/$sha"
        fi
    done <<< "$list"
    materialize "$list"
    note "submodule files: done"
    verify "$list"
}

case ${1:-} in
    create) shift; cmd_create "$@" ;;
    add-submodules) shift; cmd_add_submodules "$@" ;;
    update-submodules) shift; cmd_update_submodules "$@" ;;
    *) usage ;;
esac
