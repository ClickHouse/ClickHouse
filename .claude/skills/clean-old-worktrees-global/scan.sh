#!/bin/bash
# Scan git checkouts and build directories under a root directory and classify them by recent activity.
# Usage: scan.sh <out_dir> [root=$HOME] [age='1 day ago'] [parallelism=16]
# Outputs in <out_dir>, with fields separated by tabs (directory names containing a tab or a newline are not supported
# and are listed in skipped.txt instead):
#   checkouts.txt     "<wt|main>\t<RECENT|old|UNKNOWN>\t<dir>" for every direct child of root that is a git checkout.
#                     UNKNOWN means the age could not be established (e.g. unreadable subdirectories): keep it.
#   wt_old_status.txt "<dir>\tdirty=N\tunpushed=N\tsubmodule_commits=N\tbranch=B" for every old worktree.
#                     Any count that could not be established is ERROR.
#   build_old.txt     "<dir>" for old build* directories inside checkouts that are kept (recent ones and main repos)
#   skipped.txt       "<reason>\t<shell-quoted dir>" for directories that were not classified

set -u
out=$(realpath "$1")
root=$(realpath "${2:-$HOME}")
age=${3:-1 day ago}
par=${4:-16}
mkdir -p "$out"
cd "$root" || exit 1
export AGE="$age"
export SKIPPED="$out/skipped.txt"
: > "$SKIPPED"

# Prints RECENT if find (with the given arguments) finds anything newer than AGE, old if the whole traversal succeeded
# without finding anything, and UNKNOWN if the traversal failed, because then the absence of recent files is not proven.
age_of()
{
    local r s
    r=$(find "$@" -newermt "$AGE" -print -quit 2>/dev/null)
    s=$?
    if [ -n "$r" ]; then echo RECENT; elif [ "$s" = 0 ]; then echo old; else echo UNKNOWN; fi
}

# Returns false (and records the reason) for names that cannot be represented in the line- and tab-based lists.
supported_name()
{
    case $1 in
        *$'\t'*|*$'\n'*) printf 'unsupported name\t%q\n' "$1" >> "$SKIPPED"; return 1 ;;
    esac
}
export -f age_of supported_name

# 1. Classify checkouts. Build dirs, contrib and .git are pruned: they are huge and do not reflect use of the checkout.
#    Modifications inside contrib submodules are detected by the status check in step 2.
# shellcheck disable=SC2016 # The inner scripts are expanded by the inner shell.
find . -mindepth 1 -maxdepth 1 -type d -printf '%P\0' | xargs -r -0 -P "$par" -n1 bash -c '
    d=$1
    supported_name "$d" || exit 0
    if [ -f "$d/.git" ]; then t=wt; elif [ -d "$d/.git" ]; then t=main; else exit 0; fi
    a=$(age_of "$d" -xdev \( -path "$d/build*" -o -path "$d/contrib" -o -path "$d/.git" \) -prune -o)
    printf "%s\t%s\t%s\n" "$t" "$a" "$d"
' _ | sort -t $'\t' -k3 > "$out/checkouts.txt"

# 2. Safety status of old worktrees. Submodules are included, although walking every contrib submodule is slow:
#    their working trees and git directories (inside the worktree's metadata in the main repo) are deleted with the
#    worktree, and nothing else preserves them.
#    submodule_commits counts submodule pointer changes in unpushed commits: they may refer to commits that exist only
#    in the worktree's submodules.
# shellcheck disable=SC2016
awk -F '\t' '$1=="wt" && $2=="old" {print $3}' "$out/checkouts.txt" | xargs -r -d '\n' -P "$par" -n1 bash -c '
    d=$1
    if s=$(git -C "$d" status --porcelain --ignore-submodules=none 2>&1); then
        dirty=$(grep -v -e "^?? build" -e "^?? tmp/" <<< "$s" | grep -c .)
    else
        dirty=ERROR
    fi
    if s=$(git -C "$d" rev-list HEAD --not --remotes 2>/dev/null); then unpushed=$(grep -c . <<< "$s"); else unpushed=ERROR; fi
    if s=$(git -C "$d" log HEAD --not --remotes --format= --raw --no-abbrev 2>/dev/null); then
        subcommits=$(awk "/^:/ && (\$1 == \":160000\" || \$2 == \"160000\")" <<< "$s" | grep -c .)
    else
        subcommits=ERROR
    fi
    br=$(git -C "$d" rev-parse --abbrev-ref HEAD 2>/dev/null)
    printf "%s\tdirty=%s\tunpushed=%s\tsubmodule_commits=%s\tbranch=%s\n" "$d" "$dirty" "$unpushed" "$subcommits" "$br"
' _ | sort > "$out/wt_old_status.txt"

# 3. Old build directories inside checkouts that will be kept (recent checkouts and all main repos).
#    Directories whose age cannot be established are not listed.
# shellcheck disable=SC2016
awk -F '\t' '$1=="main" || $2!="old" {print $3}' "$out/checkouts.txt" \
    | while read -r d; do find "$d" -mindepth 1 -maxdepth 1 -type d -name 'build*' -print0; done \
    | xargs -r -0 -P "$par" -n1 bash -c '
        supported_name "$1" || exit 0
        a=$(age_of "$1" -xdev)
        if [ "$a" = old ]; then echo "$1"; elif [ "$a" = UNKNOWN ]; then printf "age unknown\t%q\n" "$1" >> "$SKIPPED"; fi
    ' _ \
    | sort > "$out/build_old.txt"

echo "checkouts: $(wc -l < "$out/checkouts.txt"), recent: $(grep -c $'\tRECENT\t' "$out/checkouts.txt"), unknown age: $(grep -c $'\tUNKNOWN\t' "$out/checkouts.txt")"
echo "old worktrees: $(wc -l < "$out/wt_old_status.txt"), of them not clean+pushed: $(grep -vc $'\tdirty=0\tunpushed=0\tsubmodule_commits=0\t' "$out/wt_old_status.txt")"
echo "old build dirs in kept checkouts: $(wc -l < "$out/build_old.txt")"
echo "skipped: $(wc -l < "$SKIPPED")"
