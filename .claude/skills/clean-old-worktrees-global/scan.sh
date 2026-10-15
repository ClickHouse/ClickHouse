#!/bin/bash
# Scan git checkouts and build directories under a root directory and classify them by recent activity.
# Usage: scan.sh <out_dir> [root=$HOME] [age='1 day ago'] [parallelism=16]
# Outputs in <out_dir>:
#   checkouts.txt     "<wt|main> <dir> <RECENT|old>" for every direct child of root that is a git checkout
#   wt_old_status.txt "<dir> dirty=N unpushed=N branch=B" for every old worktree
#   build_old.txt     old build* directories inside checkouts that are kept (recent ones and main repos)

set -u
out=$(realpath "$1")
root=$(realpath "${2:-$HOME}")
age=${3:-1 day ago}
par=${4:-16}
mkdir -p "$out"
cd "$root" || exit 1
export AGE="$age"

# 1. Classify checkouts. Build dirs, contrib and .git are pruned: they are huge and do not reflect use of the checkout.
# shellcheck disable=SC2016 # The inner scripts are expanded by the inner shell.
find . -mindepth 1 -maxdepth 1 -type d -printf '%P\n' | xargs -r -d '\n' -P "$par" -n1 bash -c '
    d=$1
    if [ -f "$d/.git" ]; then t=wt; elif [ -d "$d/.git" ]; then t=main; else exit 0; fi
    r=$(find "$d" -xdev \( -path "$d/build*" -o -path "$d/contrib" -o -path "$d/.git" \) -prune -o -newermt "$AGE" -print -quit 2>/dev/null)
    if [ -n "$r" ]; then echo "$t $d RECENT"; else echo "$t $d old"; fi
' _ | sort -k2 > "$out/checkouts.txt"

# 2. Safety status of old worktrees. --ignore-submodules is essential: otherwise git walks every contrib submodule and takes forever.
# shellcheck disable=SC2016
awk '$1=="wt" && $3=="old" {print $2}' "$out/checkouts.txt" | xargs -r -d '\n' -P "$par" -n1 bash -c '
    d=$1
    dirty=$(git -C "$d" status --porcelain --ignore-submodules=all 2>&1 | grep -v -e "^?? build" -e "^?? tmp/" | wc -l)
    unpushed=$(git -C "$d" rev-list HEAD --not --remotes 2>/dev/null | wc -l)
    br=$(git -C "$d" rev-parse --abbrev-ref HEAD 2>/dev/null)
    echo "$d dirty=$dirty unpushed=$unpushed branch=$br"
' _ | sort > "$out/wt_old_status.txt"

# 3. Old build directories inside checkouts that will be kept (recent checkouts and all main repos).
# shellcheck disable=SC2016
awk '$1=="main" || $3=="RECENT" {print $2}' "$out/checkouts.txt" \
    | while read -r d; do find "$d" -mindepth 1 -maxdepth 1 -type d -name 'build*'; done \
    | xargs -r -d '\n' -P "$par" -n1 bash -c 'r=$(find "$1" -xdev -newermt "$AGE" -print -quit 2>/dev/null); [ -z "$r" ] && echo "$1"' _ \
    | sort > "$out/build_old.txt"

echo "checkouts: $(wc -l < "$out/checkouts.txt"), recent: $(grep -c ' RECENT$' "$out/checkouts.txt")"
echo "old worktrees: $(wc -l < "$out/wt_old_status.txt"), of them not clean+pushed: $(grep -vc 'dirty=0 unpushed=0' "$out/wt_old_status.txt")"
echo "old build dirs in kept checkouts: $(wc -l < "$out/build_old.txt")"
