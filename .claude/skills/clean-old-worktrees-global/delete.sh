#!/bin/bash
# Delete the directories listed in a file (paths relative to root), then prune worktree metadata in the affected main repos.
# Usage: delete.sh <list_file> [root=$HOME]
# Deletion uses `sudo rm -rf` when passwordless sudo is available, because integration tests run in Docker leave root-owned files.

set -u
list=$(realpath "$1")
root=$(realpath "${2:-$HOME}")
cd "$root" || exit 1

# Collect main repos of worktrees before their `.git` files disappear.
mains=$(while read -r d; do [ -f "$d/.git" ] && sed -n 's#^gitdir: \(.*\)/\.git/worktrees/.*#\1#p' "$d/.git"; done < "$list" | sort -u)

rm_cmd=(rm -rf)
sudo -n true 2>/dev/null && rm_cmd=(sudo rm -rf)

# Refuse anything outside root or empty lines.
grep -v -e '^$' -e '^/' -e '\.\.' "$list" | sed "s#^#$root/#" | xargs -r -d '\n' -P 8 -n1 "${rm_cmd[@]}"

for m in $mains; do git -C "$m" worktree prune; done

left=0
while read -r d; do [ -n "$d" ] && [ -e "$d" ] && { echo "LEFT: $d"; left=$((left+1)); }; done < "$list"
echo "remaining: $left"
for m in $mains; do echo "$m: $(git -C "$m" worktree list | grep -c prunable) prunable"; done
df -h "$root" | tail -1
