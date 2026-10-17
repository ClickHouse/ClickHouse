#!/bin/bash
# Delete the directories listed in a file (paths relative to root, one per line), then prune worktree metadata in the
# affected main repos.
# Usage: delete.sh <list_file> [root=$HOME]
# Entries must be a direct child of root (a worktree) or a build* directory inside one. Right before deletion, every entry
# is checked again and skipped if it:
#   - is a main repository (`.git` is a directory) or a symlink;
#   - contains a mount point (e.g. `/dev` or `/proc` bind-mounted into a chroot);
#   - contains a block device file;
#   - is in use by a process: its working directory, root, executable, open file or mapped file is inside.
# Deletion uses `sudo rm -rf` when passwordless sudo is available, because integration tests run in Docker leave
# root-owned files. Processes of other users can only be inspected with sudo; without it the script refuses to run
# unless ALLOW_UNINSPECTED=1 is set.

set -u
list=$(realpath "$1")
root=$(realpath "${2:-$HOME}")
cd "$root" || exit 1

sudo_cmd=()
if sudo -n true 2>/dev/null; then
    sudo_cmd=(sudo -n)
elif [ "${ALLOW_UNINSPECTED:-0}" != 1 ]; then
    echo "Passwordless sudo is not available: processes of other users (e.g. in Docker containers) cannot be inspected." >&2
    echo "Check them manually and rerun with ALLOW_UNINSPECTED=1." >&2
    exit 1
fi

state=$(mktemp -d)
trap 'rm -rf "$state"' EXIT

# Mount points, with the octal escapes of /proc/self/mountinfo decoded.
awk '{print $5}' /proc/self/mountinfo | while read -r m; do printf '%b\n' "$m"; done > "$state/mounts"

# Paths in use by processes. Mapped file names are the sixth and following fields of /proc/<pid>/maps.
# shellcheck disable=SC2016
"${sudo_cmd[@]}" bash -c '
    for p in /proc/[0-9]*; do
        for l in cwd root exe; do readlink "$p/$l"; done
        for f in "$p"/fd/*; do readlink "$f"; done
        sed -n -E "s#^([^ ]+ +){5}(/.*)#\2#p" "$p/maps"
    done 2>/dev/null
' | sort -u > "$state/active"

# Collect main repos of worktrees before their `.git` files disappear.
mains=$(while read -r d; do [ -n "$d" ] && [ -f "$d/.git" ] && sed -n 's#^gitdir: \(.*\)/\.git/worktrees/.*#\1#p' "$d/.git"; done < "$list" | sort -u)

export ROOT="$root" STATE="$state"
export SUDO="${sudo_cmd[*]}"

# shellcheck disable=SC2016
tr '\n' '\0' < "$list" | xargs -r -0 -P 8 -n1 bash -c '
    d=$1
    skip() { printf "SKIPPED (%s): %s\n" "$1" "$d"; exit 0; }
    [ -n "$d" ] || exit 0
    case $d in
        /*|*//*|*/) skip "not a relative path" ;;
        ..|../*|*/..|*/../*|.|./*|*/.|*/./*) skip "dot components" ;;
        */*/*) skip "too deep" ;;
        */*) case ${d#*/} in build*) ;; *) skip "not a build directory" ;; esac ;;
    esac
    p=$ROOT/$d
    [ -e "$p" ] || [ -L "$p" ] || exit 0
    [ -L "$p" ] && skip "symlink"
    [ -d "$p" ] || skip "not a directory"
    [ -d "$p/.git" ] && skip "main repository"
    is_under() { P=$p awk "{ if (\$0 == ENVIRON[\"P\"] || index(\$0, ENVIRON[\"P\"] \"/\") == 1) { found = 1; exit } } END { exit !found }" "$1"; }
    is_under "$STATE/mounts" && skip "contains a mount point"
    is_under "$STATE/active" && skip "in use by a process"
    if ! b=$($SUDO find "$p" -xdev -type b -print -quit 2>/dev/null); then skip "cannot check for block devices"; fi
    [ -n "$b" ] && skip "contains block device $b"
    $SUDO rm -rf --one-file-system "$p"
' _

for m in $mains; do git -C "$m" worktree prune; done

left=0
while read -r d; do [ -n "$d" ] && [ -e "$d" ] && { echo "LEFT: $d"; left=$((left+1)); }; done < "$list"
echo "remaining: $left"
for m in $mains; do echo "$m: $(git -C "$m" worktree list | grep -c prunable) prunable"; done
df -h "$root" | tail -1
