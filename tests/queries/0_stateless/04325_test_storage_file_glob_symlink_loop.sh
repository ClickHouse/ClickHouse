#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

USER_FILES_PATH=$($CLICKHOUSE_CLIENT_BINARY --query "select _path,_file from file('nonexist.txt', 'CSV', 'val1 char')" 2>&1 \
    | grep Exception | awk '{gsub("/nonexist.txt","",$9); print $9}')

TEST_DIR_NAME="$CLICKHOUSE_TEST_UNIQUE_NAME"
TEST_DIR_ABS="$USER_FILES_PATH/$TEST_DIR_NAME"

mkdir -p "$TEST_DIR_ABS/loop/dir1/dir2"
printf "row1\nrow2\nrow3\n" > "$TEST_DIR_ABS/loop/dir1/dir2/file.txt"

# Brace-expansion subtree: a single subdirectory holds both a `.txt` and a `.csv` file.
# A correct cycle guard must not mark `subdir` as "visited" globally during the
# `.txt` walk, otherwise the `.csv` walk will silently skip it.
mkdir -p "$TEST_DIR_ABS/brace/d/subdir"
printf "row1\n" > "$TEST_DIR_ABS/brace/d/subdir/a.txt"
printf "row2\n" > "$TEST_DIR_ABS/brace/d/subdir/b.csv"

# Two independent symlinks pointing at the same target. Walking through each
# independently (a single recursion path through `parentA/link`, then a separate
# recursion path through `parentB/link`) is not a cycle, and a correct guard
# must let both walks reach the target.
mkdir -p "$TEST_DIR_ABS/aliases/target/sub" "$TEST_DIR_ABS/aliases/parentA" "$TEST_DIR_ABS/aliases/parentB"
printf "row1\n" > "$TEST_DIR_ABS/aliases/target/sub/file.txt"
ln -s ../target "$TEST_DIR_ABS/aliases/parentA/link"
ln -s ../target "$TEST_DIR_ABS/aliases/parentB/link"

# Descendant-to-root symlink with a sibling directory: `descroot/a/back` -> `..`,
# so canonical(`descroot/a/back`) == canonical(`descroot`). Without an entry-time
# guard on the initial glob root, the recursive walk through `descroot/a/back`
# would re-enter `descroot/b` and read `file.txt`, and the top-level walk would
# also enter `descroot/b` directly and read the same file again, returning two
# rows instead of one.
mkdir -p "$TEST_DIR_ABS/descroot/a" "$TEST_DIR_ABS/descroot/b"
printf "row1\n" > "$TEST_DIR_ABS/descroot/b/file.txt"
ln -s .. "$TEST_DIR_ABS/descroot/a/back"

# Finite-glob through ancestor symlink: `finite/a/back` -> `..`, sibling `finite/file.txt`.
# A finite (non-`**`) glob like `*/*/*.txt` legitimately reaches `file.txt` via
# `finite/a/back/file.txt`. The cycle guard must not activate for finite globs;
# otherwise the inner walk's canonical(`finite`) collides with the outer walk's
# canonical(`finite`) and the file is silently dropped.
mkdir -p "$TEST_DIR_ABS/finite/a"
printf "row1\n" > "$TEST_DIR_ABS/finite/file.txt"
ln -s .. "$TEST_DIR_ABS/finite/a/back"

# Mutual symlink cycle: `mutual/sub/a -> b`, `mutual/sub/b -> a`, plus a real file
# `mutual/sub/real.txt`. Resolving either symlink target hits `ELOOP` immediately
# (the kernel detects the cycle while resolving the entry itself). The throwing
# `directory_entry::is_directory` overload would surface this as a raw
# `filesystem_error` and abort the entire glob expansion before the canonical-stack
# guard could prune the entry. Using the `std::error_code` overload lets us skip
# the unresolvable entries silently and still return the real file.
mkdir -p "$TEST_DIR_ABS/mutual/sub"
printf "row1\n" > "$TEST_DIR_ABS/mutual/sub/real.txt"
ln -s b "$TEST_DIR_ABS/mutual/sub/a"
ln -s a "$TEST_DIR_ABS/mutual/sub/b"

# Pre-asterisk ancestor: `preast/root/a/back -> ..`, `preast/root/b/sub/file.txt`.
# Pattern `preast/root/*/**/*.txt` traverses the finite `*` over `a` and `b` BEFORE
# reaching the `**` segment. Without seeding bounded ancestors when the expanded
# pattern contains `**` anywhere, the inner `**` walk under `root/a` only seeds
# canonical(`root/a`) and accepts `back -> ..` as a fresh directory; the resulting
# descent re-reads `b/sub/file.txt` while the top-level `*` walk also reads it
# through `b`, producing a duplicate row. The fix seeds ALL bounded ancestors
# (including the initial glob root) for any expansion containing `**`.
mkdir -p "$TEST_DIR_ABS/preast/root/a" "$TEST_DIR_ABS/preast/root/b/sub"
printf "row1\n" > "$TEST_DIR_ABS/preast/root/b/sub/file.txt"
ln -s .. "$TEST_DIR_ABS/preast/root/a/back"

# Finite glob with adjacent asterisks: `adj/root/file.txt`, `adj/root/a/back -> ..`.
# Pattern `adj/root/a**/back/*.txt` has NO recursive `**` path segment: `a**` is a
# finite component (it matches the literal directory `a` here, since `*` and `*`
# both expand to empty). The cycle guard must NOT activate for this expansion,
# otherwise the walk through `a/back` resolves canonical to the seeded `adj/root`
# and the legitimate match through `back/file.txt` is silently dropped. A naive
# substring `find("**") != npos` over the expanded pattern would falsely classify
# this as recursive; the correct detector mirrors the per-segment recursion test
# in `listFilesWithRegexpMatchingImpl` (a path component must equal exactly `**`).
mkdir -p "$TEST_DIR_ABS/adj/root/a"
printf "row1\n" > "$TEST_DIR_ABS/adj/root/file.txt"
ln -s .. "$TEST_DIR_ABS/adj/root/a/back"

trap 'rm -rf "$TEST_DIR_ABS"' EXIT

# Ancestor-loop symlink: `loop/dir1/dir2/loop_to_root` points back at `loop/dir1`,
# so following `dir2/loop_to_root` recreates the same `dir1/dir2/loop_to_root`
# infinitely. The kernel would surface this as `Too many levels of symbolic
# links` after roughly 40 levels.
ln -s ../../dir1 "$TEST_DIR_ABS/loop/dir1/dir2/loop_to_root"

# Real cycle: recursive `**` glob would otherwise descend the loop until ELOOP.
# With recursion-stack tracking the loop is broken and the real `file.txt` is
# read.
echo "ancestor-loop"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/loop/dir1/**/*.txt', 'TSV', 'val String')"

# Brace expansion: `{txt,csv}` expands into two separate walks. Both walks must
# enter `subdir` and report their respective files. With a global visited-path
# set the second walk would silently skip `subdir`; with recursion-stack
# tracking both files are returned.
echo "brace-expansion"
$CLICKHOUSE_CLIENT --query "SELECT _file FROM file('$TEST_DIR_NAME/brace/d/**/*.{txt,csv}', 'TSV', 'val String') ORDER BY _file"

# Independent symlinks pointing at the same target through brace alternatives.
# Each branch is its own descent and must succeed independently.
echo "independent-aliases"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/aliases/{parentA,parentB}/**/*.txt', 'TSV', 'val String')"

# Descendant-to-root: must return exactly 1 row (not 2). The recursion-stack
# guard inserts the initial glob root on entry, so a descendant `back` symlink
# whose canonical is the root is rejected as a cycle and the sibling `b/file.txt`
# is read only once via the top-level walk.
echo "descendant-to-root"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/descroot/**/*.txt', 'TSV', 'val String')"

# Finite glob through ancestor symlink: must return exactly 1 row. The finite
# pattern `*/*/*.txt` reaches `finite/file.txt` via `finite/a/back/file.txt`.
# The cycle guard is not active for finite globs (no `**`) so the inner walk
# is allowed to reach the file via the symlink path.
echo "finite-glob-through-ancestor-link"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/finite/*/*/*.txt', 'TSV', 'val String')"

# Mutual symlink cycle: must return 1 (the real file). Without the `std::error_code`
# overload of `is_directory` the throwing overload aborts the directory walk with
# `Too many levels of symbolic links` and the query fails. With the fix the two
# unresolvable entries are skipped and the real file is returned.
echo "mutual-symlink-cycle"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/mutual/**/*.txt', 'TSV', 'val String')"

# Pre-asterisk ancestor seed: must return 1 (no duplicate row). The pattern uses
# a finite `*` before `**`, and a deeper symlink resolves to the initial glob
# root. Without seeding bounded ancestors when the expansion contains `**` the
# guard only protects the `**` recursion and the same file is read twice.
echo "pre-asterisk-ancestor-seed"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/preast/root/*/**/*.txt', 'TSV', 'val String')"

# Finite glob with adjacent asterisks: must return 1 (the file reached via the
# legitimate `a/back` symlink path). The pattern has no `**` path segment, so the
# cycle guard must stay inactive. A naive substring detector would falsely treat
# `a**` as recursive, activate the guard, and drop the match.
echo "finite-glob-with-adjacent-asterisks"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/adj/root/a**/back/*.txt', 'TSV', 'val String')"

# Server alive afterwards.
$CLICKHOUSE_CLIENT --query "SELECT 'alive'"
