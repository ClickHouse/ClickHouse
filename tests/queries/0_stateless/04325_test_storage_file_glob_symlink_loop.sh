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

# Finite glob with adjacent asterisks: `adj/root/file.txt` reachable through two
# directories, `adj/root/a1/back -> ..` and `adj/root/a2/back -> ..`. Pattern
# `adj/root/a**/back/*.txt` has NO recursive `**` path segment: `a**` is a finite
# component (it matches `a1` and `a2` here, since one `*` can expand to empty). The
# cycle guard must NOT activate for this expansion, otherwise the walk through the first
# `back` seeds `adj/root` and the second match is silently dropped. A naive substring
# `find("**") != npos` over the expanded pattern would falsely classify this as
# recursive; the correct detector mirrors the per-segment recursion test in
# `listFilesWithRegexpMatchingImpl` (a path component must equal exactly `**`).
# TWO such directories are what makes the assertion observable. Both paths are returned,
# `a1/back/file.txt` and `a2/back/file.txt`, so the expected count is two and the expected
# `via` values are `a1` and `a2`, exactly as without this change. One directory would give
# one path under every key and show nothing. Substring detection of `**` and unconditional
# canonical deduplication would each return one path instead.
mkdir -p "$TEST_DIR_ABS/adj/root/a1" "$TEST_DIR_ABS/adj/root/a2"
printf "row1\n" > "$TEST_DIR_ABS/adj/root/file.txt"
ln -s .. "$TEST_DIR_ABS/adj/root/a1/back"
ln -s .. "$TEST_DIR_ABS/adj/root/a2/back"

# Bounded finite tail after the last `**`: `overprune/root/deep/mid/back -> ../..`
# (canonical == `overprune/root`), real file `overprune/root/f.txt`. Pattern
# `overprune/root/**/mid/*/*.txt` has the `**` match `deep`, then a BOUNDED tail
# `mid/*/*.txt` with no whole-segment `**`, so no unbounded recursion is possible; the
# inner `*` legitimately matches the symlink `back` whose canonical equals the
# `**`-frame ancestor `overprune/root`, reaching `back/f.txt`. The guard must be keyed
# off the REMAINING pattern (still has a `**`?) rather than the whole expanded pattern,
# otherwise it activates in the bounded tail and drops this valid finite-tail match.
mkdir -p "$TEST_DIR_ABS/overprune/root/deep/mid"
printf "row1\n" > "$TEST_DIR_ABS/overprune/root/f.txt"
ln -s ../.. "$TEST_DIR_ABS/overprune/root/deep/mid/back"

# Adjacent globstars over a symlink loop: `globstarloop/root/top.txt`,
# `globstarloop/root/a/back -> ..` (canonical(`root/a/back`) == canonical(`root`)).
# Pattern `root/**/**/*.txt` reaches `top.txt` ONLY through the zero-level
# re-application chain: both `**` segments match zero directory levels and the
# trailing `*.txt` is re-applied at `root` itself. Each re-application re-enters
# `root` with a SHORTER remaining pattern while the caller frame at `root` is still
# in progress, so the visited-frame set must distinguish those frames or
# `top.txt` is dropped (0 rows instead of 1). The `back -> ..` symlink still forms
# a genuine loop during the recursive descent, which the guard breaks, so the walk
# terminates with no `Too many levels of symbolic links` exception. 04489 covers
# adjacent globstars without symlinks and the scenarios above cover symlink loops
# without the `**/**` interaction, so neither covers this combination.
mkdir -p "$TEST_DIR_ABS/globstarloop/root/a"
printf "row1\n" > "$TEST_DIR_ABS/globstarloop/root/top.txt"
ln -s .. "$TEST_DIR_ABS/globstarloop/root/a/back"

# Re-entry with a DIFFERENT remaining pattern, reached through a glob rather than a
# zero-level `**`: `reenter/root/top.txt`, `reenter/root/x/back -> ..`. For
# `root/*/**/top.txt` the finite `*` matches `x`, the walk follows `back` into
# canonical(`root`), and there the `**` matches zero levels so `top.txt` is
# re-applied. That inner frame is (`root`, `/top.txt`) while the outer frame still
# in progress is (`root`, `/*/**/top.txt`): same directory, different remaining
# pattern, therefore new work and not a cycle. Keying the visited-frame set on the
# canonical directory alone conflates the two and returns 0 rows.
mkdir -p "$TEST_DIR_ABS/reenter/root/x"
printf "row1\n" > "$TEST_DIR_ABS/reenter/root/top.txt"
ln -s .. "$TEST_DIR_ABS/reenter/root/x/back"

# The same file named twice, inside a bounded finite tail: real file
# `dedup/root/f.txt` with two symlinks `dedup/root/deep/mid/alias{A,B} -> ../..` that
# both resolve to `dedup/root`. For `root/**/mid/*/f.txt` the `**` matches `deep`, and
# the remaining `mid/*/f.txt` has no whole-segment `**`, so frame tracking is inactive
# there by design and the inner `*` legitimately matches both aliases. The file is
# therefore named as `mid/aliasA/f.txt` and `mid/aliasB/f.txt`; those two spellings
# resolve to one file but their lexically-normal forms differ, so a lexical
# deduplication key reports 2 rows. Deduplicating on the canonical path is what
# collapses them. The bounded tail matters: with a pattern that still has a `**` left,
# frame tracking would prune the second alias before it ever reached the output, so
# such a case cannot test the deduplication key at all.
mkdir -p "$TEST_DIR_ABS/dedup/root/deep/mid"
printf "row1\n" > "$TEST_DIR_ABS/dedup/root/f.txt"
ln -s ../.. "$TEST_DIR_ABS/dedup/root/deep/mid/aliasA"
ln -s ../.. "$TEST_DIR_ABS/dedup/root/deep/mid/aliasB"

# A symlink to a FILE, alongside its target. Both `alias.txt` and `real.txt` are names
# the pattern selected and both must be reported, so the deduplication key resolves only
# the PARENT directory and leaves the final component as written. Resolving the whole
# path collapses the two into one row and drops a `_file` value the user asked for. This
# has to hold for a `**` expansion too, where the key is canonical: the zero-level branch
# reaches both entries at `root` itself, so a fully-canonical key would map them both to
# `real.txt`. A plain `*` and an implicit directory listing must report two rows as well.
mkdir -p "$TEST_DIR_ABS/filelink/root"
printf "row1\n" > "$TEST_DIR_ABS/filelink/root/real.txt"
ln -s real.txt "$TEST_DIR_ABS/filelink/root/alias.txt"

# Several sibling ancestor loops under one root: `branching/root/b1..b3/up -> ../..`.
# This is the scenario the traversal pruning itself is for, as distinct from output
# deduplication. Each `up` re-enters the root, where the walk can descend into all
# three branches again, so without pruning the number of visited paths grows like
# 3^depth and the walk only stops at the kernel symlink limit around 40 levels. That
# is far too much work to finish, while deduplication cannot help because it filters
# results and not traversal. With pruning each (directory, remaining pattern) frame is
# entered once, so this completes immediately and reports the four real files
# (`top.txt` plus one per branch). A single-branch loop is not enough here: it stays
# cheap even unpruned, which is why the scenarios above cannot detect a missing guard.
mkdir -p "$TEST_DIR_ABS/branching/root"
printf "row1\n" > "$TEST_DIR_ABS/branching/root/top.txt"
for b in b1 b2 b3; do
    mkdir -p "$TEST_DIR_ABS/branching/root/$b"
    printf "row1\n" > "$TEST_DIR_ABS/branching/root/$b/f.txt"
    ln -s ../.. "$TEST_DIR_ABS/branching/root/$b/up"
done

# A dense alias graph with no cycle through an ancestor: ten sibling directories, each
# holding a symlink to every other one, so every directory is reachable under many
# different names. Nothing here loops back to the root, so a guard that only looks at
# the frames currently being descended prunes none of it, and the walk enumerates paths
# combinatorially in the number of aliases. Recording every frame the walk has entered,
# rather than only the ones still in progress, bounds this to one visit per directory
# per remaining pattern, which is safe because a repeated frame enumerates exactly the
# paths the first one already did. Only the ten real files are reported.
mkdir -p "$TEST_DIR_ABS/aliasgraph/root"
for i in $(seq 1 10); do
    mkdir -p "$TEST_DIR_ABS/aliasgraph/root/d$i"
    printf "row1\n" > "$TEST_DIR_ABS/aliasgraph/root/d$i/f.txt"
done
for i in $(seq 1 10); do
    for j in $(seq 1 10); do
        [ "$i" != "$j" ] && ln -s "../d$j" "$TEST_DIR_ABS/aliasgraph/root/d$i/to$j"
    done
done

# A chain of ten nested directories where every level also carries five symlinks to its
# own real child, so the number of LEXICAL paths from the root down to the leaf is 6^10
# while only ten distinct directories exist. The query below spends nine finite `*`
# components walking that chain before it reaches the `**`, and none of those levels
# loops back to an ancestor. A guard that only tracked the `**` segment itself would
# leave every one of those finite frames unrecorded and re-walk the whole subtree once
# per lexical spelling; the file is still found exactly once, so no count can see the
# difference. Recording every frame whose remaining pattern still holds a `**` prunes
# the chain to one visit per directory. Measured on this shape: tracking the finite
# prefix answers in 0.2s, tracking only the `**` segment does not finish within 300s.
mkdir -p "$TEST_DIR_ABS/prefixchain/root"
prefix_chain_dir="$TEST_DIR_ABS/prefixchain/root"
for i in $(seq 1 10); do
    mkdir -p "$prefix_chain_dir/l$i"
    for a in 1 2 3 4 5; do ln -s "l$i" "$prefix_chain_dir/al$a"; done
    prefix_chain_dir="$prefix_chain_dir/l$i"
done
printf "row1\n" > "$prefix_chain_dir/f.txt"

# One real directory reached under two symlink names: `aliaswrite/root/target` plus
# `aliasA` and `aliasB` both pointing at it. For `root/**/f.txt` the walk reaches the
# same directory three times, so the pattern names one file under three paths and the
# write must stay refused. The traversal pruning returns before any match is emitted, so
# the collapse has to be reported where the frame is pruned and not only where a matched
# path is dropped; without that, this write is allowed and appends to the target.
mkdir -p "$TEST_DIR_ABS/aliaswrite/root/target"
printf "row1\n" > "$TEST_DIR_ABS/aliaswrite/root/target/f.txt"
ln -s target "$TEST_DIR_ABS/aliaswrite/root/aliasA"
ln -s target "$TEST_DIR_ABS/aliaswrite/root/aliasB"

# A finite glob matching exactly one file, with no symlink anywhere: the write must be
# ALLOWED, as it is without this change. This is the case a read-only guard keyed on glob
# syntax rather than on an actual collapse would wrongly refuse.
mkdir -p "$TEST_DIR_ABS/onematch"
printf "row1\n" > "$TEST_DIR_ABS/onematch/only.txt"

# An aliased directory holding no matching file: `emptyalias/root/only.tsv` is the only
# match, beside an empty directory reachable as itself and as `alias`. The walk prunes the
# aliased frame, but that frame would have matched nothing, so nothing was collapsed and
# the write must stay ALLOWED. A guard that treated any pruned alias as a collapse would
# refuse it, which no other scenario here would notice.
mkdir -p "$TEST_DIR_ABS/emptyalias/root/empty"
printf "row1\n" > "$TEST_DIR_ABS/emptyalias/root/only.tsv"
ln -s empty "$TEST_DIR_ABS/emptyalias/root/alias"

# The same two aliases, but the matching file sits one level BELOW the aliased directory:
# `nestedalias/root/target/inner/f.txt`, with `aliasA` and `aliasB` both naming `target`.
# The pruned frame is the one at `target`, while the match is emitted by its descendant, so
# a match has to count towards every frame the walk is inside and not only the innermost
# one. Without that, this write is allowed and appends to the target.
mkdir -p "$TEST_DIR_ABS/nestedalias/root/target/inner"
printf "row1\n" > "$TEST_DIR_ABS/nestedalias/root/target/inner/f.txt"
ln -s target "$TEST_DIR_ABS/nestedalias/root/aliasA"
ln -s target "$TEST_DIR_ABS/nestedalias/root/aliasB"

# A collision that happens BEFORE its claiming frame has found any match. One child of
# `latematch/root` holds `back -> ..`, which loops the walk back into the root frame, and the
# other holds the only matching file, so the collision happens while the root frame has matched
# nothing yet. The file has two names under `root/**/*.txt` (`root/<match>/zzz.txt` and
# `root/<loop>/back/<match>/zzz.txt`, which the finite spellings `root/*/*.txt` and
# `root/*/*/*/*.txt` return one each), so the write must be refused.
#
# For the collision to precede the match, the walk has to enter the loop child first, and
# `fs::directory_iterator` performs no sorting: the order is `readdir` order, a property of the
# filesystem rather than of the names. Measured over identical name sets, ext4 and tmpfs return
# a hash order while XFS returns creation order, so neither naming nor creation order fixes
# which child comes first. So do not assume an order, read it: create both children, ask the
# filesystem which one it enumerates first (`ls -U`, the same unsorted order the iterator
# uses), and put the loop under that one. Adding entries inside the children does not reorder
# the parent, so the order observed here is the order the walk takes.
mkdir -p "$TEST_DIR_ABS/latematch/root/aaa" "$TEST_DIR_ABS/latematch/root/mmm"
LATEMATCH_LOOP=$(ls -U "$TEST_DIR_ABS/latematch/root" | head -1)
LATEMATCH_MATCH=$(ls -U "$TEST_DIR_ABS/latematch/root" | tail -1)
ln -s .. "$TEST_DIR_ABS/latematch/root/$LATEMATCH_LOOP/back"
printf "row1\n" > "$TEST_DIR_ABS/latematch/root/$LATEMATCH_MATCH/zzz.txt"

trap 'rm -rf "$TEST_DIR_ABS"' EXIT

# Ancestor-loop symlink: `loop/dir1/dir2/loop_to_root` points back at `loop/dir1`,
# so following `dir2/loop_to_root` recreates the same `dir1/dir2/loop_to_root`
# infinitely. The kernel would surface this as `Too many levels of symbolic
# links` after roughly 40 levels.
ln -s ../../dir1 "$TEST_DIR_ABS/loop/dir1/dir2/loop_to_root"

# Real cycle: recursive `**` glob would otherwise descend the loop until ELOOP.
# With visited-frame tracking the loop is broken and the real `file.txt` is
# read.
echo "ancestor-loop"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/loop/dir1/**/*.txt', 'TSV', 'val String')"

# Brace expansion: `{txt,csv}` expands into two separate walks. Both walks must
# enter `subdir` and report their respective files. With a global visited-path
# set the second walk would silently skip `subdir`; with per-pattern visited-frame
# tracking both files are returned.
echo "brace-expansion"
$CLICKHOUSE_CLIENT --query "SELECT _file FROM file('$TEST_DIR_NAME/brace/d/**/*.{txt,csv}', 'TSV', 'val String') ORDER BY _file"

# Independent symlinks pointing at the same target through brace alternatives.
# Each branch is its own descent and must succeed independently.
echo "independent-aliases"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/aliases/{parentA,parentB}/**/*.txt', 'TSV', 'val String')"

# Descendant-to-root: must return exactly 1 row (not 2). The visited-frame
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

# The expected answer is two rows: the file is reported through `a1/back` and through
# `a2/back`, with `via` values `a1` and `a2`. One row would mean the guard activated on
# this finite expansion, or that deduplication collapsed the two names.
echo "finite-glob-with-adjacent-asterisks"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/adj/root/a**/back/*.txt', 'TSV', 'val String')"
$CLICKHOUSE_CLIENT --query "SELECT splitByChar('/', _path)[-3] AS via FROM file('$TEST_DIR_NAME/adj/root/a**/back/*.txt', 'TSV', 'val String') ORDER BY via"

# Bounded finite tail after the last `**`: must return 1. The `**` matches `deep`, then
# the bounded suffix `mid/*/*.txt` reaches `f.txt` via the `back -> ../..` symlink. No
# `**` remains in the suffix, so no unbounded recursion is possible and the guard must
# not prune the match even though canonical(`.../mid/back`) equals the on-stack root.
echo "bounded-finite-tail-after-last-globstar"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/overprune/root/**/mid/*/*.txt', 'TSV', 'val String')"

# Adjacent globstars over a symlink loop, exercising the zero-level re-application
# while the remaining suffix still has another whole-segment `**`: must return 1
# (`top.txt`) and must not raise `Too many levels of symbolic links`. The row is
# reachable only through the zero-level re-application chain that re-enters the
# on-stack root; the `back -> ..` loop is broken during the recursive descent.
echo "adjacent-globstars-through-symlink-loop"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/globstarloop/root/**/**/*.txt', 'TSV', 'val String')"

# Re-entry with a different remaining pattern: both spellings must return 1. The
# only route to `top.txt` re-enters the on-stack root carrying a shorter remaining
# pattern, once with the symlink reached through a finite `*` and once with it named
# literally. Keying the visited-frame set on the canonical directory alone prunes
# that re-entry and both return 0.
echo "reenter-same-dir-different-remaining-pattern"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/reenter/root/*/**/top.txt', 'TSV', 'val String')"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/reenter/root/*/back/**/top.txt', 'TSV', 'val String')"

# Canonical deduplication: must return 1, not 2. Inside the bounded tail both aliases
# legitimately reach the one real file, so it is named under two different lexical
# paths; only a canonical deduplication key collapses them.
echo "canonical-dedup-through-ancestor-symlink"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/dedup/root/**/mid/*/f.txt', 'TSV', 'val String')"

# Traversal pruning under several sibling ancestor loops: must return 4 and must
# finish. Unpruned, the three `up -> ../..` links let the walk re-descend every
# branch from the re-entered root, so the visited-path count grows exponentially and
# the walk runs until the kernel symlink limit. Output deduplication cannot prevent
# that because it filters results rather than traversal, so this is the assertion
# that fails if the visited-frame pruning is removed while every other scenario
# here still passes.
echo "branching-ancestor-loops-are-pruned"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/branching/root/**/*.txt', 'TSV', 'val String')"

# Dense alias graph without any ancestor cycle: must return 10 and must finish. Every
# directory is reachable under many names, so pruning only the frames still being
# descended leaves the path count growing combinatorially with the number of aliases.
# This is the assertion that fails if completed frames are forgotten rather than kept.
# Measured on this shape, forgetting them costs 0.5s at six aliases, 30s at eight and
# over 700s at ten, while keeping them stays at about 0.1s throughout.
echo "dense-alias-graph-is-bounded"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/aliasgraph/root/**/*.txt', 'TSV', 'val String')"

# Finite `*` components BEFORE the `**`, over a chain of aliased directories: must return 1
# and must finish. The nine `*` levels are walked before any `**` is reached, so this is the
# assertion that fails if frame tracking is narrowed to the `**` segment itself while every
# other scenario here still passes, including the two above, which both begin at a `**`.
echo "finite-prefix-before-globstar-is-pruned"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/prefixchain/root/*/*/*/*/*/*/*/*/*/**/f.txt', 'TSV', 'val String')"

# A file symlink beside its target: both names must still be reported, by a plain `*`,
# by a recursive `**`, and by an implicit directory listing. The `**` spelling is the one
# that uses the canonical key, so it is what fails if the key resolves the final path
# component instead of only its parent.
echo "file-symlink-keeps-both-names"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/filelink/root/*.txt', 'TSV', 'val String')"
$CLICKHOUSE_CLIENT --query "SELECT _file FROM file('$TEST_DIR_NAME/filelink/root/*.txt', 'TSV', 'val String') ORDER BY _file"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/filelink/root', 'TSV', 'val String')"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/filelink/root/**/*.txt', 'TSV', 'val String')"
$CLICKHOUSE_CLIENT --query "SELECT _file FROM file('$TEST_DIR_NAME/filelink/root/**/*.txt', 'TSV', 'val String') ORDER BY _file"

# Writes through a glob stay refused even when deduplication leaves one path. The
# pattern below matches one file under two names, so a read-only test based on the
# number of surviving paths would let the write through and modify the target.
# `</dev/null` is required: an `INSERT ... VALUES` makes the client read the data from
# standard input, so with the runner's standard input still open it would block until
# the test times out instead of reporting the refusal.
echo "glob-insert-stays-readonly"
$CLICKHOUSE_CLIENT --query "INSERT INTO TABLE FUNCTION file('$TEST_DIR_NAME/dedup/root/**/mid/*/f.txt', 'TSV', 'val String') VALUES ('written')" </dev/null 2>&1 \
    | grep -qF "readonly mode because of globs" && echo "refused"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/dedup/root/f.txt', 'TSV', 'val String')"

# The same refusal when the collapse comes from traversal pruning rather than from a
# dropped matched path: one directory under two alias names. The row count afterwards is
# what shows the write did not land.
echo "aliased-dir-insert-stays-readonly"
$CLICKHOUSE_CLIENT --query "INSERT INTO TABLE FUNCTION file('$TEST_DIR_NAME/aliaswrite/root/**/f.txt', 'TSV', 'val String') VALUES ('written')" </dev/null 2>&1 \
    | grep -qF "readonly mode because of globs" && echo "refused"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/aliaswrite/root/target/f.txt', 'TSV', 'val String')"

# A finite glob matching one file stays WRITABLE. Without this assertion, a read-only
# guard widened to any source containing glob syntax passes every other check here,
# because the finite scenarios only read and the write scenarios all expect refusal.
echo "single-match-glob-insert-is-allowed"
$CLICKHOUSE_CLIENT --query "INSERT INTO TABLE FUNCTION file('$TEST_DIR_NAME/onematch/*.txt', 'TSV', 'val String') VALUES ('written')" </dev/null 2>&1 \
    | grep -qF "readonly mode because of globs" && echo "unexpectedly refused"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/onematch/only.txt', 'TSV', 'val String')"

# Pruning an aliased frame that matched nothing is not a collapse, so this write is allowed
# too. The row count afterwards shows it landed.
echo "empty-alias-insert-is-allowed"
$CLICKHOUSE_CLIENT --query "INSERT INTO TABLE FUNCTION file('$TEST_DIR_NAME/emptyalias/root/**/only.tsv', 'TSV', 'val String') VALUES ('written')" </dev/null 2>&1 \
    | grep -qF "readonly mode because of globs" && echo "unexpectedly refused"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/emptyalias/root/only.tsv', 'TSV', 'val String')"

# The refusal must also hold when the match is emitted below the aliased directory rather
# than inside it, which is the case a per-frame count reaching only the innermost frame
# misses. The row count afterwards shows the write did not land.
echo "nested-alias-insert-stays-readonly"
$CLICKHOUSE_CLIENT --query "INSERT INTO TABLE FUNCTION file('$TEST_DIR_NAME/nestedalias/root/**/f.txt', 'TSV', 'val String') VALUES ('written')" </dev/null 2>&1 \
    | grep -qF "readonly mode because of globs" && echo "refused"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/nestedalias/root/target/inner/f.txt', 'TSV', 'val String')"

# The refusal must hold when the alias collides before the claiming frame has matched anything,
# which is what deciding after the walk rather than at the collision is for. The finite
# spellings show the two names the recursive pattern reaches, and each staying at one row shows
# that no write leaked through.
echo "late-match-alias-insert-stays-readonly"
$CLICKHOUSE_CLIENT --query "INSERT INTO TABLE FUNCTION file('$TEST_DIR_NAME/latematch/root/**/*.txt', 'TSV', 'val String') VALUES ('written')" </dev/null 2>&1 \
    | grep -qF "readonly mode because of globs" && echo "refused"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/latematch/root/*/*.txt', 'TSV', 'val String')"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM file('$TEST_DIR_NAME/latematch/root/*/*/*/*.txt', 'TSV', 'val String')"

# Server alive afterwards.
$CLICKHOUSE_CLIENT --query "SELECT 'alive'"
