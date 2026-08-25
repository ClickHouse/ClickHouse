#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `**` is a globstar (matching zero or more directory components) only when it forms a whole
# path segment. A `**` adjacent to other characters in the segment (e.g. `?**`) is NOT a
# globstar: it behaves like the legacy `*` expansion within a single directory level. This must
# hold for the local `file(...)` listing path, consistently with `makeRegexpPatternFromGlobs`
# (see the gtest in `src/Common/tests/gtest_makeRegexpPatternFromGlobs.cpp`).

# Use a unique per-test subdirectory under `user_files`: the flaky check runs the same test
# concurrently many times, and a fixed directory name would let those instances clobber each
# other's files.
dir="${CLICKHOUSE_DATABASE}"

rm -rf "${USER_FILES_PATH:?}/$dir"
mkdir -p "$USER_FILES_PATH/$dir/sub/subsub"

echo -e '0\t0' > "$USER_FILES_PATH/$dir/pick.tsv"
echo -e '1\t1' > "$USER_FILES_PATH/$dir/sub/pick.tsv"
echo -e '2\t2' > "$USER_FILES_PATH/$dir/sub/subsub/pick.tsv"

# Whole-segment `**/` is a globstar: matches zero, one and two directory levels.
echo "whole-segment **/ matches all levels including zero:"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('$dir/**/pick.tsv', 'TSV', 'a UInt8, b UInt8') ORDER BY a"

# Non-whole-segment `?**/` is not a globstar: the leading `?` requires at least one character in
# a single directory name, so it matches exactly one directory level (here `sub`), and never the
# zero-level file at the top nor the file nested two levels deep.
echo "non-whole-segment ?**/ matches exactly one directory level (only sub/pick.tsv):"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('$dir/?**/pick.tsv', 'TSV', 'a UInt8, b UInt8') ORDER BY a"

rm -rf "${USER_FILES_PATH:?}/$dir"
