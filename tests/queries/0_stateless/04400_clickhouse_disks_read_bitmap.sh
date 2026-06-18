#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses the clickhouse-disks utility; the built-in `local` disk
# (rooted at cwd) is independent of the server storage config, so no other tags needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Work in a private dir so the built-in `local` disk (rooted at the launch cwd)
# resolves a stable relative path. The `read-bitmap` command echoes a
# cwd-dependent `file:` line and a `(showing ...)` count we don't assert on; we
# filter those and the stack trace so the reference is deterministic.
workdir="${CLICKHOUSE_TMP}/04400_read_bitmap.$$"
rm -rf "$workdir"
mkdir -p "$workdir"
trap 'rm -rf "$workdir"' EXIT

run_read_bitmap() {
    # Strip the volatile `file:` header line and keep only stable output.
    ( cd "$workdir" && clickhouse-disks --disk local --query "$1" 2>&1 ) | grep -v "^file:"
}

# A valid `.rbm` for the bitmap {3, 7, 42, 12345, 1000000}, produced by the real
# DeleteBitmap::serialize writer (see gtest DeleteBitmapInspectTest). magic RBM1,
# version 1 (Roaring32), CRC-valid.
base64 -d > "$workdir/delete_bitmap_5.rbm" <<'EOF'
UkJNMQEAAAAiAAAAOjAAAAIAAAAAAAMADwAAABgAAAAgAAAAAwAHACoAOTBAQr1bQ+g=
EOF

echo "--- valid (header + stats) ---"
run_read_bitmap "read-bitmap delete_bitmap_5.rbm"

echo "--- valid (--values dumps set bits) ---"
run_read_bitmap "read-bitmap --values delete_bitmap_5.rbm"

echo "--- bad magic ---"
# A dozen bytes that are not the RBM1 magic. In --query mode the disks app prints
# the error and returns 0, so we assert on the printed lines, not $?.
printf 'NOTRBM1XXXXXX' > "$workdir/bad_magic.rbm"
run_read_bitmap "read-bitmap bad_magic.rbm" | grep -oE "magic:     BAD|not a delete-bitmap \(\.rbm\) file \(bad magic\)"

echo "--- truncated header ---"
# Fewer than the 16-byte minimum header — reported as truncated.
printf 'RBM' > "$workdir/trunc.rbm"
run_read_bitmap "read-bitmap trunc.rbm" | grep -oE "not a delete-bitmap \(\.rbm\) file \(truncated: 3 bytes, need at least a 16-byte header\)"

echo "--- missing file ---"
# A path that doesn't exist — reported up front, before any read.
run_read_bitmap "read-bitmap no_such_file.rbm" | grep -oE "does not exist"
