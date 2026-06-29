#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: uses clickhouse-disks; its built-in `local` disk is config-independent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Private dir so the built-in `local` disk (rooted at cwd) sees a stable relative
# path; the `file:` line is cwd-dependent and filtered out below for determinism.
workdir="${CLICKHOUSE_TMP}/04400_read_bitmap.$$"
rm -rf "$workdir"
mkdir -p "$workdir"
trap 'rm -rf "$workdir"' EXIT

run_read_bitmap() {
    # Strip the volatile `file:` header line and keep only stable output.
    ( cd "$workdir" && clickhouse-disks --disk local --query "$1" 2>&1 ) | grep -v "^file:"
}

# Valid `.rbm` for {3, 7, 42, 12345, 1000000} from the real DeleteBitmap::serialize
# writer (kept in lockstep with gtest DeleteBitmapInspectTest).
base64 -d > "$workdir/delete_bitmap_5.rbm" <<'EOF'
UkJNMQEAAAAiAAAAOjAAAAIAAAAAAAMADwAAABgAAAAgAAAAAwAHACoAOTBAQr1bQ+g=
EOF

echo "--- valid (header + stats) ---"
run_read_bitmap "read-bitmap delete_bitmap_5.rbm"

echo "--- valid (--values dumps set bits) ---"
run_read_bitmap "read-bitmap --values delete_bitmap_5.rbm"

echo "--- bad magic ---"
# Not the RBM1 magic. --query mode prints the error and returns 0, so assert on output.
printf 'NOTRBM1XXXXXX' > "$workdir/bad_magic.rbm"
run_read_bitmap "read-bitmap bad_magic.rbm" | grep -oE "magic:     BAD|not a delete-bitmap \(\.rbm\) file \(bad magic\)"

echo "--- truncated header ---"
# Fewer than the 16-byte minimum header.
printf 'RBM' > "$workdir/trunc.rbm"
run_read_bitmap "read-bitmap trunc.rbm" | grep -oE "not a delete-bitmap \(\.rbm\) file \(truncated: 3 bytes, need at least a 16-byte header\)"

echo "--- unsupported version ---"
# Valid RBM1 magic, version 99: rejected before the body buffer is allocated.
printf 'RBM1\x63\x00\x00\x00\x08\x00\x00\x00' > "$workdir/bad_version.rbm"
run_read_bitmap "read-bitmap bad_version.rbm" | grep -oE "version:   99 \(UNSUPPORTED\)|unsupported delete-bitmap version 99"

echo "--- missing file ---"
# Reported up front, before any read.
run_read_bitmap "read-bitmap no_such_file.rbm" | grep -oE "does not exist"
