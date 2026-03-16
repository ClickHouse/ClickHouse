#!/bin/bash
# Sync corpus between related ClickHouse fuzzers.
#
# Cross-pollinates corpus entries so that fuzzers covering overlapping input
# spaces can benefit from each other's discoveries.
#
# Usage:
#   ./corpus_sync.sh <corpus_base_dir>
#
# corpus_base_dir should contain subdirectories named after each fuzzer, e.g.:
#   corpus_base_dir/native_reader_fuzzer/
#   corpus_base_dir/format_fuzzer/
#   corpus_base_dir/data_type_deserialization_fuzzer/
#   corpus_base_dir/decode_data_type_fuzzer/
#   corpus_base_dir/gorilla_decompress_fuzzer/
#   corpus_base_dir/t64_decompress_fuzzer/
#   corpus_base_dir/delta_decompress_fuzzer/
#   corpus_base_dir/multiple_decompress_fuzzer/

set -euo pipefail

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

usage() {
    echo "Usage: $0 <corpus_base_dir>" >&2
    exit 1
}

# copy_corpus <src_dir> <dst_dir>
#
# Copies all files from src_dir into dst_dir using cp -n (no-clobber) so that
# existing corpus entries are never overwritten.  Prints the number of files
# considered and the number actually copied.
copy_corpus() {
    local src_dir="$1"
    local dst_dir="$2"

    if [[ ! -d "$src_dir" ]]; then
        echo "  [SKIP] source directory not found: $src_dir"
        return
    fi

    mkdir -p "$dst_dir"

    # Count files in source before copying.
    local src_count
    src_count=$(find "$src_dir" -maxdepth 1 -type f | wc -l | tr -d ' ')

    if [[ "$src_count" -eq 0 ]]; then
        echo "  [SKIP] source directory is empty: $src_dir"
        return
    fi

    # Count files already present in destination before the copy.
    local before_count
    before_count=$(find "$dst_dir" -maxdepth 1 -type f | wc -l | tr -d ' ')

    # Copy without overwriting.  cp -n is POSIX-compatible on Linux and macOS.
    find "$src_dir" -maxdepth 1 -type f -exec cp -n {} "$dst_dir/" \;

    # Count files in destination after the copy.
    local after_count
    after_count=$(find "$dst_dir" -maxdepth 1 -type f | wc -l | tr -d ' ')

    local copied=$(( after_count - before_count ))
    echo "  Copied $copied / $src_count file(s) from '$src_dir' → '$dst_dir' ($before_count existing, $after_count total)"
}

# sync_pair <src_fuzzer> <dst_fuzzer>
#
# Convenience wrapper: resolves absolute paths and calls copy_corpus.
sync_pair() {
    local src_fuzzer="$1"
    local dst_fuzzer="$2"
    local src_dir="${BASE_DIR}/${src_fuzzer}"
    local dst_dir="${BASE_DIR}/${dst_fuzzer}"

    echo "Syncing: $src_fuzzer → $dst_fuzzer"
    copy_corpus "$src_dir" "$dst_dir"
}

# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    usage
fi

BASE_DIR="${1%/}"   # strip trailing slash for consistent path construction

if [[ ! -d "$BASE_DIR" ]]; then
    echo "Error: corpus base directory not found: $BASE_DIR" >&2
    exit 1
fi

echo "Corpus base directory: $BASE_DIR"
echo ""

# ---------------------------------------------------------------------------
# Cross-pollination rules
# ---------------------------------------------------------------------------

# 1. Binary type encodings → text-format type deserialization (and vice versa).
#
#    decode_data_type_fuzzer operates on binary-encoded type representations,
#    while data_type_deserialization_fuzzer uses textual type names.  Sharing
#    corpus in both directions helps each fuzzer explore edge cases discovered
#    by the other.

echo "=== Data-type fuzzer pair ==="
sync_pair "decode_data_type_fuzzer" "data_type_deserialization_fuzzer"
sync_pair "data_type_deserialization_fuzzer" "decode_data_type_fuzzer"
echo ""

# 2. Individual codec fuzzers → multi-codec chain fuzzer.
#
#    gorilla_decompress_fuzzer, t64_decompress_fuzzer, and
#    delta_decompress_fuzzer all produce valid codec outputs that can serve as
#    inputs to codec chain processing inside multiple_decompress_fuzzer.

echo "=== Compression codec fuzzers → multiple_decompress_fuzzer ==="
sync_pair "gorilla_decompress_fuzzer" "multiple_decompress_fuzzer"
sync_pair "t64_decompress_fuzzer"     "multiple_decompress_fuzzer"
sync_pair "delta_decompress_fuzzer"   "multiple_decompress_fuzzer"
echo ""

echo "Corpus sync complete."
