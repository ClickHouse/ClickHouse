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

# wrap_codec_corpus <src_fuzzer> <method_byte> <header_size> <size_offset> <dst_fuzzer>
#
# Re-encodes every seed of a single-codec decompressor fuzzer as a one-element
# codec chain for multiple_decompress_fuzzer.  The two input grammars differ, so
# a raw file copy would only add seeds that fail in the chain header parse:
#
#   single-codec seed:   [AuxiliaryRandomData][codec payload]
#   multiple seed:       [decompressed_size:8][chain_length:1][codec byte...]
#                        [block header: method:1 compressed_size:4 decompressed_size:4][codec payload]
#
# The single-codec harnesses use `decompressed_size % 65536` as the output
# buffer size, and `CompressionCodecMultiple` requires the final block header
# to carry exactly the size it was asked to produce, so the same reduced value
# is written into both places.  Seeds that reduce to 0 are skipped: a zero
# decompressed size is rejected by `readDecompressedBlockSize` before any codec
# code runs.  Output files are named by the SHA-1 of their content, matching the
# libFuzzer corpus convention, so re-running the sync is idempotent.
wrap_codec_corpus() {
    local src_fuzzer="$1"
    local method_byte="$2"
    local header_size="$3"
    local size_offset="$4"
    local dst_fuzzer="$5"
    local src_dir="${BASE_DIR}/${src_fuzzer}"
    local dst_dir="${BASE_DIR}/${dst_fuzzer}"

    echo "Wrapping: $src_fuzzer → $dst_fuzzer (codec byte 0x$method_byte)"

    if [[ ! -d "$src_dir" ]]; then
        echo "  [SKIP] source directory not found: $src_dir"
        return
    fi

    mkdir -p "$dst_dir"

    local considered=0
    local written=0
    local skipped=0
    local seed
    while IFS= read -r -d '' seed; do
        considered=$(( considered + 1 ))

        local seed_size
        seed_size=$(stat -c %s "$seed")
        if [[ "$seed_size" -lt "$header_size" ]]; then
            skipped=$(( skipped + 1 ))
            continue
        fi

        # Little-endian 64-bit decompressed_size from the harness header,
        # reduced the same way the harness reduces it.
        local decompressed_size
        decompressed_size=$(od -An -t u8 -j "$size_offset" -N 8 "$seed" | tr -d ' ')
        decompressed_size=$(( decompressed_size % 65536 ))
        if [[ "$decompressed_size" -eq 0 ]]; then
            skipped=$(( skipped + 1 ))
            continue
        fi

        local payload_size=$(( seed_size - header_size ))
        # Inner block = 9-byte header + payload.
        local compressed_size=$(( payload_size + 9 ))

        local tmp_file
        tmp_file=$(mktemp "$dst_dir/.wrap.XXXXXX")
        {
            # [decompressed_size:8] for the multiple_decompress_fuzzer harness.
            le_bytes "$decompressed_size" 8
            # [chain_length:1][codec byte:1]
            printf '\x01'
            printf "\\x$method_byte"
            # Inner compressed block header: [method:1][compressed_size:4][decompressed_size:4]
            printf "\\x$method_byte"
            le_bytes "$compressed_size" 4
            le_bytes "$decompressed_size" 4
            # Codec payload, verbatim.
            tail -c "$payload_size" "$seed"
        } > "$tmp_file"

        local name
        name=$(sha1sum "$tmp_file" | cut -d' ' -f1)
        if [[ -e "$dst_dir/$name" ]]; then
            rm -f "$tmp_file"
        else
            mv "$tmp_file" "$dst_dir/$name"
            written=$(( written + 1 ))
        fi
    done < <(find "$src_dir" -maxdepth 1 -type f -print0)

    echo "  Wrapped $written / $considered seed(s) from '$src_dir' → '$dst_dir' ($skipped skipped as too short or zero-sized)"
}

# le_bytes <value> <width>
#
# Writes <value> as a little-endian unsigned integer of <width> bytes to stdout.
le_bytes() {
    local value="$1"
    local width="$2"
    local i
    for (( i = 0; i < width; i++ )); do
        printf "\\x$(printf '%02x' $(( (value >> (8 * i)) & 0xff )))"
    done
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

# 1. Individual codec fuzzers → multi-codec chain fuzzer.
#
#    The single-codec decompressor fuzzers and multiple_decompress_fuzzer do not
#    share an input grammar: the former feed a bare codec payload to
#    doDecompressData, the latter needs a `Multiple` chain descriptor followed
#    by a full compressed block.  Each seed is therefore re-encoded as a
#    one-element chain rather than copied verbatim.
#
#    Arguments: <src_fuzzer> <CompressionMethodByte> <header size> <offset of
#    decompressed_size within the header>.  gorilla/t64 use
#    `struct { size_t decompressed_size; }` (8 bytes); delta uses
#    `struct { UInt8 delta_size_bytes; size_t decompressed_size; }`, which is
#    16 bytes with the size at offset 8 after padding.
#
#    The data-type fuzzers (decode_data_type_fuzzer, binary `BinaryTypeIndex`
#    stream vs data_type_deserialization_fuzzer, textual type name followed by
#    a newline) are deliberately not paired: converting between those grammars
#    needs the type parser, so a file copy would only produce seeds that fail
#    in the first parse step.

echo "=== Compression codec fuzzers → multiple_decompress_fuzzer ==="
wrap_codec_corpus "gorilla_decompress_fuzzer" 95 8  0 "multiple_decompress_fuzzer"
wrap_codec_corpus "t64_decompress_fuzzer"     93 8  0 "multiple_decompress_fuzzer"
wrap_codec_corpus "delta_decompress_fuzzer"   92 16 8 "multiple_decompress_fuzzer"
echo ""

echo "Corpus sync complete."
