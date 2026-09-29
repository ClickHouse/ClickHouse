#!/usr/bin/env bash
#
# Strip internal symbols from a Rust static library (.a), keeping only
# the specified public symbols visible.
#
# This dramatically reduces symbol table bloat from Rust libraries like prqlc,
# whose parser combinator (chumsky) generates deeply nested generic types
# with very long symbol names.
#
# The process:
# 1. Extract .o files from the .a archive
# 2. Partial-link them into a single .o (resolves all internal cross-references)
# 3. Localize all symbols except the specified public ones
# 4. Strip symbols not needed for relocations
# 5. Repackage as .a
#
# Usage: strip_rust_symbols.sh <input.a> <output.a> <ar> <objcopy> <ld> <symbol1> [symbol2] ...
#
# The input archive is never modified: the stripped archive is written to
# <output.a>, so the step is an ordinary input -> output build rule that the
# build system re-runs only when an input changed.  The output is written to a
# temporary file and renamed into place, so a failed run never leaves a
# half-written <output.a> that looks up to date.
#
# The ar/objcopy/ld tools must be from a compatible LLVM toolchain
# (e.g., llvm-ar, llvm-objcopy, ld.lld) to avoid LLVM version mismatches
# with Rust-compiled code.

set -eu

IN_PATH="${1:-}"
OUT_PATH="${2:-}"
AR="${3:-}"
OBJCOPY="${4:-}"
LD="${5:-}"

if [ -z "$IN_PATH" ] || [ -z "$OUT_PATH" ] || [ -z "$AR" ] || [ -z "$OBJCOPY" ] || [ -z "$LD" ] || [ $# -lt 6 ]; then
    echo "Usage: $0 <input.a> <output.a> <ar> <objcopy> <ld> <symbol1> [symbol2] ..." >&2
    exit 1
fi
shift 5

if [ ! -f "$IN_PATH" ]; then
    echo "Error: Rust library not found: $IN_PATH" >&2
    exit 1
fi

if [ "$IN_PATH" -ef "$OUT_PATH" ]; then
    echo "Error: input and output must be different files: $IN_PATH" >&2
    exit 1
fi

IN_PATH=$(realpath "$IN_PATH")
TMP_OUT="$OUT_PATH.tmp"
WORK_DIR=$(mktemp -d)
cleanup() { rm -rf "$WORK_DIR"; rm -f "$TMP_OUT"; }
trap cleanup EXIT

# Extract object files
mkdir "$WORK_DIR/objs"
(cd "$WORK_DIR/objs" && "$AR" x "$IN_PATH")

# Partial link: combine all .o into one, resolving internal cross-references.
"$LD" -r -o "$WORK_DIR/combined.o" "$WORK_DIR"/objs/*.o

# Build objcopy flags to keep only the specified public symbols
KEEP_FLAGS=()
for sym in "$@"; do
    KEEP_FLAGS+=("--keep-global-symbol=$sym")
done

# Localize all symbols except the public ones, then strip unneeded locals
"$OBJCOPY" "${KEEP_FLAGS[@]}" --strip-unneeded "$WORK_DIR/combined.o" "$WORK_DIR/stripped.o"

# Repackage as .a.  `ar r` adds to an existing archive, so start from scratch.
rm -f "$TMP_OUT"
"$AR" rcs "$TMP_OUT" "$WORK_DIR/stripped.o"
mv -f "$TMP_OUT" "$OUT_PATH"
