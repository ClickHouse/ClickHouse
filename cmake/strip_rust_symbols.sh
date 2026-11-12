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
# Usage: strip_rust_symbols.sh <library.a> <ar> <objcopy> <ld> <symbol1> [symbol2] ...
#
# The ar/objcopy/ld tools must be from a compatible LLVM toolchain
# (e.g., llvm-ar, llvm-objcopy, ld.lld) to avoid LLVM version mismatches
# with Rust-compiled code.

set -e

LIB_PATH=$(realpath "$1")
AR="$2"
OBJCOPY="$3"
LD="$4"
shift 4

if [ -z "$LIB_PATH" ] || [ $# -eq 0 ]; then
    echo "Usage: $0 <library.a> <ar> <objcopy> <ld> <symbol1> [symbol2] ..." >&2
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap "rm -rf '$WORK_DIR'" EXIT

# Extract object files
mkdir "$WORK_DIR/objs"
(cd "$WORK_DIR/objs" && "$AR" x "$LIB_PATH")

# Partial link: combine all .o into one, resolving internal cross-references.
"$LD" -r -o "$WORK_DIR/combined.o" "$WORK_DIR"/objs/*.o

# Build objcopy flags to keep only the specified public symbols
KEEP_FLAGS=""
for sym in "$@"; do
    KEEP_FLAGS="$KEEP_FLAGS --keep-global-symbol=$sym"
done

# Localize all symbols except the public ones, then strip unneeded locals
"$OBJCOPY" $KEEP_FLAGS --strip-unneeded "$WORK_DIR/combined.o" "$WORK_DIR/stripped.o"

# Repackage as .a (replace original)
rm -f "$LIB_PATH"
"$AR" rcs "$LIB_PATH" "$WORK_DIR/stripped.o"
