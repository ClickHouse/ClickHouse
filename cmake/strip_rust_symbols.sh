#!/usr/bin/env bash
#
# Strip internal symbols from a Rust static library (.a): localize and drop
# the Rust symbols and those of the C libraries bundled into it, keeping the
# C (and C++) entry points of the crates visible.
#
# This dramatically reduces symbol table bloat from Rust libraries like prqlc,
# whose parser combinator (chumsky) generates deeply nested generic types
# with very long symbol names.
#
# The process:
# 1. Extract .o files from the .a archive
# 2. Partial-link them into a single .o (resolves all internal cross-references)
# 3. Localize the Rust symbols: legacy mangling (`_ZN...17h<hash>E`, the
#    standard library) and v0 (`_R...`, our crates). C++ calls none of them.
#    Also localize the strong globals of the objects built neither by rustc nor
#    by `cxx`: the C libraries that `-sys` crates compile and bundle (zstd, LZ4,
#    Oniguruma, ...), and those of `compiler_builtins`. ClickHouse links its own
#    copies of many of them, and in the one object of the partial link each is a
#    duplicate definition, not an archive member the linker leaves unpulled.
# 4. Strip symbols not needed for relocations
# 5. Repackage as .a
#
# Usage: strip_rust_symbols.sh <library.a> <ar> <objcopy> <ld> <nm>
#
# The ar/objcopy/ld/nm tools must be from a compatible LLVM toolchain
# (e.g., llvm-ar, llvm-objcopy, ld.lld, llvm-nm) to avoid LLVM version
# mismatches with Rust-compiled code.

set -e

LIB_PATH=$(realpath "$1")
AR="$2"
OBJCOPY="$3"
LD="$4"
NM="$5"
shift 5

if [ -z "$LIB_PATH" ] || [ -z "$NM" ] || [ $# -ne 0 ]; then
    echo "Usage: $0 <library.a> <ar> <objcopy> <ld> <nm>" >&2
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap "rm -rf '$WORK_DIR'" EXIT

# Extract object files
mkdir "$WORK_DIR/objs"
(cd "$WORK_DIR/objs" && "$AR" x "$LIB_PATH")

# The strong globals of the bundled C libraries and of `compiler_builtins`. The objects of rustc are `*.rcgu.o`; those
# of `cxx` are its runtime, `<hash>-cxx.o`, and the bridges, `<hash>-<file>.rs.o`, which define the C++ entry points
# (the `cc` crate prefixes the name of every object with a hash).
: > "$WORK_DIR/bundled.txt"
for obj in "$WORK_DIR"/objs/*.o; do
    case "$(basename "$obj")" in
        compiler_builtins-*.rcgu.o) ;;
        *.rcgu.o | *.rs.o | *-cxx.o) continue ;;
    esac
    "$NM" -g -P --defined-only "$obj" | awk '$2 !~ /^[VvWw]$/ { print $1 }' >> "$WORK_DIR/bundled.txt"
done

# Partial link: combine all .o into one, resolving internal cross-references.
"$LD" -r -o "$WORK_DIR/combined.o" "$WORK_DIR"/objs/*.o

# Localize the Rust symbols and the bundled ones, then strip unneeded locals
# Drop .llvm_addrsig: after ld -r and objcopy its sh_link is 0, so lld --icf=safe ignores it with a warning
"$OBJCOPY" --wildcard --localize-symbol='_ZN*17h????????????????E*' --localize-symbol='_R[0-9A-Z]*' \
    --localize-symbols="$WORK_DIR/bundled.txt" \
    --strip-unneeded --remove-section=.llvm_addrsig "$WORK_DIR/combined.o" "$WORK_DIR/stripped.o"

# Repackage as .a (replace original)
rm -f "$LIB_PATH"
"$AR" rcs "$LIB_PATH" "$WORK_DIR/stripped.o"
