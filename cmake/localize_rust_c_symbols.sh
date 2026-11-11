#!/usr/bin/env bash
#
# Localize the C library symbols a Rust static library (.a) defines that
# ClickHouse also provides its own definitions for.
#
# Rust static libraries bundle compiler_builtins and libm, which export
# standard C symbols (compiler-rt builtins like __multi3/__divti3, libm
# functions like cbrt/sqrt, etc.) as globals.  When linked before our own
# implementations, these silently win, bypassing our -falign-functions=64,
# -march and other optimization flags.
#
# Rather than a hand-maintained allowlist, we localize exactly the set of
# symbols that BOTH the Rust archive defines as globals AND the reference
# libraries (libllvmlibc, compiler-rt builtins, the platform libm) define.
# This is self-maintaining and fail-safe:
#   - A symbol is localized only when another definition exists in a library
#     that the final binary links, so the link always resolves it to that copy.
#   - Symbols Rust defines that nothing else provides (crate FFI entry points
#     such as prql_to_sql, Rust runtime helpers, and even libm functions absent
#     from these references) are never in the intersection and are left untouched.
#
# Functions like memcpy/memset appear only as undefined references in the Rust
# archives, so they resolve from our implementations at link time anyway and
# need no localization here.
#
# Usage: localize_rust_c_symbols.sh <library.a> <ar> <objcopy> <nm> <ref>...
#
# <ref>... are the reference libraries whose defined symbols determine what to
# localize: libllvmlibc and clang_rt_builtins (ClickHouse-built static archives)
# and the platform libm (a sysroot/toolchain static archive or shared object).
# At least one is required; CMake passes the ones that exist for the target.
#
# The ar/objcopy/nm tools must come from a compatible LLVM toolchain
# (e.g. llvm-ar, llvm-objcopy, llvm-nm).  CMake resolves them via
# ch_find_program and passes the full paths so this script never has to
# search $PATH or infer one tool's name from another.

set -eu

# Keep sort and comm on the same portable bytewise ordering.
export LC_ALL=C

LIB_PATH="${1:-}"
AR="${2:-}"
OBJCOPY="${3:-}"
NM="${4:-}"

if [ -z "$LIB_PATH" ] || [ -z "$AR" ] || [ -z "$OBJCOPY" ] || [ -z "$NM" ]; then
    echo "Usage: $0 <library.a> <ar> <objcopy> <nm> <ref>..." >&2
    exit 1
fi
shift 4

if [ "$#" -eq 0 ]; then
    echo "Error: no reference libraries given; cannot decide which symbols to localize" >&2
    exit 1
fi
REF_LIBS=("$@")

if [ ! -f "$LIB_PATH" ]; then
    echo "Error: Rust library not found: $LIB_PATH" >&2
    echo "If this is a workspace subcrate, set IMPORTED_LOCATION on the target" >&2
    echo "before calling clickhouse_config_crate_flags()." >&2
    exit 1
fi

TMPFILE=$(mktemp)
REF_SYMS=$(mktemp)
LOCALIZE_FILE=$(mktemp)
WORK_DIR=""
cleanup() { rm -f "$TMPFILE" "$REF_SYMS" "$LOCALIZE_FILE"; [ -n "$WORK_DIR" ] && rm -rf "$WORK_DIR"; true; }
trap cleanup EXIT

# Print the names of all globally-defined symbols in a library.  Handles both
# static archives (plain nm) and shared objects (nm -D reads the dynamic symbol
# table of an otherwise-stripped .so); the union covers either kind.  Glibc
# version suffixes such as @@GLIBC_2.27 are stripped so names compare against the
# Rust archive.  nm may print per-member errors for non-ELF members (debug
# metadata, linker scripts); those are non-fatal as long as one member is read.
# T/D/B/C/W/V are the defined globals and i is an IFUNC (glibc resolves ceil,
# rint, trunc, ... this way); undefined (U) and local (lowercase) are excluded.
defined_globals() {
    { "$NM" "$1" 2>/dev/null; "$NM" -D "$1" 2>/dev/null; } \
        | grep -E '^[0-9a-f]+ [TDBCWVi] ' \
        | awk '{print $3}' \
        | sed 's/@.*//'
}

# Defined globals from our own reference libraries: the set we are allowed to
# localize in the Rust archive.
for ref in "${REF_LIBS[@]}"; do
    if [ ! -f "$ref" ]; then
        echo "Error: reference library not found: $ref" >&2
        exit 1
    fi
done

for ref in "${REF_LIBS[@]}"; do
    defined_globals "$ref"
done | sort -u > "$REF_SYMS"

if [ ! -s "$REF_SYMS" ]; then
    echo "Error: reference libraries produced no symbols; cannot localize Rust C symbols" >&2
    exit 1
fi

# Defined globals from the Rust archive.  A truly broken/missing archive
# produces empty output and is caught below.
defined_globals "$LIB_PATH" | sort -u > "$TMPFILE" || true

if [ ! -s "$TMPFILE" ]; then
    echo "Error: $NM produced no symbols for $LIB_PATH; cannot localize Rust C symbols" >&2
    exit 1
fi

# Localize the intersection: symbols the Rust archive defines that we also define.
comm -12 "$TMPFILE" "$REF_SYMS" > "$LOCALIZE_FILE"

if [ -s "$LOCALIZE_FILE" ]; then
    # Try direct objcopy on the archive first (fast path).
    # Some Rust archives contain non-ELF members (e.g. debug metadata objects)
    # that cause llvm-objcopy to fail.  In that case, fall back to extracting
    # individual members, processing only valid ELF objects, and repacking.
    if ! "$OBJCOPY" --localize-symbols="$LOCALIZE_FILE" "$LIB_PATH" 2>/dev/null; then
        WORK_DIR=$(mktemp -d)

        (cd "$WORK_DIR" && "$AR" x "$LIB_PATH")
        for obj in "$WORK_DIR"/*.o; do
            "$OBJCOPY" --localize-symbols="$LOCALIZE_FILE" "$obj" 2>/dev/null || true
        done
        "$AR" rcs "$LIB_PATH" "$WORK_DIR"/*.o
    fi
fi
