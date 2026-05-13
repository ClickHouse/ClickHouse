#!/usr/bin/env bash
#
# Localize known C library symbols in a Rust static library (.a).
#
# Rust static libraries bundle compiler_builtins and libm, which export
# standard C symbols (cbrt, fma, sin, memcpy, __muloti4, etc.) as globals.
# When linked before our own C implementations (libllvmlibc, glibc-compat),
# these silently win — bypassing our -falign-functions=64, -march, and other
# optimization flags.
#
# This script localizes those known C library symbols so the linker picks
# our own implementations instead.  All other symbols (Rust-mangled, crate
# FFI, etc.) are left untouched.
#
# Usage: localize_rust_c_symbols.sh <library.a> <objcopy>

set -eu

LIB_PATH="$1"
OBJCOPY="$2"

if [ -z "$LIB_PATH" ] || [ -z "$OBJCOPY" ]; then
    echo "Usage: $0 <library.a> <objcopy>" >&2
    exit 1
fi

if [ ! -f "$LIB_PATH" ]; then
    echo "Error: Rust library not found: $LIB_PATH" >&2
    echo "If this is a workspace subcrate, set IMPORTED_LOCATION on the target" >&2
    echo "before calling clickhouse_config_crate_flags()." >&2
    exit 1
fi

# Known C library symbols that Rust's compiler_builtins / libm may export.
# This list covers:
#   - C math functions (libm)
#   - Compiler-rt builtins (__muloti4, __divti3, etc.)
#   - C string/memory functions that may be provided by Rust
KNOWN_C_SYMBOLS=(
    # Math functions (float64)
    cbrt cos sin tan acos asin atan atan2
    exp exp2 exp10 log log2 log10 log1p expm1
    pow sqrt hypot
    fma fmod remainder
    ceil floor round trunc nearbyint rint
    copysign fabs fdim fmax fmin
    erf erfc tgamma lgamma
    frexp ldexp modf scalbn ilogb logb
    nextafter nexttoward
    # Math functions (float32)
    cbrtf cosf sinf tanf acosf asinf atanf atan2f
    expf exp2f exp10f logf log2f log10f log1pf expm1f
    powf sqrtf hypotf
    fmaf fmodf remainderf
    ceilf floorf roundf truncf nearbyintf rintf
    copysignf fabsf fdimf fmaxf fminf
    erff erfcf tgammaf lgammaf
    frexpf ldexpf modff scalbnf ilogbf logbf
    nextafterf nexttowardf
    # Math functions (long double)
    ceill floorl roundl truncl
    sqrtl frexpl ldexpl scalbnl
    # Compiler-rt integer builtins
    __absvdi2 __absvsi2 __absvti2
    __addvdi3 __addvsi3 __addvti3
    __cmpdi2 __cmpti2
    __divdc3 __divsc3
    __muldc3 __mulsc3
    __mulvdi3 __mulvsi3 __mulvti3
    __negdf2 __negsf2 __negdi2 __negti2
    __negvdi2 __negvsi2 __negvti2
    __paritydi2 __paritysi2 __parityti2
    __popcountdi2 __popcountsi2 __popcountti2
    __muloti4
)

# Collect which of these actually exist as global symbols in the library
TMPFILE=$(mktemp)
WORK_DIR=""
cleanup() { rm -f "$TMPFILE"; [ -n "$WORK_DIR" ] && rm -rf "$WORK_DIR"; true; }
trap cleanup EXIT

if ! command -v llvm-nm >/dev/null 2>&1; then
    echo "Error: llvm-nm not found in PATH (required for Rust C-symbol localization)" >&2
    exit 1
fi

# Get all global defined symbols from the library. llvm-nm may print per-member errors for non-ELF archive
# members (e.g. debug metadata objects) - those are non-fatal as long as at least one ELF member is read.
# A truly broken/missing archive produces empty output and is caught below.
llvm-nm "$LIB_PATH" 2>/dev/null \
    | grep -E '^[0-9a-f]+ [TDBCWV] ' \
    | awk '{print $3}' \
    | sort -u \
    > "$TMPFILE" || true

if [ ! -s "$TMPFILE" ]; then
    echo "Error: llvm-nm produced no symbols for $LIB_PATH; cannot localize Rust C symbols" >&2
    exit 1
fi

# Build objcopy flags for symbols that exist in both lists
LOCALIZE_FLAGS=""
for sym in "${KNOWN_C_SYMBOLS[@]}"; do
    if grep -qxF "$sym" "$TMPFILE"; then
        LOCALIZE_FLAGS="$LOCALIZE_FLAGS --localize-symbol=$sym"
    fi
done

if [ -n "$LOCALIZE_FLAGS" ]; then
    # Try direct objcopy on the archive first (fast path).
    # Some Rust archives contain non-ELF members (e.g. debug metadata objects)
    # that cause llvm-objcopy to fail.  In that case, fall back to extracting
    # individual members, processing only valid ELF objects, and repacking.
    if ! $OBJCOPY $LOCALIZE_FLAGS "$LIB_PATH" 2>/dev/null; then
        # Substitute only inside the basename so a directory containing `objcopy`
        # is not rewritten and a versioned name like `llvm-objcopy-21` still maps
        # to `llvm-ar-21`.
        OBJCOPY_NAME="${OBJCOPY##*/}"
        AR_NAME="${OBJCOPY_NAME/objcopy/ar}"
        if [ "$OBJCOPY_NAME" = "$OBJCOPY" ]; then
            AR="$AR_NAME"
        else
            AR="${OBJCOPY%/*}/$AR_NAME"
        fi
        WORK_DIR=$(mktemp -d)

        (cd "$WORK_DIR" && "$AR" x "$LIB_PATH")
        for obj in "$WORK_DIR"/*.o; do
            $OBJCOPY $LOCALIZE_FLAGS "$obj" 2>/dev/null || true
        done
        "$AR" rcs "$LIB_PATH" "$WORK_DIR"/*.o
    fi
fi
