#!/usr/bin/env bash
# Regenerates `include/vortex_ffi.h` from `src/lib.rs`.
#
# The two halves of this FFI are separate translation units linked by symbol name, so a signature
# changed on one side only is silent undefined behaviour rather than a compile error. Generating
# the header removes that failure mode: the Rust source, including its doc comments, is the only
# place either half is written.
#
# Run this after changing anything the header exposes, and commit the result. CI regenerates the
# header and fails if the committed one differs.
#
# Needs cbindgen at the pinned version:
#
#     cargo install cbindgen --version 0.29.0 --locked
#
# The output is left exactly as cbindgen formats it - deliberately not passed through
# `clang-format`, so that the result depends on the pinned cbindgen alone and CI cannot fail over a
# formatter version. It is a generated file; it does not follow the repository's C++ style.

set -euo pipefail

CBINDGEN_VERSION="0.29.0"

CRATE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT="$CRATE_DIR/include/vortex_ffi.h"

if ! command -v cbindgen > /dev/null; then
    echo "error: cbindgen is not on PATH." >&2
    echo "       cargo install cbindgen --version $CBINDGEN_VERSION --locked" >&2
    exit 1
fi

have="$(cbindgen --version | awk '{print $2}')"
if [ "$have" != "$CBINDGEN_VERSION" ]; then
    echo "error: cbindgen $have is on PATH, but the header is generated with $CBINDGEN_VERSION." >&2
    echo "       Different versions format differently, which would fail the CI check." >&2
    echo "       cargo install cbindgen --version $CBINDGEN_VERSION --locked --force" >&2
    exit 1
fi

cbindgen --config "$CRATE_DIR/cbindgen.toml" \
         --crate _ch_rust_vortex \
         --output "$OUTPUT" \
         --quiet

echo "wrote $OUTPUT"
