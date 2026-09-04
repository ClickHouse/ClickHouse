#!/usr/bin/env bash
#
# Process a Rust static library to fix COMDAT-related linker errors with
# --gc-sections.
#
# Two problems with the cxx bridge objects:
#
# 1. .eh_frame contains references to DW.ref.__gxx_personality_v0 (COMDAT).
#    --gc-sections discards the COMDAT group but keeps .eh_frame, breaking
#    relocations.
#
# 2. __clang_call_terminate is in a COMDAT group. When multiple .o files
#    each have their own copy, the linker discards all but one but fails
#    to redirect references from the discarded .o's non-COMDAT sections,
#    causing "relocation refers to a symbol in a discarded section" errors.
#
# The fix:
# 1. Remove .eh_frame from each .o (eliminates DW.ref.__gxx_personality_v0).
# 2. Rename __clang_call_terminate in each .o to a unique per-file name
#    (e.g. __clang_call_terminate_<hash>). This changes the COMDAT group
#    signature so the linker sees each as a distinct group and keeps all
#    copies, avoiding the "discarded section" problem.
#
# Since SKIM uses catch_unwind, panics never propagate through the FFI
# boundary, so .eh_frame and __clang_call_terminate are not needed at
# runtime; the renamed copies are harmless dead code.
#
# Usage: partial_link_rust.sh <library.a> <ar> <objcopy>

set -e

LIB_PATH=$(realpath "$1")
AR="$2"
OBJCOPY="$3"

if [ -z "$LIB_PATH" ] || [ -z "$AR" ] || [ -z "$OBJCOPY" ]; then
    echo "Usage: $0 <library.a> <ar> <objcopy>" >&2
    exit 1
fi

WORK_DIR=$(mktemp -d)
trap "rm -rf '$WORK_DIR'" EXIT

# Extract all object files
mkdir "$WORK_DIR/objs"
(cd "$WORK_DIR/objs" && "$AR" x "$LIB_PATH")

OBJ_COUNT=$(ls "$WORK_DIR"/objs/*.o | wc -l)
echo "Processing $OBJ_COUNT object files..."

# Step 1: Remove .eh_frame from each .o to eliminate
# DW.ref.__gxx_personality_v0 references that --gc-sections would break.
# Step 2: Rename __clang_call_terminate to a unique per-file name so
# each .o's COMDAT group has a distinct signature that the linker won't
# try to deduplicate.
for obj in "$WORK_DIR"/objs/*.o; do
    "$OBJCOPY" --remove-section .eh_frame "$obj" 2>/dev/null || true

    # Generate a unique suffix from the filename hash
    suffix=$(echo "$(basename "$obj")" | md5sum | head -c 8)
    "$OBJCOPY" --redefine-sym "__clang_call_terminate=__clang_call_terminate_${suffix}" "$obj" 2>/dev/null || true
done

# Repackage as .a (replace original)
rm -f "$LIB_PATH"
(cd "$WORK_DIR/objs" && "$AR" rcs "$LIB_PATH" *.o)
