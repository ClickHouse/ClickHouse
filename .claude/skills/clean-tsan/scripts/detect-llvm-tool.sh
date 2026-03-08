#!/bin/bash
# Print the path to an LLVM tool matching the compiler used for the TSan build.
#
# Usage: detect-llvm-tool.sh <tool-name> [--build-dir PATH]
#
# Reads CMAKE_CXX_COMPILER from the build directory's CMakeCache.txt and asks
# the compiler for the matching tool via --print-prog-name. This ensures the
# tool version matches the compiler (e.g. llvm-symbolizer-21 for clang++-21).
#
# Examples:
#   detect-llvm-tool.sh llvm-symbolizer          # → /usr/bin/llvm-symbolizer-21
#   detect-llvm-tool.sh lldb                      # → /usr/bin/lldb-21
#   detect-llvm-tool.sh lldb --build-dir build_debug
#
# Output: prints the absolute path to the tool on stdout.
# Exit code: 0 if found, 1 if not.

set -euo pipefail

usage() {
    echo "Usage: $0 <tool-name> [--build-dir PATH]" >&2
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

TOOL="$1"
shift

BUILD_DIR="build_tsan"

while [ $# -gt 0 ]; do
    case "$1" in
        --build-dir)
            BUILD_DIR="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            ;;
    esac
done

CACHE="$BUILD_DIR/CMakeCache.txt"
if [ ! -f "$CACHE" ]; then
    echo "ERROR: $CACHE not found" >&2
    exit 1
fi

CXX=$(grep '^CMAKE_CXX_COMPILER:' "$CACHE" | cut -d= -f2)
if [ -z "$CXX" ]; then
    echo "ERROR: CMAKE_CXX_COMPILER not found in $CACHE" >&2
    exit 1
fi

if ! command -v "$CXX" &>/dev/null; then
    echo "ERROR: Compiler not found: $CXX" >&2
    exit 1
fi

RESULT=$("$CXX" --print-prog-name="$TOOL" 2>/dev/null || true)
if [ -x "$RESULT" ]; then
    echo "$RESULT"
    exit 0
fi

echo "ERROR: $TOOL not found for compiler $CXX" >&2
exit 1
