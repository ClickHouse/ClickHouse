#!/usr/bin/env bash
# Thin wrapper around utils/c++expr for use by Claude as an AI tool.
# Auto-detects build directory and sets up working directory, then delegates to c++expr.
#
# Usage:
#   cppexpr.sh [options] 'CODE'          — compile against ClickHouse (default)
#   cppexpr.sh --plain [options] 'CODE'  — compile standalone, no ClickHouse
#
# All options except --plain and -B are passed through to c++expr as-is.
# Run utils/c++expr --help for the full option list.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLICKHOUSE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CPPEXPR="$CLICKHOUSE_ROOT/utils/c++expr"

if [ ! -x "$CPPEXPR" ]; then
    echo "error: c++expr not found at $CPPEXPR" >&2
    exit 1
fi

# Extract --plain and -B flags; pass everything else through to c++expr
PLAIN_MODE=
BUILD_DIR=
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --plain) PLAIN_MODE=y; shift ;;
        -B)      BUILD_DIR="$2"; shift 2 ;;
        *)       ARGS+=("$1"); shift ;;
    esac
done

if [ -n "$PLAIN_MODE" ]; then
    exec "$CPPEXPR" "${ARGS[@]}"
fi

# Auto-detect build directory
if [ -z "$BUILD_DIR" ]; then
    for candidate in build build_debug build_release build_asan build_tsan build_msan build_ubsan; do
        if [ -f "$CLICKHOUSE_ROOT/$candidate/CMakeCache.txt" ]; then
            BUILD_DIR="$candidate"
            break
        fi
    done
    if [ -z "$BUILD_DIR" ]; then
        echo "error: no build directory found in $CLICKHOUSE_ROOT (run cmake first)" >&2
        exit 1
    fi
fi

cd "$CLICKHOUSE_ROOT/src"
exec "$CPPEXPR" -I -B "$BUILD_DIR" "${ARGS[@]}"
