#!/usr/bin/env bash
# Build with RBE, then strip both recorded originals for comparison.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
export SOURCE_DATE_EPOCH=1
if [[ "${CMAKE_REFERENCE_ARCH:-x86_64}" == aarch64 ]]; then
    exec bash bazel/run_native_reference.sh --compare
fi
bazel --host_jvm_args=-Xmx24g build \
    --config=cmake_match \
    --noannounce_rc --color=no --curses=no \
    --show_progress_rate_limit=30 //:clickhouse //bazel:cmake_toolchain_files
python3 -B bazel/verify_stripped_hash.py recorded
