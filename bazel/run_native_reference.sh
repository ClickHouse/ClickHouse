#!/usr/bin/env bash
# Prepare sources on the hosted coordinator; CMake and LLVM execute on ARM RBE.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
mode=cmake_reference_aarch64
case "${1:-}" in
    '') ;;
    --configure-only) mode=cmake_configure_aarch64 ;;
    --compare) mode=stripped_hash_aarch64 ;;
    *) echo 'Usage: bazel/run_native_reference.sh [--configure-only|--compare]' >&2; exit 2 ;;
esac
python3 -B bazel/prepare_native_reference.py
if [[ -n ${BUILDBUDDY_ARTIFACTS_DIRECTORY:-} ]]; then
    cp bazel/native_reference_inputs/source.json "$BUILDBUDDY_ARTIFACTS_DIRECTORY/source-inputs.json"
    cp bazel/native_reference_inputs/tools.json "$BUILDBUDDY_ARTIFACTS_DIRECTORY/arm-tools-inputs.json"
fi
for attempt in 1 2 3; do
    if bazel --host_jvm_args=-Xmx24g build --config=cmake_match_aarch64 \
        --noannounce_rc --color=no --curses=no --show_progress_rate_limit=30 "//bazel:$mode"; then
        break
    else
        status=$?
        if [[ "$status" != 34 || "$attempt" == 3 ]]; then
            exit "$status"
        fi
        echo "CMAKE_REFERENCE_REMOTE_RETRY=$attempt (Bazel remote execution error 34)" >&2
        sleep 10
    fi
done
output="bazel-bin/bazel/$mode"
if [[ -n ${BUILDBUDDY_ARTIFACTS_DIRECTORY:-} ]]; then
    cp -RL --no-preserve=mode,ownership "$output/." "$BUILDBUDDY_ARTIFACTS_DIRECTORY/"
fi
python3 -B - "$output/status.json" <<'PY'
import json
from pathlib import Path
import sys
result = json.loads(Path(sys.argv[1]).read_text())
print(json.dumps(result, indent=2), flush=True)
if result['status'] != 0:
    raise SystemExit(result['status'])
PY
