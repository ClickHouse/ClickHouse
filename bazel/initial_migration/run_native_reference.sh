#!/usr/bin/env bash
# Prepare sources on the hosted coordinator; CMake and LLVM execute on ARM RBE.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
mode=cmake_reference_aarch64
phase=built
usage='Usage: bazel/initial_migration/run_native_reference.sh [--configure-only|--compare|--migration-evidence]'
if (( $# > 1 )); then echo "$usage" >&2; exit 2; fi
files=(status.json)
case "${1:-}" in
    '') ;;
    --configure-only) mode=cmake_configure_aarch64; phase=configured ;;
    --compare) mode=stripped_hash_aarch64; phase=compared ;;
    --migration-evidence)
        # Inputs to refresh_cmake_metadata.py --full-reference.
        files+=(
            src/Storages/System/StorageSystemBuildOptions.generated.cpp
            base/base/GitHash.generated.cpp
            src/Storages/System/StorageSystemLicenses.generated.cpp.gz
            contrib/clickstack-cmake/ClickStackResources.generated.h
            contrib/sql-console-cmake/SQLConsoleResources.generated.h
            llvm-config-headers.tar.gz
            resource-gzip-headers.json
        ) ;;
    *) echo "$usage" >&2; exit 2 ;;
esac
if [[ "$phase" == built ]]; then files+=(cmake-reference-summary.json); fi
downloads=(--remote_download_outputs=minimal)
for file in "${files[@]}"; do
    downloads+=("--remote_download_regex=^bazel-out/[^/]+/bin/bazel/$mode/${file//./[.]}$")
done
python3 -B bazel/initial_migration/prepare_native_reference.py
if [[ -n ${BUILDBUDDY_ARTIFACTS_DIRECTORY:-} ]]; then
    cp bazel/native_reference_inputs/source.json "$BUILDBUDDY_ARTIFACTS_DIRECTORY/source-inputs.json"
    cp bazel/native_reference_inputs/tools.json "$BUILDBUDDY_ARTIFACTS_DIRECTORY/arm-tools-inputs.json"
fi
bazel --host_jvm_args=-Xmx24g build --config=cmake_match_aarch64 \
    --noannounce_rc --color=no --curses=no --show_progress_rate_limit=30 "${downloads[@]}" "//bazel:$mode"
output="bazel-bin/bazel/$mode"
python3 -B - "$output" "$phase" <<'PY'
import json
from pathlib import Path
import sys
output = Path(sys.argv[1])
result = json.loads((output / "status.json").read_text())
if type(result.get("status")) is not int or result["status"] != 0 or result.get("phase") != sys.argv[2]:
    raise SystemExit("Unexpected native reference status: " + str(result))
print(json.dumps(result, indent=2), flush=True)
if result["phase"] == "built":
    print(json.dumps(json.loads((output / "cmake-reference-summary.json").read_text()), indent=2))
PY
if [[ -n ${BUILDBUDDY_ARTIFACTS_DIRECTORY:-} ]]; then
    for file in "${files[@]}"; do
        if [[ "$file" == */* ]]; then mkdir -p "$BUILDBUDDY_ARTIFACTS_DIRECTORY/${file%/*}"; fi
        cp -- "$output/$file" "$BUILDBUDDY_ARTIFACTS_DIRECTORY/$file"
    done
fi
