#!/usr/bin/env bash
# Compare the remotely normalized ClickHouse with the native CMake reference.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
export SOURCE_DATE_EPOCH=1

arch=${CMAKE_REFERENCE_ARCH:-x86_64}
case "$arch" in
    x86_64) config=cmake_match ;;
    aarch64) config=cmake_match_aarch64 ;;
    *) echo 'CMAKE_REFERENCE_ARCH must be x86_64 or aarch64' >&2; exit 2 ;;
esac
case "$#:${1-}" in
    0:|1:--compare-originals) ;;
    *) echo "Usage: $0 [--compare-originals]" >&2; exit 2 ;;
esac
build=(bazel --host_jvm_args=-Xmx24g build --config="$config" --remote_download_outputs=minimal
       --noannounce_rc --color=no --curses=no --show_progress_rate_limit=30)

if [[ ${1-} == --compare-originals ]]; then
    if [[ $arch == aarch64 ]]; then
        if [[ -z ${CMAKE_REFERENCE_ELF:-} ]]; then
            echo 'Set CMAKE_REFERENCE_ELF to the recorded native ARM64 CMake ELF or its gzip archive.' >&2
            exit 2
        fi
        reference=$(mktemp -d "${TMPDIR:-/tmp}/clickhouse-arm-reference.XXXXXXXX")
        trap 'rm -rf -- "$reference"' EXIT
        python3 -B bazel/initial_migration/prepare_recorded_arm_reference.py "$CMAKE_REFERENCE_ELF" "$reference"
        "${build[@]}" --inject_repository="recorded_cmake_aarch64=$reference" --lockfile_mode=off \
            --remote_download_regex='^bazel-out/[^/]+/bin/external/[+]local_repository[+]recorded_cmake_aarch64/compare_recorded_aarch64/status[.]json$' \
            @recorded_cmake_aarch64//:compare_recorded_aarch64
        status=bazel-bin/external/+local_repository+recorded_cmake_aarch64/compare_recorded_aarch64/status.json
        python3 -B bazel/initial_migration/prepare_recorded_arm_reference.py --show-result "$status"
        if [[ -n ${BUILDBUDDY_ARTIFACTS_DIRECTORY:-} ]]; then
            cp -- "$status" "$BUILDBUDDY_ARTIFACTS_DIRECTORY/status.json"
        fi
        exit 0
    fi
    # recorded() reads the x86 ELF, toolchain manifest and selected Clang. The
    # staged clang symlink points to the LLVM multicall executable llvm.stripped.
    files='clickhouse|bazel/cmake_toolchain_files[.]json|external/llvm[+]/toolchain/stage1_linux_x86_64/bin/clang([+][+])?|external/llvm[+][+]llvm[+]llvm-project/llvm/llvm[.]stripped'
    "${build[@]}" --remote_download_regex="^bazel-out/[^/]+/bin/($files)$" \
        //:clickhouse //bazel:cmake_toolchain_files
    exec python3 -B bazel/initial_migration/verify_stripped_hash.py recorded
fi

bep=$(mktemp "${TMPDIR:-/tmp}/clickhouse-parity.XXXXXXXX")
trap 'rm -f -- "$bep"' EXIT
"${build[@]}" --build_event_json_file="$bep" //:clickhouse //bazel/initial_migration:clickhouse.normalized
python3 -B bazel/initial_migration/read_binary_metadata.py --arch "$arch" "$bep"
