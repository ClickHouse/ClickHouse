#!/bin/bash
set -xeu
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

# This script is separated into two stages, cloning and everything else, so
# that we can run the "everything else" stage from the cloned source.
stage=${stage:-}

# Compiler version, normally set by Dockerfile
export LLVM_VERSION=${LLVM_VERSION:-18}

# A variable to pass additional flags to CMake.
# Here we explicitly default it to nothing so that bash doesn't complain about
# it being undefined. Also read it as array so that we can pass an empty list
# of additional variable to cmake properly, and it doesn't generate an extra
# empty parameter.
# Read it as CMAKE_FLAGS to not lose exported FASTTEST_CMAKE_FLAGS on subsequent launch
read -ra CMAKE_FLAGS <<< "${FASTTEST_CMAKE_FLAGS:-}"

# Run only matching tests.
FASTTEST_FOCUS=${FASTTEST_FOCUS:-""}

FASTTEST_WORKSPACE=$(readlink -f "${FASTTEST_WORKSPACE:-.}")
FASTTEST_SOURCE=$(readlink -f "${FASTTEST_SOURCE:-$FASTTEST_WORKSPACE/ch}")
FASTTEST_BUILD=$(readlink -f "${FASTTEST_BUILD:-${BUILD:-$FASTTEST_WORKSPACE/build}}")
FASTTEST_DATA=$(readlink -f "${FASTTEST_DATA:-$FASTTEST_WORKSPACE/db-fasttest}")
FASTTEST_OUTPUT=$(readlink -f "${FASTTEST_OUTPUT:-$FASTTEST_WORKSPACE}")
PATH="$FASTTEST_BUILD/programs:$FASTTEST_SOURCE/tests:$PATH"
# Work around for non-existent user
if [ "$HOME" == "/" ]; then
    HOME="$FASTTEST_WORKSPACE/user-home"
    mkdir -p "$HOME"
    export HOME
fi

# Export these variables, so that all subsequent invocations of the script
# use them, and not try to guess them anew, which leads to weird effects.
export FASTTEST_WORKSPACE
export FASTTEST_SOURCE
export FASTTEST_BUILD
export FASTTEST_DATA
export FASTTEST_OUTPUT
export PATH

function ccache_status
{
    ccache --show-config ||:
    ccache --show-stats ||:
    SCCACHE_NO_DAEMON=1 sccache --show-stats ||:
}

function start_server
{
    set -m # Spawn server in its own process groups

    local opts=(
        --config-file "$FASTTEST_DATA/config.xml"
        --pid-file "$FASTTEST_DATA/clickhouse-server.pid"
        --
        --path "$FASTTEST_DATA"
        --user_files_path "$FASTTEST_DATA/user_files"
        --top_level_domains_path "$FASTTEST_DATA/top_level_domains"
        --keeper_server.storage_path "$FASTTEST_DATA/coordination"
    )
    clickhouse-server "${opts[@]}" &>> "$FASTTEST_OUTPUT/server.log" &
    set +m

    for _ in {1..60}; do
        if clickhouse-client --query "select 1"; then
            break
        fi
        sleep 1
    done

    if ! clickhouse-client --query "select 1"; then
        echo "Failed to wait until ClickHouse server starts."
        return 1
    fi

    local server_pid
    server_pid="$(cat "$FASTTEST_DATA/clickhouse-server.pid")"
    echo "ClickHouse server pid '$server_pid' started and responded"
}

export -f start_server

function clone_root
{
    [ "$UID" -eq 0 ] && git config --global --add safe.directory "$FASTTEST_SOURCE"
    git clone --depth 1 https://github.com/ClickHouse/ClickHouse.git -- "$FASTTEST_SOURCE" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/clone_log.txt"

    (
        cd "$FASTTEST_SOURCE"
        if [ "$PULL_REQUEST_NUMBER" != "0" ]; then
            if git fetch --depth 1 origin "+refs/pull/$PULL_REQUEST_NUMBER/merge"; then
                git checkout FETCH_HEAD
                echo "Checked out pull/$PULL_REQUEST_NUMBER/merge ($(git rev-parse FETCH_HEAD))"
            else
                git fetch --depth 1 origin "+refs/pull/$PULL_REQUEST_NUMBER/head"
                git checkout "$COMMIT_SHA"
                echo "Checked out nominal SHA $COMMIT_SHA for PR $PULL_REQUEST_NUMBER"
            fi
        else
            if [ -v COMMIT_SHA ]; then
                git fetch --depth 1 origin "$COMMIT_SHA"
                git checkout "$COMMIT_SHA"
                echo "Checked out nominal SHA $COMMIT_SHA for master"
            else
                echo  "Using default repository head $(git rev-parse HEAD)"
            fi
        fi
    )
}

function clone_submodules
{
    (
        cd "$FASTTEST_SOURCE"

        SUBMODULES_TO_UPDATE=(
            contrib/sysroot
            contrib/magic_enum
            contrib/abseil-cpp
            contrib/boost
            contrib/zlib-ng
            contrib/libxml2
            contrib/libunwind
            contrib/fmtlib
            contrib/aklomp-base64
            contrib/cctz
            contrib/libcpuid
            contrib/libdivide
            contrib/double-conversion
            contrib/llvm-project
            contrib/lz4
            contrib/zstd
            contrib/fastops
            contrib/rapidjson
            contrib/re2
            contrib/sparsehash-c11
            contrib/croaring
            contrib/miniselect
            contrib/xz
            contrib/dragonbox
            contrib/fast_float
            contrib/NuRaft
            contrib/jemalloc
            contrib/replxx
            contrib/wyhash
            contrib/c-ares
            contrib/morton-nd
            contrib/xxHash
            contrib/expected
            contrib/simdjson
            contrib/liburing
            contrib/libfiu
            contrib/incbin
            contrib/yaml-cpp
        )

        git submodule sync
        git submodule init

        # Network is unreliable
        for _ in {1..10}
        do
            # --jobs does not work as fast as real parallel running
            printf '%s\0' "${SUBMODULES_TO_UPDATE[@]}" | \
                xargs --max-procs=100 --null --no-run-if-empty --max-args=1 \
                  git submodule update --depth 1 --single-branch && break
            sleep 1
        done

        git submodule foreach git reset --hard
        git submodule foreach git checkout @ -f
        git submodule foreach git clean -xfd
    )
}

function run_cmake
{
    CMAKE_LIBS_CONFIG=(
        "-DENABLE_LIBRARIES=0"
        "-DENABLE_TESTS=0"
        "-DENABLE_UTILS=0"
        "-DENABLE_THINLTO=0"
        "-DENABLE_NURAFT=1"
        "-DENABLE_SIMDJSON=1"
        "-DENABLE_JEMALLOC=1"
        "-DENABLE_LIBURING=1"
        "-DENABLE_YAML_CPP=1"
    )

    export CCACHE_DIR="$FASTTEST_WORKSPACE/ccache"
    export CCACHE_COMPRESSLEVEL=5
    export CCACHE_BASEDIR="$FASTTEST_SOURCE"
    export CCACHE_NOHASHDIR=true
    export CCACHE_COMPILERCHECK=content
    export CCACHE_MAXSIZE=15G

    ccache_status
    ccache --zero-stats ||:

    mkdir "$FASTTEST_BUILD" ||:

    (
        cd "$FASTTEST_BUILD"
        cmake "$FASTTEST_SOURCE" -DCMAKE_CXX_COMPILER="clang++-${LLVM_VERSION}" -DCMAKE_C_COMPILER="clang-${LLVM_VERSION}" -DCMAKE_TOOLCHAIN_FILE="${FASTTEST_SOURCE}/cmake/linux/toolchain-x86_64-musl.cmake" "${CMAKE_LIBS_CONFIG[@]}" "${CMAKE_FLAGS[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/cmake_log.txt"
    )
}

function build
{
    (
        cd "$FASTTEST_BUILD"
        TIMEFORMAT=$'\nreal\t%3R\nuser\t%3U\nsys\t%3S'
        ( time ninja clickhouse-bundle clickhouse-stripped) |& ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/build_log.txt"
        BUILD_SECONDS_ELAPSED=$(awk '/^....-..-.. ..:..:.. real\t[0-9]/ {print $4}' < "$FASTTEST_OUTPUT/build_log.txt")
        echo "build_clickhouse_fasttest_binary: [ OK ] $BUILD_SECONDS_ELAPSED sec." \
          | ts '%Y-%m-%d %H:%M:%S' \
          | tee "$FASTTEST_OUTPUT/test_result.txt"

        (
            # This query should fail, and print stacktrace with proper symbol names (even on a stripped binary)
            clickhouse_output=$(programs/clickhouse-stripped --stacktrace -q 'select' 2>&1 || :)
            if [[ $clickhouse_output =~ DB::LocalServer::main ]]; then
                echo "stripped_clickhouse_shows_symbols_names: [ OK ] 0 sec."
            else
                echo -e "stripped_clickhouse_shows_symbols_names: [ FAIL ] 0 sec. - clickhouse output:\n\n$clickhouse_output\n"
            fi
        ) | ts '%Y-%m-%d %H:%M:%S' | tee -a "$FASTTEST_OUTPUT/test_result.txt"

        if [ "$COPY_CLICKHOUSE_BINARY_TO_OUTPUT" -eq "1" ]; then
            mkdir -p "$FASTTEST_OUTPUT/binaries/"
            cp programs/clickhouse "$FASTTEST_OUTPUT/binaries/clickhouse"

            zstd --threads=0 programs/clickhouse-stripped -o "$FASTTEST_OUTPUT/binaries/clickhouse-stripped.zst"
        fi
        ccache_status
        ccache --evict-older-than 1d ||:
    )
}

function configure
{
    clickhouse-client --version
    clickhouse-test --help

    mkdir -p "$FASTTEST_DATA"{,/client-config}
    cp -a "$FASTTEST_SOURCE/programs/server/"{config,users}.xml "$FASTTEST_DATA"
    "$FASTTEST_SOURCE/tests/config/install.sh" "$FASTTEST_DATA" "$FASTTEST_DATA/client-config"
    cp -a "$FASTTEST_SOURCE/programs/server/config.d/log_to_console.xml" "$FASTTEST_DATA/config.d"
    # doesn't support SSL
    rm -f "$FASTTEST_DATA/config.d/secure_ports.xml"
}

function run_tests
{
    clickhouse-server --version
    clickhouse-test --help

    start_server

    set +e
    local NPROC
    NPROC=$(nproc)
    NPROC=$((NPROC / 2))
    if [[ $NPROC == 0 ]]; then
      NPROC=1
    fi

    export CLICKHOUSE_CONFIG_DIR=$FASTTEST_DATA
    export CLICKHOUSE_CONFIG="$FASTTEST_DATA/config.xml"
    export CLICKHOUSE_USER_FILES="$FASTTEST_DATA/user_files"
    export CLICKHOUSE_SCHEMA_FILES="$FASTTEST_DATA/format_schemas"

    local test_opts=(
        --hung-check
        --fast-tests-only
        --no-random-settings
        --no-random-merge-tree-settings
        --no-long
        --testname
        --shard
        --zookeeper
        --check-zookeeper-session
        --order random
        --print-time
        --report-logs-stats
        --jobs "${NPROC}"
        --timeout 30 # We don't want slow test being introduced again in this check
    )
    time clickhouse-test "${test_opts[@]}" -- "$FASTTEST_FOCUS" 2>&1 \
        | ts '%Y-%m-%d %H:%M:%S' \
        | tee -a "$FASTTEST_OUTPUT/test_result.txt"
    set -e

    clickhouse stop --pid-path "$FASTTEST_DATA"
}

export -f run_tests

case "$stage" in
"")
    ls -la
    ;&
"clone_root")
    clone_root
    ;&
"clone_submodules")
    clone_submodules 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/submodule_log.txt"
    ;&
"run_cmake")
    cd "$FASTTEST_WORKSPACE"
    run_cmake
    ;&
"build")
    build
    ;&
"configure")
    # The `install_log.txt` is also needed for compatibility with old CI task --
    # if there is no log, it will decide that build failed.
    configure 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/install_log.txt"
    ;&
"run_tests")
    run_tests ||:
    /repo/tests/docker_scripts/process_functional_tests_result.py --in-results-dir "$FASTTEST_OUTPUT/" \
        --out-results-file "$FASTTEST_OUTPUT/test_results.tsv" \
        --out-status-file "$FASTTEST_OUTPUT/check_status.tsv" || echo -e "failure\tCannot parse results" > "$FASTTEST_OUTPUT/check_status.tsv"
    ;;
*)
    echo "Unknown test stage '$stage'"
    exit 1
esac

pstree -apgT
jobs
