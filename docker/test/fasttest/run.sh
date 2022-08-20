#!/bin/bash
set -xeu
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

# This script is separated into two stages, cloning and everything else, so
# that we can run the "everything else" stage from the cloned source.
stage=${stage:-}

# Compiler version, normally set by Dockerfile
export LLVM_VERSION=${LLVM_VERSION:-13}

# A variable to pass additional flags to CMake.
# Here we explicitly default it to nothing so that bash doesn't complain about
# it being undefined. Also read it as array so that we can pass an empty list
# of additional variable to cmake properly, and it doesn't generate an extra
# empty parameter.
read -ra FASTTEST_CMAKE_FLAGS <<< "${FASTTEST_CMAKE_FLAGS:-}"

# Run only matching tests.
FASTTEST_FOCUS=${FASTTEST_FOCUS:-""}

FASTTEST_WORKSPACE=$(readlink -f "${FASTTEST_WORKSPACE:-.}")
FASTTEST_SOURCE=$(readlink -f "${FASTTEST_SOURCE:-$FASTTEST_WORKSPACE/ch}")
FASTTEST_BUILD=$(readlink -f "${FASTTEST_BUILD:-${BUILD:-$FASTTEST_WORKSPACE/build}}")
FASTTEST_DATA=$(readlink -f "${FASTTEST_DATA:-$FASTTEST_WORKSPACE/db-fasttest}")
FASTTEST_OUTPUT=$(readlink -f "${FASTTEST_OUTPUT:-$FASTTEST_WORKSPACE}")
PATH="$FASTTEST_BUILD/programs:$FASTTEST_SOURCE/tests:$PATH"

# Export these variables, so that all subsequent invocations of the script
# use them, and not try to guess them anew, which leads to weird effects.
export FASTTEST_WORKSPACE
export FASTTEST_SOURCE
export FASTTEST_BUILD
export FASTTEST_DATA
export FASTTEST_OUT
export PATH

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

function clone_root
{
    git config --global --add safe.directory "$FASTTEST_SOURCE"
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
            contrib/poco
            contrib/libunwind
            contrib/fmtlib
            contrib/base64
            contrib/cctz
            contrib/libcpuid
            contrib/double-conversion
            contrib/libcxx
            contrib/libcxxabi
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
            contrib/hashidsxx
            contrib/c-ares
        )

        git submodule sync
        git submodule update --jobs=16 --depth 1 --init "${SUBMODULES_TO_UPDATE[@]}"
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
        "-DENABLE_EMBEDDED_COMPILER=0"
        "-DENABLE_THINLTO=0"
        "-DUSE_UNWIND=1"
        "-DENABLE_NURAFT=1"
        "-DENABLE_JEMALLOC=1"
        "-DENABLE_REPLXX=1"
    )

    export CCACHE_DIR="$FASTTEST_WORKSPACE/ccache"
    export CCACHE_COMPRESSLEVEL=5
    export CCACHE_BASEDIR="$FASTTEST_SOURCE"
    export CCACHE_NOHASHDIR=true
    export CCACHE_COMPILERCHECK=content
    export CCACHE_MAXSIZE=15G

    ccache --show-stats ||:
    ccache --zero-stats ||:

    mkdir "$FASTTEST_BUILD" ||:

    (
        cd "$FASTTEST_BUILD"
        cmake "$FASTTEST_SOURCE" -DCMAKE_CXX_COMPILER="clang++-${LLVM_VERSION}" -DCMAKE_C_COMPILER="clang-${LLVM_VERSION}" "${CMAKE_LIBS_CONFIG[@]}" "${FASTTEST_CMAKE_FLAGS[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/cmake_log.txt"
    )
}

function build
{
    (
        cd "$FASTTEST_BUILD"
        time ninja clickhouse-bundle 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/build_log.txt"
        if [ "$COPY_CLICKHOUSE_BINARY_TO_OUTPUT" -eq "1" ]; then
            cp programs/clickhouse "$FASTTEST_OUTPUT/clickhouse"

            strip programs/clickhouse -o "$FASTTEST_OUTPUT/clickhouse-stripped"
            gzip "$FASTTEST_OUTPUT/clickhouse-stripped"
        fi
        ccache --show-stats ||:
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

    local test_opts=(
        --hung-check
        --fast-tests-only
        --no-random-settings
        --no-long
        --testname
        --shard
        --zookeeper
        --check-zookeeper-session
        --order random
        --print-time
        --jobs "${NPROC}"
    )
    time clickhouse-test "${test_opts[@]}" -- "$FASTTEST_FOCUS" 2>&1 \
        | ts '%Y-%m-%d %H:%M:%S' \
        | tee "$FASTTEST_OUTPUT/test_result.txt"
    set -e

    clickhouse stop --pid-path "$FASTTEST_DATA"
}

case "$stage" in
"")
    ls -la
    ;&
"clone_root")
    clone_root

    # Pass control to the script from cloned sources, unless asked otherwise.
    if ! [ -v FASTTEST_LOCAL_SCRIPT ]
    then
        # 'run' stage is deprecated, used for compatibility with old scripts.
        # Replace with 'clone_submodules' after Nov 1, 2020.
        # cd and CLICKHOUSE_DIR are also a setup for old scripts, remove as well.
        # In modern script we undo it by changing back into workspace dir right
        # away, see below. Remove that as well.
        cd "$FASTTEST_SOURCE"
        CLICKHOUSE_DIR=$(pwd)
        export CLICKHOUSE_DIR
        stage=run "$FASTTEST_SOURCE/docker/test/fasttest/run.sh"
        exit $?
    fi
    ;&
"run")
    # A deprecated stage that is called by old script and equivalent to everything
    # after cloning root, starting with cloning submodules.
    ;&
"clone_submodules")
    # Recover after being called from the old script that changes into source directory.
    # See the compatibility hacks in `clone_root` stage above. Remove at the same time,
    # after Nov 1, 2020.
    cd "$FASTTEST_WORKSPACE"
    clone_submodules 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee "$FASTTEST_OUTPUT/submodule_log.txt"
    ;&
"run_cmake")
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
    run_tests
    /process_functional_tests_result.py --in-results-dir "$FASTTEST_OUTPUT/" \
        --out-results-file "$FASTTEST_OUTPUT/test_results.tsv" \
        --out-status-file "$FASTTEST_OUTPUT/check_status.tsv" || echo -e "failure\tCannot parse results" > "$FASTTEST_OUTPUT/check_status.tsv"
    ;;
*)
    echo "Unknown test stage '$stage'"
    exit 1
esac

pstree -apgT
jobs
