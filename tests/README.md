# [WIP] Running CI tests & checks locally

## Style Check

#### in docker
```sh
# use docker to run all checks at once and avoid package installation:
mkdir /tmp/test_output
docker run --volume=.:/ClickHouse --volume=/tmp/test_output:/test_output -u $(id -u ${USER}):$(id -g ${USER}) --cap-add=SYS_PTRACE clickhouse/style-test

# run certain check (e.g.: check-duplicate-includes.sh)
docker run --volume=.:/ClickHouse --volume=/tmp/test_output:/test_output -u $(id -u ${USER}):$(id -g ${USER}) --cap-add=SYS_PTRACE --entrypoint= -w/ClickHouse/utils/check-style clickhouse/style-test ./check-duplicate-includes.sh
```

#### on a host
```sh
# refer to ./docker/test/style/Dockerfile whenever needed, to check required packages and installation commands

cd ./utils/check-style

# Check duplicate includes:
./check-duplicate-includes.sh

# Check style
./check-style

# Check python formatting with black
./check-black

# Check python type hinting with mypy
./check-mypy

# Check typos
./check-typos

# Check docs spelling
./check-doc-aspell

# Check whitespaces
./check-whitespaces

# Check workflows
./check-workflows

# Check submodules
./check-submodules

# Check shell scripts with shellcheck
./shellcheck-run.sh
```

## Fast Tests:

FastTest CI job builds minimal CH version without extra libraries and runs limited number of Functional tests to check for a smoke

#### Building minimal CH
```sh
# following git commands suggests updating only necesarry submodule for minimal CH build
# you can use your normal CH build for Fast Tests, though these commands give the idea of how CH is being built for Fast Tests in CI
git clone --depth 1 https://github.com/ClickHouse/ClickHouse.git
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
    contrib/simdjson
    contrib/liburing
    contrib/libfiu
    contrib/incbin
    contrib/yaml-cpp
)
git submodule sync
git submodule init
# --jobs does not work as fast as real parallel running
printf '%s\0' "${SUBMODULES_TO_UPDATE[@]}" | \
    xargs --max-procs=100 --null --no-run-if-empty --max-args=1 \
        git submodule update --depth 1 --single-branch

# configuring build
LLVM_VERSION=${LLVM_VERSION:-17}

CMAKE_LIBS_CONFIG=(
    "-DENABLE_LIBRARIES=0"
    "-DENABLE_TESTS=0"
    "-DENABLE_UTILS=0"
    "-DENABLE_EMBEDDED_COMPILER=0"
    "-DENABLE_THINLTO=0"
    "-DENABLE_NURAFT=1"
    "-DENABLE_SIMDJSON=1"
    "-DENABLE_JEMALLOC=1"
    "-DENABLE_LIBURING=1"
    "-DENABLE_YAML_CPP=1"
)
mkdir build
cd build
cmake  -DCMAKE_CXX_COMPILER="clang++-${LLVM_VERSION}" -DCMAKE_C_COMPILER="clang-${LLVM_VERSION}" "${CMAKE_LIBS_CONFIG[@]}" ..
ninja clickhouse-bundle
cd ..
```

#### Running Fast (Smoke, Functional) Tests
```sh
# prepare test CH configuration - adjust file locations if necessary
mkdir -p /tmp/etc/server
mkdir -p /tmp/etc/client
cp programs/server/{config,users}.xml /tmp/etc/server/
./tests/config/install.sh /tmp/etc/server/ /tmp/etc/client/
cp programs/server/config.d/log_to_console.xml /tmp/etc/server/config.d/
rm /tmp/etc/server/config.d/secure_ports.xml
sudo mkdir /var/lib/clickhouse
sudo chmod 777 /var/lib/clickhouse

# prepare server command options
opts=(
    --config-file /tmp/etc/server/config.xml
    --pid-file /tmp/etc/server/clickhouse-server.pid
    --
    --path /tmp/etc/server
    --user_files_path /tmp/etc/server/user_files
    --top_level_domains_path /tmp/etc/server/top_level_domains
    --keeper_server.storage_path /tmp/etc/server/coordination
)
# start server
./build/programs/clickhouse-server "${opts[@]}"

# start a new terminal for tests
# prepare test runner command options
test_opts=(
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
)
# start test runner
cd ./tests;
time ./clickhouse-test -c ../build/programs "${test_opts[@]}"

```


## Functional Tests

Tests are comprised in Test Suites (TS) and located in `./tests/queries/<TS num>_<TS name>/*`.

Each Test Case (TC) represented by two obligatory files:
 * test script or queries: `./<TC num>_<TC name>.<sh|sql|python|expect|j2>`
 * test expected values: `./<TC num>_<TC name>.reference`

To run tests use a test runner:
```sh
cd ./tests
./clickhouse-test [--help] [test ...]
# e.g.:
# Running all tests:
#  ./clickhouse-test
# Running tests that match given pattern
#  ./clickhouse-test hash_table
# See all runner options in help menu
```

## Static analysis with clang-tidy build

#### in docker
```sh
mkdir build_tidy

# configure cmake
docker run --rm --volume=.:/build -u $(id -u ${USER}):$(id -g ${USER}) --entrypoint=  -w /build/build-tidy clickhouse/binary-builder:latest cmake -DCMAKE_C_COMPILER=clang-17 -DCMAKE_CXX_COMPILER=clang++-17 -LA -DCMAKE_BUILD_TYPE=Debug -DSANITIZE= -DENABLE_CHECK_HEAVY_BUILDS=1 -DCOMPILER_CACHE=ccache -DENABLE_CLANG_TIDY=1 -DENABLE_TESTS=1 -DENABLE_EXAMPLES=1 -DENABLE_UTILS=1 -DENABLE_BUILD_PROFILING=1 -DCLICKHOUSE_OFFICIAL_BUILD=1 ..

# build
docker run --rm --volume=.:/build -u $(id -u ${USER}):$(id -g ${USER}) --entrypoint=  -w /build/build-tidy clickhouse/binary-builder:latest ninja -k0 all
```

#### on a host
```sh
# install dependencies if not yet
# sudo apt-get install clang-tidy-17

mkdir build_tidy && cd build_tidy
cmake --debug-trycompile -DCMAKE_CXX_COMPILER="clang++-17" -DCMAKE_C_COMPILER="clang-17" -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DSANITIZE= -DENABLE_CHECK_HEAVY_BUILDS=1 -DCMAKE_C_COMPILER=clang-17 -DCMAKE_CXX_COMPILER=clang++-17 -DCOMPILER_CACHE=ccache -DENABLE_CLANG_TIDY=1 -DENABLE_TESTS=1 -DENABLE_EXAMPLES=1 -DENABLE_UTILS=1 -DENABLE_BUILD_PROFILING=1 -DCLICKHOUSE_OFFICIAL_BUILD=1 ..
ninja all
```
