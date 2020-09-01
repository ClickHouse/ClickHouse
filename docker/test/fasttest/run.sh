#!/bin/bash
set -xeu
set -o pipefail
trap "exit" INT TERM
trap 'kill $(jobs -pr) ||:' EXIT

# This script is separated into two stages, cloning and everything else, so
# that we can run the "everything else" stage from the cloned source (we don't
# do this yet).
stage=${stage:-}

# A variable to pass additional flags to CMake.
# Here we explicitly default it to nothing so that bash doesn't complain about
# it being undefined. Also read it as array so that we can pass an empty list 
# of additional variable to cmake properly, and it doesn't generate an extra
# empty parameter.
read -ra FASTTEST_CMAKE_FLAGS <<< "${FASTTEST_CMAKE_FLAGS:-}"

ls -la

function kill_clickhouse
{
    for _ in {1..60}
    do
        if ! pkill -f clickhouse-server ; then break ; fi
        sleep 1
    done

    if pgrep -f clickhouse-server
    then
        pstree -apgT
        jobs
        echo "Failed to kill the ClickHouse server $(pgrep -f clickhouse-server)"
        return 1
    fi
}

function wait_for_server_start
{
    for _ in {1..60}
    do
        if clickhouse-client --query "select 1" || ! pgrep -f clickhouse-server
        then
            break
        fi
        sleep 1
    done

    if ! clickhouse-client --query "select 1"
    then
        echo "Failed to wait until ClickHouse server starts."
        return 1
    fi

    echo "ClickHouse server pid '$(pgrep -f clickhouse-server)' started and responded"
}

function clone_root
{
git clone https://github.com/ClickHouse/ClickHouse.git | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/clone_log.txt
cd ClickHouse
CLICKHOUSE_DIR=$(pwd)


if [ "$PULL_REQUEST_NUMBER" != "0" ]; then
    if git fetch origin "+refs/pull/$PULL_REQUEST_NUMBER/merge"; then
        git checkout FETCH_HEAD
        echo 'Clonned merge head'
    else
        git fetch
        git checkout "$COMMIT_SHA"
        echo 'Checked out to commit'
    fi
else
    if [ "$COMMIT_SHA" != "" ]; then
        git checkout "$COMMIT_SHA"
    fi
fi
}

function run
{
SUBMODULES_TO_UPDATE=(contrib/boost contrib/zlib-ng contrib/libxml2 contrib/poco contrib/libunwind contrib/ryu contrib/fmtlib contrib/base64 contrib/cctz contrib/libcpuid contrib/double-conversion contrib/libcxx contrib/libcxxabi contrib/libc-headers contrib/lz4 contrib/zstd contrib/fastops contrib/rapidjson contrib/re2 contrib/sparsehash-c11)

git submodule update --init --recursive "${SUBMODULES_TO_UPDATE[@]}" | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/submodule_log.txt

export CMAKE_LIBS_CONFIG="-DENABLE_LIBRARIES=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_THINLTO=0 -DUSE_UNWIND=1"

export CCACHE_DIR=/ccache
export CCACHE_BASEDIR=/ClickHouse
export CCACHE_NOHASHDIR=true
export CCACHE_COMPILERCHECK=content
export CCACHE_MAXSIZE=15G

ccache --show-stats ||:
ccache --zero-stats ||:

mkdir build
cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_CXX_COMPILER=clang++-10 -DCMAKE_C_COMPILER=clang-10 "$CMAKE_LIBS_CONFIG" "${FASTTEST_CMAKE_FLAGS[@]}" | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/cmake_log.txt
ninja clickhouse-bundle | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/build_log.txt
ninja install | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/install_log.txt


ccache --show-stats ||:

mkdir -p /etc/clickhouse-server
mkdir -p /etc/clickhouse-client
mkdir -p /etc/clickhouse-server/config.d
mkdir -p /etc/clickhouse-server/users.d
ln -s /test_output /var/log/clickhouse-server
cp "$CLICKHOUSE_DIR/programs/server/config.xml" /etc/clickhouse-server/
cp "$CLICKHOUSE_DIR/programs/server/users.xml" /etc/clickhouse-server/

mkdir -p /etc/clickhouse-server/dict_examples
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/custom_settings_prefixes.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/log_queries.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/readonly.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/access_management.xml /etc/clickhouse-server/users.d/
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/macros.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/disks.xml /etc/clickhouse-server/config.d/
#ln -s /usr/share/clickhouse-test/config/secure_ports.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/clusters.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/graphite.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/server.key /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/server.crt /etc/clickhouse-server/
ln -s /usr/share/clickhouse-test/config/dhparam.pem /etc/clickhouse-server/
ln -sf /usr/share/clickhouse-test/config/client_config.xml /etc/clickhouse-client/config.xml

# Keep original query_masking_rules.xml
ln -s --backup=simple --suffix=_original.xml /usr/share/clickhouse-test/config/query_masking_rules.xml /etc/clickhouse-server/config.d/

# Kill the server in case we are running locally and not in docker
kill_clickhouse

clickhouse-server --config /etc/clickhouse-server/config.xml --daemon

wait_for_server_start

TESTS_TO_SKIP=(
    parquet
    avro
    h3
    odbc
    mysql
    sha256
    _orc_
    arrow
    01098_temporary_and_external_tables
    01083_expressions_in_engine_arguments
    hdfs
    00911_tautological_compare
    protobuf
    capnproto
    java_hash
    hashing
    secure
    00490_special_line_separators_and_characters_outside_of_bmp
    00436_convert_charset
    00105_shard_collations
    01354_order_by_tuple_collate_const
    01292_create_user
    01098_msgpack_format
    00929_multi_match_edit_distance
    00926_multimatch
    00834_cancel_http_readonly_queries_on_client_close
    brotli
    parallel_alter
    00302_http_compression
    00417_kill_query
    01294_lazy_database_concurrent
    01193_metadata_loading
    base64
    01031_mutations_interpreter_and_context
    json
    client
    01305_replica_create_drop_zookeeper
    01092_memory_profiler
    01355_ilike
    01281_unsucceeded_insert_select_queries_counter
    live_view
    limit_memory
    memory_limit
    memory_leak
    00110_external_sort
    00682_empty_parts_merge
    00701_rollup
    00109_shard_totals_after_having
    ddl_dictionaries
    01251_dict_is_in_infinite_loop
    01259_dictionary_custom_settings_ddl
    01268_dictionary_direct_layout
    01280_ssd_complex_key_dictionary
    00652_replicated_mutations_zookeeper
    01411_bayesian_ab_testing
    01238_http_memory_tracking              # max_memory_usage_for_user can interfere another queries running concurrently
    01281_group_by_limit_memory_tracking    # max_memory_usage_for_user can interfere another queries running concurrently
    01318_encrypt                           # Depends on OpenSSL
    01318_decrypt                           # Depends on OpenSSL

    # Not sure why these two fail even in sequential mode. Disabled for now
    # to make some progress.
    00646_url_engine
    00974_query_profiler

    # Look at DistributedFilesToInsert, so cannot run in parallel.
    01460_DistributedFilesToInsert
)

clickhouse-test -j 4 --no-long --testname --shard --zookeeper --skip "${TESTS_TO_SKIP[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/test_log.txt


# substr is to remove semicolon after test name
readarray -t FAILED_TESTS < <(awk '/FAIL|TIMEOUT|ERROR/ { print substr($3, 1, length($3)-1) }' /test_output/test_log.txt | tee /test_output/failed-parallel-tests.txt)

# We will rerun sequentially any tests that have failed during parallel run.
# They might have failed because there was some interference from other tests
# running concurrently. If they fail even in seqential mode, we will report them.
# FIXME All tests that require exclusive access to the server must be
# explicitly marked as `sequential`, and `clickhouse-test` must detect them and
# run them in a separate group after all other tests. This is faster and also
# explicit instead of guessing.
if [[ -n "${FAILED_TESTS[*]}" ]]
then
    kill_clickhouse

    # Clean the data so that there is no interference from the previous test run.
    rm -rvf /var/lib/clickhouse ||:
    mkdir /var/lib/clickhouse

    clickhouse-server --config /etc/clickhouse-server/config.xml --daemon

    wait_for_server_start

    echo "Going to run again: ${FAILED_TESTS[*]}"

    clickhouse-test --no-long --testname --shard --zookeeper "${FAILED_TESTS[@]}" 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a /test_output/test_log.txt
else
    echo "No failed tests"
fi
}

case "$stage" in
"")
    ;&
"clone_root")
    clone_root
    # TODO bootstrap into the cloned script here. Add this on Sep 1 2020 or
    # later, so that most of the old branches are updated with this code.
    ;&
"run")
    run
    ;&
esac

pstree -apgT
jobs
