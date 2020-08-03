#!/bin/bash

set -x -e

ls -la

git clone https://github.com/ClickHouse/ClickHouse.git | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/clone_log.txt
cd ClickHouse
CLICKHOUSE_DIR=`pwd`


if [ "$PULL_REQUEST_NUMBER" != "0" ]; then
    if git fetch origin "+refs/pull/$PULL_REQUEST_NUMBER/merge"; then
        git checkout FETCH_HEAD
        echo 'Clonned merge head'
    else
        git fetch
        git checkout $COMMIT_SHA
        echo 'Checked out to commit'
    fi
else
    if [ "$COMMIT_SHA" != "" ]; then
        git checkout $COMMIT_SHA
    fi
fi

SUBMODULES_TO_UPDATE="contrib/boost contrib/zlib-ng contrib/libxml2 contrib/poco contrib/libunwind contrib/ryu contrib/fmtlib contrib/base64 contrib/cctz contrib/libcpuid contrib/double-conversion contrib/libcxx contrib/libcxxabi contrib/libc-headers contrib/lz4 contrib/zstd contrib/fastops contrib/rapidjson contrib/re2 contrib/sparsehash-c11"

git submodule update --init --recursive $SUBMODULES_TO_UPDATE | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/submodule_log.txt

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
CLICKHOUSE_BUILD_DIR=`pwd`
cmake .. -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_CXX_COMPILER=clang++-10 -DCMAKE_C_COMPILER=clang-10 $CMAKE_LIBS_CONFIG | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/cmake_log.txt
ninja clickhouse-bundle | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/build_log.txt
ninja install | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/install_log.txt


ccache --show-stats ||:

mkdir -p /etc/clickhouse-server
mkdir -p /etc/clickhouse-client
mkdir -p /etc/clickhouse-server/config.d
mkdir -p /etc/clickhouse-server/users.d
mkdir -p /var/log/clickhouse-server
cp $CLICKHOUSE_DIR/programs/server/config.xml /etc/clickhouse-server/
cp $CLICKHOUSE_DIR/programs/server/users.xml /etc/clickhouse-server/

mkdir -p /etc/clickhouse-server/dict_examples
ln -s /usr/share/clickhouse-test/config/ints_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/strings_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/decimals_dictionary.xml /etc/clickhouse-server/dict_examples/
ln -s /usr/share/clickhouse-test/config/zookeeper.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/listen.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/part_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/text_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/metric_log.xml /etc/clickhouse-server/config.d/
ln -s /usr/share/clickhouse-test/config/query_masking_rules.xml /etc/clickhouse-server/config.d/
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

clickhouse-server --config /etc/clickhouse-server/config.xml --daemon

until clickhouse-client --query "SELECT 1"
do
    sleep 0.1
done

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
    # TRUNCATE TABLE system.query_log -- conflicts with other tests
    01413_rows_events
)

clickhouse-test -j 4 --no-long --testname --shard --zookeeper --skip ${TESTS_TO_SKIP[*]} 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee /test_output/test_log.txt


kill_clickhouse () {
    kill `ps ax | grep clickhouse-server | grep -v 'grep' | awk '{print $1}'` 2>/dev/null

    for i in {1..10}
    do
        if ! kill -0 `ps ax | grep clickhouse-server | grep -v 'grep' | awk '{print $1}'`; then
            echo "No clickhouse process"
            break
        else
            echo "Process" `ps ax | grep clickhouse-server | grep -v 'grep' | awk '{print $1}'` "still alive"
            sleep 10
        fi
    done
}


FAILED_TESTS=`grep 'FAIL\|TIMEOUT\|ERROR' /test_output/test_log.txt | awk 'BEGIN { ORS=" " }; { print substr($3, 1, length($3)-1) }'`


if [[ ! -z "$FAILED_TESTS" ]]; then
    kill_clickhouse

    clickhouse-server --config /etc/clickhouse-server/config.xml --daemon

    until clickhouse-client --query "SELECT 1"
    do
        sleep 0.1
    done

    echo "Going to run again: $FAILED_TESTS"

    clickhouse-test --no-long --testname --shard --zookeeper $FAILED_TESTS 2>&1 | ts '%Y-%m-%d %H:%M:%S' | tee -a /test_output/test_log.txt
else
    echo "No failed tests"
fi

mv /var/log/clickhouse-server/* /test_output
