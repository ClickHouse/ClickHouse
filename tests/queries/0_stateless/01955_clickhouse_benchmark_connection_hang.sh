#!/usr/bin/env bash
# Tags: no-ubsan
# Tag no-ubsan: Limits RLIMIT_NOFILE, see comment in the test

# shellcheck disable=SC2086

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# NOTE: Tests with limit for number of opened files cannot be run under UBsan.
#
# UBsan needs to create pipe each time it need to check the type:
#
#     pipe()
#     __sanitizer::IsAccessibleMemoryRange(unsigned long, unsigned long)
#     __ubsan::checkDynamicType(void*, void*, unsigned long) + 271
#     HandleDynamicTypeCacheMiss(__ubsan::DynamicTypeCacheMissData*, unsigned long, unsigned long, __ubsan::ReportOptions) + 34
#     __ubsan_handle_dynamic_type_cache_miss_abort + 58
#
# Obviously it will fail if RLIMIT_NOFILE exceeded (like in this test), and the UBsan will falsely report [1]:
#
#     01955_clickhouse_benchmark_connection_hang:                             [ FAIL ] 1.56 sec. - result differs with reference:
#     --- /usr/share/clickhouse-test/queries/0_stateless/01955_clickhouse_benchmark_connection_hang.reference	2021-07-21 11:14:58.000000000 +0300
#     +++ /tmp/clickhouse-test/0_stateless/01955_clickhouse_benchmark_connection_hang.stdout	2021-07-21 11:53:45.684050372 +0300
#     @@ -1,3 +1,22 @@
#      Loaded 1 queries.
#     -I/O error: Too many open files
#     -70
#     +../contrib/libcxx/include/memory:3212:19: runtime error: member call on address 0x00002939d5c0 which does not point to an object of type 'std::__1::__shared_weak_count'
#     +0x00002939d5c0: note: object has invalid vptr
#     +<memory cannot be printed>
#     +==558==WARNING: Can't create a socket pair to start external symbolizer (errno: 24)
#     +==558==WARNING: Can't create a socket pair to start external symbolizer (errno: 24)
#     +==558==WARNING: Can't create a socket pair to start external symbolizer (errno: 24)
#     +==558==WARNING: Can't create a socket pair to start external symbolizer (errno: 24)
#     +==558==WARNING: Can't create a socket pair to start external symbolizer (errno: 24)
#     +==558==WARNING: Failed to use and restart external symbolizer!
#     +    #0 0xfe86b57  (/usr/bin/clickhouse+0xfe86b57)
#     +    #1 0xfe83fd7  (/usr/bin/clickhouse+0xfe83fd7)
#     +    #2 0xfe89af4  (/usr/bin/clickhouse+0xfe89af4)
#     +    #3 0xfe81fa9  (/usr/bin/clickhouse+0xfe81fa9)
#     +    #4 0x1f377609  (/usr/bin/clickhouse+0x1f377609)
#     +    #5 0xfe7e2a1  (/usr/bin/clickhouse+0xfe7e2a1)
#     +    #6 0xfce1003  (/usr/bin/clickhouse+0xfce1003)
#     +    #7 0x7f3345bd30b2  (/lib/x86_64-linux-gnu/libc.so.6+0x270b2)
#     +    #8 0xfcbf0ed  (/usr/bin/clickhouse+0xfcbf0ed)
#     +
#     +SUMMARY: UndefinedBehaviorSanitizer: undefined-behavior ../contrib/libcxx/include/memory:3212:19 in 
#     +1
#
# Stacktrace from lldb:
#
#      thread #1, name = 'clickhouse-benc', stop reason = Dynamic type mismatch
#    * frame #0: 0x000000000fffc070 clickhouse`__ubsan_on_report
#      frame #1: 0x000000000fff6511 clickhouse`__ubsan::Diag::~Diag() + 209
#      frame #2: 0x000000000fffcb11 clickhouse`HandleDynamicTypeCacheMiss(__ubsan::DynamicTypeCacheMissData*, unsigned long, unsigned long, __ubsan::ReportOptions) + 609
#      frame #3: 0x000000000fffcf2a clickhouse`__ubsan_handle_dynamic_type_cache_miss_abort + 58
#      frame #4: 0x00000000101a33f8 clickhouse`std::__1::shared_ptr<PoolBase<DB::Connection>::PoolEntryHelper>::~shared_ptr(this=<unavailable>) + 152 at memory:3212
#      frame #5: 0x00000000101a267a clickhouse`PoolBase<DB::Connection>::Entry::~Entry(this=<unavailable>) + 26 at PoolBase.h:67
#      frame #6: 0x00000000101a0878 clickhouse`DB::ConnectionPool::get(this=<unavailable>, timeouts=0x00007fffffffc278, settings=<unavailable>, force_connected=true) + 664 at ConnectionPool.h:93
#      frame #7: 0x00000000101a6395 clickhouse`DB::Benchmark::runBenchmark(this=<unavailable>) + 981 at Benchmark.cpp:309
#      frame #8: 0x000000001019e84a clickhouse`DB::Benchmark::main(this=0x00007fffffffd8c8, (null)=<unavailable>) + 586 at Benchmark.cpp:128
#      frame #9: 0x000000001f5d028a clickhouse`Poco::Util::Application::run(this=0x00007fffffffd8c8) + 42 at Application.cpp:334
#      frame #10: 0x000000001019ab42 clickhouse`mainEntryClickHouseBenchmark(argc=<unavailable>, argv=<unavailable>) + 6978 at Benchmark.cpp:655
#      frame #11: 0x000000000fffdfc4 clickhouse`main(argc_=<unavailable>, argv_=<unavailable>) + 356 at main.cpp:366
#      frame #12: 0x00007ffff7de6d0a libc.so.6`__libc_start_main(main=(clickhouse`main at main.cpp:339), argc=7, argv=0x00007fffffffe1e8, init=<unavailable>, fini=<unavailable>, rtld_fini=<unavailable>, stack_end=0x00007fffffffe1d8) + 234 at libc-start.c:308
#      frame #13: 0x000000000ffdc0aa clickhouse`_start + 42
#
#   [1]: https://clickhouse-test-reports.s3.yandex.net/26656/f17ca450ac991603e6400c7caef49c493ac69739/functional_stateless_tests_(ubsan).html#fail1

# Limit number of files to 50, and we will get EMFILE for some of socket()
prlimit --nofile=50 $CLICKHOUSE_BENCHMARK --iterations 1 --concurrency 50 --query 'select 1' 2>&1
echo $?
