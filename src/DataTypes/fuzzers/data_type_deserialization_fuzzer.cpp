#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeFactory.h>

#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/registerAggregateFunctions.h>


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    using namespace DB;

    static SharedContextHolder shared_context;
    static ContextMutablePtr context;

    auto initialize = [&]() mutable
    {
        shared_context = Context::createShared();
        context = Context::createGlobal(shared_context.get());
        context->makeGlobalContext();
        context->setApplicationType(Context::ApplicationType::LOCAL);

        MainThreadStatus::getInstance();

        registerAggregateFunctions();
        return true;
    };

    static bool initialized = initialize();
    (void) initialized;

    total_memory_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1_GiB);
    CurrentThread::get().memory_tracker.resetCounters();
    CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

    /// The input format is as follows:
    /// - data type name on the first line,
    /// - the data for the rest of the input.

    /// Compile the code as follows:
    ///   mkdir build_asan_fuzz
    ///   cd build_asan_fuzz
    ///   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
    ///
    /// The corpus is located here:
    /// https://github.com/ClickHouse/fuzz-corpus/tree/main/data_type_deserialization
    ///
    /// The fuzzer can be run as follows:
    ///   ../../../build_asan_fuzz/src/DataTypes/fuzzers/data_type_deserialization_fuzzer corpus -jobs=64 -rss_limit_mb=8192

    /// clickhouse-local --query "SELECT toJSONString(*) FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.data_type_families)" > dictionary

    DB::ReadBufferFromMemory in(data, size);

    String data_type;
    readStringUntilNewlineInto(data_type, in);
    assertChar('\n', in);

    DataTypePtr type = DataTypeFactory::instance().get(data_type);

    FormatSettings settings;
    settings.max_binary_string_size = 100;
    settings.max_binary_array_size = 100;

    Field field;
    type->getDefaultSerialization()->deserializeBinary(field, in, settings);

    return 0;
}
catch (...)
{
    return 1;
}
