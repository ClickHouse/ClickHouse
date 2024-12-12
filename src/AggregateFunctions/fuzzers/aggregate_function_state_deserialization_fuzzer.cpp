#include <base/types.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>

#include <Common/Arena.h>
#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Interpreters/Context.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/registerAggregateFunctions.h>

#include <base/scope_guard.h>

using namespace DB;


ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerAggregateFunctions();

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        total_memory_tracker.resetCounters();
        total_memory_tracker.setHardLimit(1_GiB);
        CurrentThread::get().memory_tracker.resetCounters();
        CurrentThread::get().memory_tracker.setHardLimit(1_GiB);

        /// The input format is as follows:
        /// - the aggregate function name on the first line, possible with parameters, then data types of the arguments,
        ///   example: quantile(0.5), Float64
        /// - the serialized aggregation state for the rest of the input.

        /// Compile the code as follows:
        ///   mkdir build_asan_fuzz
        ///   cd build_asan_fuzz
        ///   CC=clang CXX=clang++ cmake -D SANITIZE=address -D ENABLE_FUZZING=1 -D WITH_COVERAGE=1 ..
        ///
        /// The corpus is located here:
        /// https://github.com/ClickHouse/fuzz-corpus/tree/main/aggregate_function_state_deserialization
        ///
        /// The fuzzer can be run as follows:
        ///   ../../../build_asan_fuzz/src/DataTypes/fuzzers/aggregate_function_state_deserialization corpus -jobs=64 -rss_limit_mb=8192

        DB::ReadBufferFromMemory in(data, size);

        String args;
        readStringUntilNewlineInto(args, in);
        assertChar('\n', in);

        DataTypePtr type = DataTypeFactory::instance().get(fmt::format("AggregateFunction({})", args));
        AggregateFunctionPtr func = assert_cast<const DataTypeAggregateFunction &>(*type).getFunction();

        Arena arena;
        char * place = arena.alignedAlloc(func->sizeOfData(), func->alignOfData());
        func->create(place);
        SCOPE_EXIT(func->destroy(place));
        func->deserialize(place, in, {}, &arena);
    }
    catch (...)
    {
    }

    return 0;
}
