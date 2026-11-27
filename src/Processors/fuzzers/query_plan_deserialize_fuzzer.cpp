#include <AggregateFunctions/registerAggregateFunctions.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Parsers/SecretArguments.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>


/// The input is the body of a `QueryPlan` packet, which `TCPHandler::receiveQueryPlan` reads from
/// the initiator of a query with `serialize_query_plan`. The sets are made as `executeQuery` does next.
/// No table is attached, so the sets and steps that read a table fail to resolve it.

using namespace DB;

/// No engine is registered, so the secrets of their arguments are shown.
[[maybe_unused]] static const bool secret_arguments_finder_installed
    = (setSecretArgumentsFinder(&NoSecretArgumentsFinder::instance()), true);

static ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerFunctions();
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

        /// The steps keep the context of the query they are deserialized for.
        auto query_context = Context::createCopy(context);
        query_context->makeQueryContext();

        ReadBufferFromMemory in(data, size);
        auto plan_and_sets = QueryPlan::deserialize(in, query_context, getBinaryTypeDecodingComplexityLimit(query_context));
        std::ignore = QueryPlan::makeSets(std::move(plan_and_sets), query_context);
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed plans are rejected with exceptions.
    }

    return 0;
}
