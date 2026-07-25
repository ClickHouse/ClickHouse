#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>

using namespace DB;

/// Feeds arbitrary bytes to `QueryPlan::deserialize` — the entry point that consumes the
/// inter-server `QueryPlan` packet. The v4 outline-first format localizes malformed input at
/// frame boundaries; any exception is fine, crashes and sanitizer reports are findings.

static ContextMutablePtr fuzzer_context;

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv);
extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerInitialize(const int *, char ***)
{
    if (fuzzer_context)
        return 0;

    static SharedContextHolder shared_context = Context::createShared();
    fuzzer_context = Context::createGlobal(shared_context.get());
    fuzzer_context->makeGlobalContext();

    registerFunctions();
    registerAggregateFunctions();
    QueryPlanStepRegistry::registerPlanSteps();

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        ReadBufferFromMemory in(data, size);
        /// Cap the type-decoding complexity the way an untrusted client packet does.
        auto plan_and_sets = QueryPlan::deserialize(in, fuzzer_context, /*max_type_complexity=*/1000);
    }
    catch (...)
    {
        /// Malformed input is expected to throw.
    }

    return 0;
}
