#include <iostream>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include "Processors/Executors/PullingPipelineExecutor.h"

using namespace DB;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    std::string input = std::string(reinterpret_cast<const char*>(data), size);

    static SharedContextHolder shared_context;
    static ContextMutablePtr context;
    static size_t execution_count = 0;
    ++execution_count;
    if (execution_count == 1)
    {
        shared_context = Context::createShared();
        context = Context::createGlobal(shared_context.get());
        context->makeGlobalContext();
        context->setApplicationType(Context::ApplicationType::LOCAL);
    }

    auto io = DB::executeQuery(input, context, true, QueryProcessingStage::Complete);

    PullingPipelineExecutor executor(io.pipeline);
    Block res;
    while (!res && executor.pull(res));

    return 0;
}
catch (...)
{
    return 1;
}
