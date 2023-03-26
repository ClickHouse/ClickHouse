#include <iostream>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include "Processors/Executors/PullingPipelineExecutor.h"

#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>

using namespace DB;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    std::string input = std::string(reinterpret_cast<const char*>(data), size);

    static SharedContextHolder shared_context;
    static ContextMutablePtr context;

    auto initialize = [&]() mutable
    {
        shared_context = Context::createShared();
        context = Context::createGlobal(shared_context.get());
        context->makeGlobalContext();
        context->setApplicationType(Context::ApplicationType::LOCAL);

        registerFunctions();
        registerAggregateFunctions();
        registerTableFunctions();
        registerStorages();
        registerDictionaries();
        registerDisks();
        registerFormats();

        return true;
    };

    static bool initialized = initialize();
    (void) initialized;

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
