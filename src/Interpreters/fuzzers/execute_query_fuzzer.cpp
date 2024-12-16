#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/registerInterpreters.h>
#include "Processors/Executors/PullingPipelineExecutor.h"

#include <Databases/registerDatabases.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>

using namespace DB;


ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    /// The SharedContext depends on the Logger which is being destroyed by AutoLoggerShutdown (global variable)
    /// And the GlobalContext depends on the SharedContext. So, this the SharedContext has to be static in order
    /// to be destroyed last.
    /// Addditionally, without it being static the shared context is destroyed on this function exit.
    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    registerInterpreters();
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerDatabases();
    registerStorages();
    registerDictionaries();
    registerDisks(/* global_skip_access_check= */ true);
    registerFormats();

    return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        std::string input = std::string(reinterpret_cast<const char*>(data), size);

        auto io = DB::executeQuery(input, context, QueryFlags{ .internal = true }, QueryProcessingStage::Complete).second;

        PullingPipelineExecutor executor(io.pipeline);
        Block res;
        while (!res && executor.pull(res));
    }
    catch (...)
    {
    }

    return 0;
}
