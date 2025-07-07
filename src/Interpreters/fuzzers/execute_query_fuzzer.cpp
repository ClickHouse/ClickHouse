#include <Databases/DatabaseMemory.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/registerInterpreters.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>

#include <Databases/registerDatabases.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/CurrentThread.h>

using namespace DB;


ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    MainThreadStatus::getInstance();

    registerInterpreters();
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerDatabases();
    registerStorages();
    registerDictionaries();
    registerDisks(/* global_skip_access_check= */ true);
    registerFormats();

    /// Initialize default database
    {
        const std::string default_database = "default";
        DatabasePtr database = std::make_shared<DatabaseMemory>(default_database, context);
        if (UUID uuid = database->getUUID(); uuid != UUIDHelpers::Nil)
            DatabaseCatalog::instance().addUUIDMapping(uuid);
        DatabaseCatalog::instance().attachDatabase(default_database, database);
        context->setCurrentDatabase(default_database);
    }

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

        std::string input = std::string(reinterpret_cast<const char*>(data), size);

        auto io = DB::executeQuery(input, context, QueryFlags{ .internal = true }, QueryProcessingStage::Complete).second;

        /// Execute only SELECTs
        if (io.pipeline.pulling())
        {
            PullingPipelineExecutor executor(io.pipeline);
            Block res;
            while (!res && executor.pull(res));
        }
        /// We don't want to execute it and thus need to finish it properly.
        else
        {
            io.onCancelOrConnectionLoss();
        }
    }
    catch (...)
    {
    }

    return 0;
}
