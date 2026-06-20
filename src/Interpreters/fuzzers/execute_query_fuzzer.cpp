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
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/CurrentThread.h>

#include <filesystem>

using namespace DB;
namespace fs = std::filesystem;


static ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

const char * config_xml = "<clickhouse></clickhouse>";

ContextMutablePtr context;

extern "C" int LLVMFuzzerInitialize(int *, char ***)
{
    if (context)
        return true;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();
    context->setConfig(getConfigurationFromXMLString(config_xml));

    /// Initialize temporary storage for processing queries
    context->setTemporaryStoragePath((fs::temp_directory_path() / "clickhouse_fuzzer_tmp" / "").string(), 0);

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

        auto query_context = Context::createCopy(context);
        query_context->makeQueryContext();
        query_context->setCurrentQueryId({});

        CurrentThread::QueryScope query_scope;
        if (!CurrentThread::getGroup())
        {
            query_scope = CurrentThread::QueryScope::create(query_context);
        }

        auto io = DB::executeQuery(input, std::move(query_context), QueryFlags{ .internal = true }, QueryProcessingStage::Complete).second;

        /// Execute only SELECTs
        if (io.pipeline.pulling())
        {
            io.executeWithCallbacks([&]()
            {
                PullingPipelineExecutor executor(io.pipeline);
                Block res;
                while (res.empty() && executor.pull(res));
            });
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
