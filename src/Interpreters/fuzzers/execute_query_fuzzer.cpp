#include <Databases/DatabaseMemory.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/registerInterpreters.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Core/Settings.h>

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
#include <Common/QueryScope.h>

#include <filesystem>
#include <cstring>
#include <iostream>
#include <map>

/// This fuzzer supports additional options: arguments after -ignore_remaining_args=1 are parsed as settings and applied to the context.

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

// Helper function to check if this is a merge run
bool isMerge(int argc, char ** argv)
{
    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg{argv[i]};
        if (std::string_view{arg.begin(), std::ranges::find(arg, '=')} == "-ignore_remaining_args")
            break;
        if (std::string_view{arg.begin(), std::ranges::find(arg, '=')} == "-merge")
            return true;
    }
    return false;
}

// Helper function to parse settings from command line arguments
std::map<std::string, std::string> parseSettingsFromArgs(int argc, char ** argv)
{
    std::map<std::string, std::string> settings;
    bool ignore_remaining = false;

    for (int i = 1; i < argc; ++i)
    {
        std::string arg{argv[i]};

        if (!ignore_remaining)
        {
            // Check for -ignore_remaining_args
            if (arg.starts_with("-ignore_remaining_args"))
            {
                ignore_remaining = true;
                continue;
            }
        }
        else
        {
            // Parse settings after -ignore_remaining_args
            size_t eq_pos = arg.find('=');
            if (eq_pos != std::string::npos)
            {
                // Skip leading dashes to get the setting name
                size_t key_start = 0;
                while (key_start < arg.length() && arg[key_start] == '-')
                    ++key_start;

                std::string key = arg.substr(key_start, eq_pos - key_start);
                std::string value = arg.substr(eq_pos + 1);
                settings[key] = value;
            }
        }
    }

    return settings;
}

extern "C" int LLVMFuzzerInitialize(const int * argc, char *** argv)
{
    if (context)
        return true;

    // Check if this is a merge run and skip initialization if so
    if (isMerge(*argc, *argv))
        return 0;

    static SharedContextHolder shared_context = Context::createShared();
    context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();
    context->setConfig(getConfigurationFromXMLString(config_xml));

    Settings settings;
    for (const auto & [key, value] : parseSettingsFromArgs(*argc, *argv))
    {
        try
        {
            settings.set(key, value);
        }
        catch (const std::exception & e)
        {
            std::cerr << "Warning: Failed to set setting '" << key << "' to '" << value << "': " << e.what() << std::endl;
        }
    }
    context->setSettings(settings);

    /// Initialize temporary storage for processing queries
    context->setTemporaryStoragePath((fs::temp_directory_path() / "clickhouse_fuzzer_tmp" / "").string(), 0);

    MainThreadStatus::getInstance();

    getActivePartsLoadingThreadPool().initialize(4, 0, 100);

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

        QueryScope query_scope;
        if (!CurrentThread::getGroup())
        {
            query_scope = QueryScope::create(query_context);
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
        // Ok
    }

    return 0;
}
