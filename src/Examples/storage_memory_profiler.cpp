/**
 * Storage Memory Profiler Tool
 *
 * A tool to measure memory allocations during SQL query execution including
 * storage creation, data insertion, and other operations.
 *
 * Based on clickhouse-local but with jemalloc heap profiling capabilities.
 *
 * Usage:
 *   MALLOC_CONF=prof:true ./clickhouse-examples storage_memory_profiler -f 01.sql -f 02.sql --output-dir profiles
 */

#include <Examples/clickhouse_examples.h>
#include <Examples/storage_memory_profiler.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <unistd.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/MapConfiguration.h>

#include <base/getMemoryAmount.h>

#include <Access/AccessControl.h>
#include <Access/MemoryAccessStorage.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Core/ServerSettings.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/registerDatabases.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/registerInterpreters.h>
#include <Parsers/parseQuery.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sources/JemallocProfileSource.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/registerStorages.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/Jemalloc.h>
#include <Common/QueryScope.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>

#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric MemoryTracking;
}

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int SYSTEM_ERROR;
}

namespace ServerSetting
{
extern const ServerSettingsUInt64 max_server_memory_usage;
extern const ServerSettingsDouble max_server_memory_usage_to_ram_ratio;
}

namespace
{
/// Create a memory database if it doesn't exist
DatabasePtr createMemoryDatabaseIfNotExists(ContextPtr context, const String & database_name)
{
    auto db = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (db)
        return db;
    db = std::make_shared<DatabaseMemory>(database_name, context);
    DatabaseCatalog::instance().attachDatabase(database_name, db);
    return db;
}
}


void StorageMemoryProfiler::printUsage()
{
    std::cerr << "Storage Memory Profiler - Measure memory consumption of ClickHouse operations\n";
    std::cerr << "\n";
    std::cerr << "Usage: clickhouse-examples storage_memory_profiler [OPTIONS] -f FILE1.sql [-f FILE2.sql ...]\n";
    std::cerr << "\n";
    std::cerr << "Options:\n";
    std::cerr << "  -f, --file FILE         SQL file to execute (can be specified multiple times)\n";
    std::cerr << "  -o, --output-dir DIR    Directory for heap dump files (default: current dir)\n";
    std::cerr << "  -p, --path DIR          Storage path for persistent data\n";
    std::cerr << "  --prefix PREFIX         Prefix for heap dump files (default: memory_profile_)\n";
    std::cerr << "  --no-system-tables      Skip system tables for faster startup\n";
    std::cerr << "  --symbolize             Symbolize each heap dump immediately\n";
    std::cerr << "  -h, --help              Show this help message\n";
    std::cerr << "\n";
    std::cerr << "Environment variables for jemalloc profiling:\n";
    std::cerr << "  Linux:  MALLOC_CONF=prof:true,prof_active:true\n";
    std::cerr << "  macOS:  JE_MALLOC_CONF=prof:true,prof_active:true\n";
    std::cerr << "\n";
    std::cerr << "Example:\n";
    std::cerr << "  MALLOC_CONF=prof:true ./clickhouse-examples storage_memory_profiler \\\n";
    std::cerr << "    -f scenarios/01_create_table.sql \\\n";
    std::cerr << "    -f scenarios/02_insert_data.sql \\\n";
    std::cerr << "    --output-dir profiles\n";
}


void StorageMemoryProfiler::printProfilingStatus()
{
#if USE_JEMALLOC
    std::cerr << "Jemalloc profiling status:\n";

    bool prof_compiled = false;
    size_t sz = sizeof(prof_compiled);
    int ret = je_mallctl("config.prof", &prof_compiled, &sz, nullptr, 0);
    std::cerr << "  config.prof (compiled with profiling): " << (ret == 0 ? (prof_compiled ? "yes" : "no") : "error") << "\n";

    bool prof_enabled = false;
    sz = sizeof(prof_enabled);
    ret = je_mallctl("opt.prof", &prof_enabled, &sz, nullptr, 0);
    std::cerr << "  opt.prof (profiling enabled at runtime): " << (ret == 0 ? (prof_enabled ? "yes" : "no") : "error") << "\n";

    if (!prof_compiled)
    {
        std::cerr << "\nProfiling is not available. jemalloc was compiled without JEMALLOC_PROF.\n";
    }
    else if (!prof_enabled)
    {
        std::cerr << "\nProfiler is compiled but not enabled. This must be set at startup.\n";
        std::cerr << "\nOn Linux, use MALLOC_CONF:\n";
        std::cerr << "  MALLOC_CONF=prof:true <program>\n";
        std::cerr << "\nOn macOS, use JE_MALLOC_CONF:\n";
        std::cerr << "  JE_MALLOC_CONF=prof:true,prof_active:true <program>\n";
    }
#else
    std::cerr << "jemalloc is not enabled in this build.\n";
#endif
}


void StorageMemoryProfiler::flushJemallocThreadCache()
{
#if USE_JEMALLOC
    int ret = je_mallctl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    if (ret != 0)
        std::cerr << "Warning: thread.tcache.flush failed: " << ret << "\n";
#endif
}


void StorageMemoryProfiler::refreshJemallocEpoch()
{
#if USE_JEMALLOC
    uint64_t epoch = 1;
    size_t epoch_size = sizeof(epoch);
    int ret = je_mallctl("epoch", &epoch, &epoch_size, &epoch, epoch_size);
    if (ret != 0)
        std::cerr << "Warning: epoch refresh failed: " << ret << "\n";
#endif
}


size_t StorageMemoryProfiler::getJemallocAllocated()
{
#if USE_JEMALLOC
    size_t allocated = 0;
    size_t allocated_size = sizeof(allocated);
    int ret = je_mallctl("stats.allocated", &allocated, &allocated_size, nullptr, 0);
    if (ret != 0)
        std::cerr << "Warning: stats.allocated failed: " << ret << "\n";
    return allocated;
#else
    return 0;
#endif
}


bool StorageMemoryProfiler::isProfilingCompiled()
{
#if USE_JEMALLOC
    bool compiled = false;
    size_t sz = sizeof(compiled);
    int ret = je_mallctl("config.prof", &compiled, &sz, nullptr, 0);
    return (ret == 0) && compiled;
#else
    return false;
#endif
}


bool StorageMemoryProfiler::isProfilingEnabled()
{
#if USE_JEMALLOC
    bool enabled = false;
    size_t enabled_size = sizeof(enabled);
    int ret = je_mallctl("opt.prof", &enabled, &enabled_size, nullptr, 0);
    return (ret == 0) && enabled;
#else
    return false;
#endif
}


std::string StorageMemoryProfiler::dumpProfile(const std::string & label)
{
#if USE_JEMALLOC
    static std::atomic<size_t> counter{0};
    std::string path
        = output_dir + "/" + profile_prefix + label + "." + std::to_string(getpid()) + "." + std::to_string(counter.fetch_add(1)) + ".heap";
    const char * path_ptr = path.c_str();
    int ret = je_mallctl("prof.dump", nullptr, nullptr, &path_ptr, sizeof(path_ptr));
    if (ret != 0)
        throw Exception(ErrorCodes::SYSTEM_ERROR, "prof.dump failed with code {} for {}", ret, path);

    /// Optionally symbolize the heap profile
    if (symbolize)
    {
        std::string symbolized_path = path + ".symbolized";
        symbolizeJemallocHeapProfile(path, symbolized_path);
        std::cerr << "  Heap dump: " << symbolized_path << "\n";
        return symbolized_path;
    }

    std::cerr << "  Heap dump: " << path << "\n";
    return path;
#else
    (void)label;
    return "";
#endif
}


void StorageMemoryProfiler::setupLogging()
{
    /// Set up minimal logging (only errors)
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%L%H:%M:%S.%i [ %p ] <%l> %s: %t"));
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel(Poco::Message::PRIO_WARNING);
}


void StorageMemoryProfiler::initializeContext()
{
    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    Poco::AutoPtr<Poco::Util::MapConfiguration> config(new Poco::Util::MapConfiguration);
    config->setString("profiles.default", "");
    config->setString("users.default.password", "");
    config->setString("users.default.networks.ip", "::/0");
    config->setString("users.default.profile", "default");
    config->setString("users.default.quota", "default");
    config->setString("quotas.default", "");
    global_context->setConfig(config);
    global_context->setApplicationType(Context::ApplicationType::LOCAL);

#define INITIALIZE_STATIC_THREAD_POOL(SUFFIX, NAME, METRIC) get##SUFFIX##ThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    APPLY_FOR_STATIC_THREAD_POOLS(INITIALIZE_STATIC_THREAD_POOL)
#undef INITIALIZE_STATIC_THREAD_POOL

    /// Set up path
    if (!data_path.empty())
    {
        fs::create_directories(data_path);
        global_context->setPath(data_path);
        global_context->setTemporaryStoragePath(fs::path(data_path) / "tmp" / "", 0);
        global_context->setFlagsPath(fs::path(data_path) / "flags" / "");
        global_context->setUserFilesPath(fs::path(data_path) / "user_files" / "");
    }
    else
    {
        /// Create a temporary directory
        std::string tmp_path = fs::temp_directory_path() / ("clickhouse_memory_profiler_" + std::to_string(getpid()));
        fs::create_directories(tmp_path);
        temporary_directory_to_delete = tmp_path;
        global_context->setPath(tmp_path);
        global_context->setTemporaryStoragePath(fs::path(tmp_path) / "tmp" / "", 0);
    }

    /// Set up memory tracking
    const auto & server_settings = global_context->getServerSettings();
    size_t max_server_memory_usage = server_settings[ServerSetting::max_server_memory_usage];
    const double max_server_memory_usage_to_ram_ratio = server_settings[ServerSetting::max_server_memory_usage_to_ram_ratio];
    const size_t physical_server_memory = getMemoryAmount();

    if (max_server_memory_usage == 0)
        max_server_memory_usage = static_cast<size_t>(static_cast<double>(physical_server_memory) * max_server_memory_usage_to_ram_ratio);

    total_memory_tracker.setHardLimit(max_server_memory_usage);
    total_memory_tracker.setDescription("(total)");
    total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);

    /// Limit on total number of concurrently executing queries.
    global_context->getProcessList().setMaxSize(0);

    /// Set up minimal access control
    auto & access_control = global_context->getAccessControl();
    access_control.setNoPasswordAllowed(true);
    access_control.setPlaintextPasswordAllowed(true);
    global_context->setUsersConfig(config);
    access_control.addMemoryStorage(MemoryAccessStorage::STORAGE_TYPE, /* allow_backup= */ false);
    global_context->setDefaultProfiles(*config);
}


void StorageMemoryProfiler::registerComponents()
{
    registerInterpreters();
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerDatabases();
    registerStorages();
    registerDictionaries();
    registerDisks(/* global_skip_access_check= */ true);
    registerFormats();
}


void StorageMemoryProfiler::setupDatabase()
{
    /// Initialize temporary database
    DatabaseCatalog::instance().initializeAndLoadTemporaryDatabase();

    /// Set up default database
    std::string default_database = "default";
    DatabasePtr database = std::make_shared<DatabaseMemory>(default_database, global_context);
    DatabaseCatalog::instance().attachDatabase(default_database, database);
    global_context->setCurrentDatabase(default_database);

    /// Attach system tables if needed
    if (!no_system_tables)
    {
        attachSystemTablesServer(
            global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE), false, false);
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(
            global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
    }

    /// Create background tasks for DDL operations
    DatabaseCatalog::instance().createBackgroundTasks();
    DatabaseCatalog::instance().startupBackgroundTasks();
}


void StorageMemoryProfiler::cleanup()
{
    if (global_context)
    {
        global_context->shutdown();
        global_context.reset();
    }
    shared_context.reset();

    /// Clean up temporary directory
    if (temporary_directory_to_delete)
    {
        try
        {
            fs::remove_all(*temporary_directory_to_delete);
        }
        catch (...)
        {
            std::cerr << "Warning: Failed to remove temporary directory: " << *temporary_directory_to_delete << "\n";
        }
    }
}


void StorageMemoryProfiler::executeQueriesFromFile(const std::string & filepath)
{
    ReadBufferFromFile file(filepath);
    String content;
    readStringUntilEOF(content, file);

    if (content.empty())
        return;

    std::vector<std::string> queries;
    const bool parsed_all = splitMultipartQuery(
                                content,
                                queries,
                                /* max_query_size= */ 0,
                                DBMS_DEFAULT_MAX_PARSER_DEPTH,
                                DBMS_DEFAULT_MAX_PARSER_BACKTRACKS,
                                /* allow_settings_after_format_in_insert= */ false,
                                /* implicit_select= */ false)
                                .second;
    if (!parsed_all)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot split all queries in SQL file: {}", filepath);

    for (const auto & query_text : queries)
    {
        auto context = Context::createCopy(global_context);
        context->makeQueryContext();
        context->setCurrentQueryId("");
        context->setClientInterface(ClientInfo::Interface::LOCAL);
        auto query_scope = QueryScope::create(context);
        auto res = executeQuery(query_text, context, QueryFlags{}, QueryProcessingStage::Complete).second;
        if (res.pipeline.initialized() && !res.pipeline.completed())
            res.pipeline.complete(std::make_shared<EmptySink>(res.pipeline.getSharedHeader()));
        executeTrivialBlockIO(res, context);
    }
}


int StorageMemoryProfiler::run(const std::vector<std::string> & args)
{
    /// Parse command line arguments
    for (size_t i = 0; i < args.size(); ++i)
    {
        const std::string & arg = args[i];

        if (arg == "-h" || arg == "--help")
        {
            printUsage();
            return 0;
        }
        else if (arg == "-f" || arg == "--file")
        {
            if (i + 1 >= args.size())
            {
                std::cerr << "Error: -f requires an argument\n";
                return 1;
            }
            sql_files.push_back(args[++i]);
        }
        else if (arg == "-o" || arg == "--output-dir")
        {
            if (i + 1 >= args.size())
            {
                std::cerr << "Error: -o requires an argument\n";
                return 1;
            }
            output_dir = args[++i];
        }
        else if (arg == "-p" || arg == "--path")
        {
            if (i + 1 >= args.size())
            {
                std::cerr << "Error: -p requires an argument\n";
                return 1;
            }
            data_path = args[++i];
        }
        else if (arg == "--prefix")
        {
            if (i + 1 >= args.size())
            {
                std::cerr << "Error: --prefix requires an argument\n";
                return 1;
            }
            profile_prefix = args[++i];
        }
        else if (arg == "--no-system-tables")
        {
            no_system_tables = true;
        }
        else if (arg == "--symbolize")
        {
            symbolize = true;
        }
        else
        {
            std::cerr << "Unknown option: " << arg << "\n";
            printUsage();
            return 1;
        }
    }

    if (sql_files.empty())
    {
        std::cerr << "Error: No SQL files specified. Use -f to specify files.\n\n";
        printUsage();
        return 1;
    }

    /// Create output directory
    fs::create_directories(output_dir);

    /// Check jemalloc profiling
    if (!isProfilingCompiled())
    {
        std::cerr << "Error: jemalloc profiling is not compiled in.\n";
        return 1;
    }
    else if (!isProfilingEnabled())
    {
        std::cerr << "Error: jemalloc profiling is not enabled.\n";
        printProfilingStatus();
        return 1;
    }

    try
    {
        setupLogging();
        registerComponents();
        initializeContext();
        setupDatabase();

        SCOPE_EXIT({ cleanup(); });

        /// Print header
        std::cout << "# Memory Profiler Summary\n";
        std::cout << "# Timestamp: " << std::chrono::system_clock::now().time_since_epoch().count() << "\n";
        std::cout << "# Files: ";
        for (size_t i = 0; i < sql_files.size(); ++i)
        {
            if (i > 0)
                std::cout << ", ";
            std::cout << fs::path(sql_files[i]).filename().string();
        }
        std::cout << "\n";
        std::cout << "#\n";
        std::cout << "checkpoint\tallocated_bytes\tdiff_from_start\tdiff_from_prev\n";

        /// Initial measurement
        flushJemallocThreadCache();
        refreshJemallocEpoch();
        size_t initial_allocated = getJemallocAllocated();
        size_t prev_allocated = initial_allocated;

        std::cout << "start\t" << initial_allocated << "\t0\t0\n";

        dumpProfile("start");

        /// Process each SQL file
        for (size_t i = 0; i < sql_files.size(); ++i)
        {
            const std::string & filepath = sql_files[i];
            std::string filename = fs::path(filepath).stem().string();

            std::cerr << "Executing: " << filepath << "...\n";

            executeQueriesFromFile(filepath);

            /// Measure memory after execution
            flushJemallocThreadCache();
            refreshJemallocEpoch();
            size_t current_allocated = getJemallocAllocated();

            std::string label = fmt::format("after_{:02d}_{}", i + 1, filename);
            int64_t diff_from_start = static_cast<int64_t>(current_allocated) - static_cast<int64_t>(initial_allocated);
            int64_t diff_from_prev = static_cast<int64_t>(current_allocated) - static_cast<int64_t>(prev_allocated);

            std::cout << label << "\t" << current_allocated << "\t" << diff_from_start << "\t" << diff_from_prev << "\n";

            dumpProfile(label);

            prev_allocated = current_allocated;
        }

        std::cerr << "\nMemory profiling complete!\n";
        std::cerr << "Output directory: " << output_dir << "\n";

        return 0;
    }
    catch (const Exception & e)
    {
        std::cerr << "Error: " << e.message() << "\n";
        return 1;
    }
    catch (const std::exception & e)
    {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
}

}

/// Entry point
int mainEntryExampleStorageMemoryProfiler(int argc, char ** argv)
{
    DB::MainThreadStatus::getInstance();

    SCOPE_EXIT_SAFE({
        DB::StaticThreadPool::shutdownAll();
        GlobalThreadPool::shutdown();
    });

    try
    {
        DB::StorageMemoryProfiler app;
        std::vector<std::string> args(argv + 1, argv + argc);
        return app.run(args);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        return 1;
    }
}
