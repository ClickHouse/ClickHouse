#include "LocalServer.h"

#include <sys/resource.h>
#include <Common/Config/getLocalConfigPath.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
#include <Core/UUID.h>
#include <base/getMemoryAmount.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/String.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Poco/SimpleFileChannel.h>
#include <Databases/registerDatabases.h>
#include <Databases/DatabaseFilesystem.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabasesOverlay.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/registerInterpreters.h>
#include <base/getFQDNOrHostName.h>
#include <Access/AccessControl.h>
#include <Common/PoolId.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/ThreadStatus.h>
#include <Common/TLDListsHolder.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Common/ThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/UseSSL.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTInsertQuery.h>
#include <Common/ErrorHandlers.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/program_options/options_description.hpp>
#include <base/argsToConfig.h>
#include <filesystem>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#   include <azure/storage/common/internal/xml_wrapper.hpp>
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
    extern const int CANNOT_LOAD_CONFIG;
    extern const int FILE_ALREADY_EXISTS;
}

void applySettingsOverridesForLocal(ContextMutablePtr context)
{
    Settings settings = context->getSettingsCopy();

    settings.allow_introspection_functions = true;
    settings.storage_file_read_method = LocalFSReadMethod::mmap;

    context->setSettings(settings);
}

Poco::Util::LayeredConfiguration & LocalServer::getClientConfiguration()
{
    return config();
}

void LocalServer::processError(const String &) const
{
    if (ignore_error)
        return;

    if (is_interactive)
    {
        String message;
        if (server_exception)
        {
            message = getExceptionMessage(*server_exception, print_stack_trace, true);
        }
        else if (client_exception)
        {
            message = client_exception->message();
        }

        fmt::print(stderr, "Received exception:\n{}\n", message);
        fmt::print(stderr, "\n");
    }
    else
    {
        if (server_exception)
            server_exception->rethrow();
        if (client_exception)
            client_exception->rethrow();
    }
}


void LocalServer::initialize(Poco::Util::Application & self)
{
    Poco::Util::Application::initialize(self);

    const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
    if (home_path_cstr)
        home_path = home_path_cstr;

    /// Load config files if exists
    std::string config_path;
    if (getClientConfiguration().has("config-file"))
        config_path = getClientConfiguration().getString("config-file");
    else if (config_path.empty() && fs::exists("config.xml"))
        config_path = "config.xml";
    else if (config_path.empty())
        config_path = getLocalConfigPath(home_path).value_or("");

    if (fs::exists(config_path))
    {
        ConfigProcessor config_processor(config_path, false, true);
        ConfigProcessor::setConfigPath(fs::path(config_path).parent_path());
        auto loaded_config = config_processor.loadConfig();
        getClientConfiguration().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    server_settings.loadSettingsFromConfig(config());

    GlobalThreadPool::initialize(
        server_settings.max_thread_pool_size,
        server_settings.max_thread_pool_free_size,
        server_settings.thread_pool_queue_size);

#if USE_AZURE_BLOB_STORAGE
    /// See the explanation near the same line in Server.cpp
    GlobalThreadPool::instance().addOnDestroyCallback([]
    {
        Azure::Storage::_internal::XmlGlobalDeinitialize();
    });
#endif

    getIOThreadPool().initialize(
        server_settings.max_io_thread_pool_size,
        server_settings.max_io_thread_pool_free_size,
        server_settings.io_thread_pool_queue_size);

    const size_t active_parts_loading_threads = server_settings.max_active_parts_loading_thread_pool_size;
    getActivePartsLoadingThreadPool().initialize(
        active_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        active_parts_loading_threads);

    const size_t outdated_parts_loading_threads = server_settings.max_outdated_parts_loading_thread_pool_size;
    getOutdatedPartsLoadingThreadPool().initialize(
        outdated_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        outdated_parts_loading_threads);

    getOutdatedPartsLoadingThreadPool().setMaxTurboThreads(active_parts_loading_threads);

    const size_t unexpected_parts_loading_threads = server_settings.max_unexpected_parts_loading_thread_pool_size;
    getUnexpectedPartsLoadingThreadPool().initialize(
        unexpected_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        unexpected_parts_loading_threads);

    getUnexpectedPartsLoadingThreadPool().setMaxTurboThreads(active_parts_loading_threads);

    const size_t cleanup_threads = server_settings.max_parts_cleaning_thread_pool_size;
    getPartsCleaningThreadPool().initialize(
        cleanup_threads,
        0, // We don't need any threads one all the parts will be deleted
        cleanup_threads);

    getDatabaseCatalogDropTablesThreadPool().initialize(
        server_settings.database_catalog_drop_table_concurrency,
        0, // We don't need any threads if there are no DROP queries.
        server_settings.database_catalog_drop_table_concurrency);
}


static DatabasePtr createMemoryDatabaseIfNotExists(ContextPtr context, const String & database_name)
{
    DatabasePtr system_database = DatabaseCatalog::instance().tryGetDatabase(database_name);
    if (!system_database)
    {
        /// TODO: add attachTableDelayed into DatabaseMemory to speedup loading
        system_database = std::make_shared<DatabaseMemory>(database_name, context);
        DatabaseCatalog::instance().attachDatabase(database_name, system_database);
    }
    return system_database;
}

static DatabasePtr createClickHouseLocalDatabaseOverlay(const String & name_, ContextPtr context_)
{
    auto databaseCombiner = std::make_shared<DatabasesOverlay>(name_, context_);
    databaseCombiner->registerNextDatabase(std::make_shared<DatabaseFilesystem>(name_, "", context_));
    databaseCombiner->registerNextDatabase(std::make_shared<DatabaseMemory>(name_, context_));
    return databaseCombiner;
}

/// If path is specified and not empty, will try to setup server environment and load existing metadata
void LocalServer::tryInitPath()
{
    std::string path;

    if (getClientConfiguration().has("path"))
    {
        // User-supplied path.
        path = getClientConfiguration().getString("path");
        Poco::trimInPlace(path);

        if (path.empty())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot work with empty storage path that is explicitly specified"
                " by the --path option. Please check the program options and"
                " correct the --path.");
        }
    }
    else
    {
        // The path is not provided explicitly - use a unique path in the system temporary directory
        // (or in the current dir if a temporary doesn't exist)
        LoggerRawPtr log = &logger();
        std::filesystem::path parent_folder;
        std::filesystem::path default_path;

        try
        {
            // try to guess a tmp folder name, and check if it's a directory (throw exception otherwise)
            parent_folder = std::filesystem::temp_directory_path();

        }
        catch (const fs::filesystem_error & e)
        {
            // The tmp folder doesn't exist? Is it a misconfiguration? Or chroot?
            LOG_DEBUG(log, "Can not get temporary folder: {}", e.what());
            parent_folder = std::filesystem::current_path();

            std::filesystem::is_directory(parent_folder); // that will throw an exception if it's not a directory
            LOG_DEBUG(log, "Will create working directory inside current directory: {}", parent_folder.string());
        }

        /// we can have another clickhouse-local running simultaneously, even with the same PID (for ex. - several dockers mounting the same folder)
        /// or it can be some leftovers from other clickhouse-local runs
        /// as we can't accurately distinguish those situations we don't touch any existent folders
        /// we just try to pick some free name for our working folder

        default_path = parent_folder / fmt::format("clickhouse-local-{}", UUIDHelpers::generateV4());

        if (fs::exists(default_path))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Unsuccessful attempt to set up the working directory: {} already exists.", default_path.string());

        /// The directory can be created lazily during the runtime.
        temporary_directory_to_delete = default_path;

        path = default_path.string();
        LOG_DEBUG(log, "Working directory created: {}", path);
    }

    global_context->setPath(fs::path(path) / "");

    global_context->setTemporaryStoragePath(fs::path(path) / "tmp" / "", 0);
    global_context->setFlagsPath(fs::path(path) / "flags" / "");

    global_context->setUserFilesPath(""); /// user's files are everywhere

    std::string user_scripts_path = getClientConfiguration().getString("user_scripts_path", fs::path(path) / "user_scripts/");
    global_context->setUserScriptsPath(user_scripts_path);

    /// top_level_domains_lists
    const std::string & top_level_domains_path = getClientConfiguration().getString("top_level_domains_path", fs::path(path) / "top_level_domains/");
    if (!top_level_domains_path.empty())
        TLDListsHolder::getInstance().parseConfig(fs::path(top_level_domains_path) / "", getClientConfiguration());
}


void LocalServer::cleanup()
{
    try
    {
        connection.reset();

        /// Suggestions are loaded async in a separate thread and it can use global context.
        /// We should reset it before resetting global_context.
        if (suggest)
            suggest.reset();

        client_context.reset();

        if (global_context)
        {
            global_context->shutdown();
            global_context.reset();
        }

        /// thread status should be destructed before shared context because it relies on process list.

        status.reset();

        // Delete the temporary directory if needed.
        if (temporary_directory_to_delete)
        {
            LOG_DEBUG(&logger(), "Removing temporary directory: {}", temporary_directory_to_delete->string());
            fs::remove_all(*temporary_directory_to_delete);
            temporary_directory_to_delete.reset();
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


std::string LocalServer::getInitialCreateTableQuery()
{
    if (!getClientConfiguration().has("table-structure") && !getClientConfiguration().has("table-file") && !getClientConfiguration().has("table-data-format") && (!isRegularFile(STDIN_FILENO) || queries.empty()))
        return {};

    auto table_name = backQuoteIfNeed(getClientConfiguration().getString("table-name", "table"));
    auto table_structure = getClientConfiguration().getString("table-structure", "auto");

    String table_file;
    if (!getClientConfiguration().has("table-file") || getClientConfiguration().getString("table-file") == "-")
    {
        /// Use Unix tools stdin naming convention
        table_file = "stdin";
    }
    else
    {
        /// Use regular file
        auto file_name = getClientConfiguration().getString("table-file");
        table_file = quoteString(file_name);
    }

    String data_format = backQuoteIfNeed(default_input_format);

    if (table_structure == "auto")
        table_structure = "";
    else
        table_structure = "(" + table_structure + ")";

    return fmt::format("CREATE TABLE {} {} ENGINE = File({}, {});",
                       table_name, table_structure, data_format, table_file);
}


static ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}


void LocalServer::setupUsers()
{
    static const char * minimal_default_user_xml =
        "<clickhouse>"
        "    <profiles>"
        "        <default></default>"
        "    </profiles>"
        "    <users>"
        "        <default>"
        "            <password></password>"
        "            <networks>"
        "                <ip>::/0</ip>"
        "            </networks>"
        "            <profile>default</profile>"
        "            <quota>default</quota>"
        "            <named_collection_control>1</named_collection_control>"
        "        </default>"
        "    </users>"
        "    <quotas>"
        "        <default></default>"
        "    </quotas>"
        "</clickhouse>";

    ConfigurationPtr users_config;
    auto & access_control = global_context->getAccessControl();
    access_control.setNoPasswordAllowed(getClientConfiguration().getBool("allow_no_password", true));
    access_control.setPlaintextPasswordAllowed(getClientConfiguration().getBool("allow_plaintext_password", true));
    if (getClientConfiguration().has("config-file") || fs::exists("config.xml"))
    {
        String config_path = getClientConfiguration().getString("config-file", "");
        bool has_user_directories = getClientConfiguration().has("user_directories");
        const auto config_dir = fs::path{config_path}.remove_filename().string();
        String users_config_path = getClientConfiguration().getString("users_config", "");

        if (users_config_path.empty() && has_user_directories)
        {
            users_config_path = getClientConfiguration().getString("user_directories.users_xml.path");
            if (fs::path(users_config_path).is_relative() && fs::exists(fs::path(config_dir) / users_config_path))
                users_config_path = fs::path(config_dir) / users_config_path;
        }

        if (users_config_path.empty())
            users_config = getConfigurationFromXMLString(minimal_default_user_xml);
        else
        {
            ConfigProcessor config_processor(users_config_path);
            const auto loaded_config = config_processor.loadConfig();
            users_config = loaded_config.configuration;
        }
    }
    else
        users_config = getConfigurationFromXMLString(minimal_default_user_xml);
    if (users_config)
        global_context->setUsersConfig(users_config);
    else
        throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "Can't load config for users");
}

void LocalServer::connect()
{
    connection_parameters = ConnectionParameters(getClientConfiguration(), "localhost");

    /// This is needed for table function input(...).
    ReadBuffer * in;
    auto table_file = getClientConfiguration().getString("table-file", "-");
    if (table_file == "-" || table_file == "stdin")
    {
        in = &std_in;
    }
    else
    {
        input = std::make_unique<ReadBufferFromFile>(table_file);
        in = input.get();
    }
    connection = LocalConnection::createConnection(
        connection_parameters, client_context, in, need_render_progress, need_render_profile_events, server_display_name);
}


int LocalServer::main(const std::vector<std::string> & /*args*/)
try
{
    UseSSL use_ssl;
    thread_status.emplace();

    StackTrace::setShowAddresses(server_settings.show_addresses_in_stack_traces);

    setupSignalHandler();

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    /// Try to increase limit on number of open files.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur < rlim.rlim_max)
        {
            rlim.rlim_cur = getClientConfiguration().getUInt("max_open_files", static_cast<unsigned>(rlim.rlim_max));
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                std::cerr << fmt::format("Cannot set max number of file descriptors to {}. Try to specify max_open_files according to your system limits. error: {}", rlim.rlim_cur, errnoToString()) << '\n';
        }
    }

    is_interactive = stdin_is_a_tty
        && (getClientConfiguration().hasOption("interactive")
            || (queries.empty() && !getClientConfiguration().has("table-structure") && queries_files.empty() && !getClientConfiguration().has("table-file")));

    if (!is_interactive)
    {
        /// We will terminate process on error
        static KillingErrorHandler error_handler;
        Poco::ErrorHandler::set(&error_handler);
    }

    registerInterpreters();
    /// Don't initialize DateLUT
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerDatabases();
    registerStorages();
    registerDictionaries();
    registerDisks(/* global_skip_access_check= */ true);
    registerFormats();

    processConfig();

    SCOPE_EXIT({ cleanup(); });

    initTTYBuffer(toProgressOption(getClientConfiguration().getString("progress", "default")));
    ASTAlterCommand::setFormatAlterCommandsWithParentheses(true);

    /// try to load user defined executable functions, throw on error and die
    try
    {
        global_context->loadOrReloadUserDefinedExecutableFunctions(getClientConfiguration());
    }
    catch (...)
    {
        tryLogCurrentException(&logger(), "Caught exception while loading user defined executable functions.");
        throw;
    }

    /// Must be called after we stopped initializing the global context and changing its settings.
    /// After this point the global context must be stayed almost unchanged till shutdown,
    /// and all necessary changes must be made to the client context instead.
    createClientContext();

    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
        std::cerr << std::endl;
    }

    connect();

    String initial_query = getInitialCreateTableQuery();
    if (!initial_query.empty())
        processQueryText(initial_query);

#if defined(FUZZING_MODE)
    runLibFuzzer();
#else
    if (is_interactive && !delayed_interactive)
    {
        runInteractive();
    }
    else
    {
        runNonInteractive();

        if (delayed_interactive)
            runInteractive();
    }
#endif

    return Application::EXIT_OK;
}
catch (const DB::Exception & e)
{
    bool need_print_stack_trace = getClientConfiguration().getBool("stacktrace", false);
    std::cerr << getExceptionMessage(e, need_print_stack_trace, true) << std::endl;
    return e.code() ? e.code() : -1;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(false) << std::endl;
    return getCurrentExceptionCode();
}

void LocalServer::updateLoggerLevel(const String & logs_level)
{
    getClientConfiguration().setString("logger.level", logs_level);
    updateLevels(getClientConfiguration(), logger());
}

void LocalServer::processConfig()
{
    if (!queries.empty() && getClientConfiguration().has("queries-file"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Options '--query' and '--queries-file' cannot be specified at the same time");

    pager = getClientConfiguration().getString("pager", "");

    delayed_interactive = getClientConfiguration().has("interactive") && (!queries.empty() || getClientConfiguration().has("queries-file"));
    if (!is_interactive || delayed_interactive)
    {
        echo_queries = getClientConfiguration().hasOption("echo") || getClientConfiguration().hasOption("verbose");
        ignore_error = getClientConfiguration().getBool("ignore-error", false);
    }

    print_stack_trace = getClientConfiguration().getBool("stacktrace", false);
    const std::string clickhouse_dialect{"clickhouse"};
    load_suggestions = (is_interactive || delayed_interactive) && !getClientConfiguration().getBool("disable_suggestion", false)
        && getClientConfiguration().getString("dialect", clickhouse_dialect) == clickhouse_dialect;
    wait_for_suggestions_to_load = getClientConfiguration().getBool("wait_for_suggestions_to_load", false);

    auto logging = (getClientConfiguration().has("logger.console")
                    || getClientConfiguration().has("logger.level")
                    || getClientConfiguration().has("log-level")
                    || getClientConfiguration().has("send_logs_level")
                    || getClientConfiguration().has("logger.log"));

    auto level = getClientConfiguration().getString("log-level", "trace");

    if (getClientConfiguration().has("server_logs_file"))
    {
        auto poco_logs_level = Poco::Logger::parseLevel(level);
        Poco::Logger::root().setLevel(poco_logs_level);
        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter;
        Poco::AutoPtr<OwnFormattingChannel> log = new OwnFormattingChannel(pf, new Poco::SimpleFileChannel(server_logs_file));
        Poco::Logger::root().setChannel(log);
    }
    else
    {
        getClientConfiguration().setString("logger", "logger");
        auto log_level_default = logging ? level : "fatal";
        getClientConfiguration().setString("logger.level", getClientConfiguration().getString("log-level", getClientConfiguration().getString("send_logs_level", log_level_default)));
        buildLoggers(getClientConfiguration(), logger(), "clickhouse-local");
    }

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);

    tryInitPath();

    LoggerRawPtr log = &logger();

    /// Maybe useless
    if (getClientConfiguration().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(getClientConfiguration(), "macros", log));

    setDefaultFormatsAndCompressionFromConfiguration();

    /// Sets external authenticators config (LDAP, Kerberos).
    global_context->setExternalAuthenticatorsConfig(getClientConfiguration());

    setupUsers();

    /// Limit on total number of concurrently executing queries.
    /// There is no need for concurrent queries, override max_concurrent_queries.
    global_context->getProcessList().setMaxSize(0);

    const size_t physical_server_memory = getMemoryAmount();

    size_t max_server_memory_usage = server_settings.max_server_memory_usage;
    double max_server_memory_usage_to_ram_ratio = server_settings.max_server_memory_usage_to_ram_ratio;

    size_t default_max_server_memory_usage = static_cast<size_t>(physical_server_memory * max_server_memory_usage_to_ram_ratio);

    if (max_server_memory_usage == 0)
    {
        max_server_memory_usage = default_max_server_memory_usage;
        LOG_INFO(log, "Setting max_server_memory_usage was set to {}"
                      " ({} available * {:.2f} max_server_memory_usage_to_ram_ratio)",
                 formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                 formatReadableSizeWithBinarySuffix(physical_server_memory),
                 max_server_memory_usage_to_ram_ratio);
    }
    else if (max_server_memory_usage > default_max_server_memory_usage)
    {
        max_server_memory_usage = default_max_server_memory_usage;
        LOG_INFO(log, "Setting max_server_memory_usage was lowered to {}"
                      " because the system has low amount of memory. The amount was"
                      " calculated as {} available"
                      " * {:.2f} max_server_memory_usage_to_ram_ratio",
                 formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                 formatReadableSizeWithBinarySuffix(physical_server_memory),
                 max_server_memory_usage_to_ram_ratio);
    }

    total_memory_tracker.setHardLimit(max_server_memory_usage);
    total_memory_tracker.setDescription("(total)");
    total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);

    const double cache_size_to_ram_max_ratio = server_settings.cache_size_to_ram_max_ratio;
    const size_t max_cache_size = static_cast<size_t>(physical_server_memory * cache_size_to_ram_max_ratio);

    String uncompressed_cache_policy = server_settings.uncompressed_cache_policy;
    size_t uncompressed_cache_size = server_settings.uncompressed_cache_size;
    double uncompressed_cache_size_ratio = server_settings.uncompressed_cache_size_ratio;
    if (uncompressed_cache_size > max_cache_size)
    {
        uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered uncompressed cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setUncompressedCache(uncompressed_cache_policy, uncompressed_cache_size, uncompressed_cache_size_ratio);

    String mark_cache_policy = server_settings.mark_cache_policy;
    size_t mark_cache_size = server_settings.mark_cache_size;
    double mark_cache_size_ratio = server_settings.mark_cache_size_ratio;
    if (!mark_cache_size)
        LOG_ERROR(log, "Too low mark cache size will lead to severe performance degradation.");
    if (mark_cache_size > max_cache_size)
    {
        mark_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered mark cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(mark_cache_size));
    }
    global_context->setMarkCache(mark_cache_policy, mark_cache_size, mark_cache_size_ratio);

    String index_uncompressed_cache_policy = server_settings.index_uncompressed_cache_policy;
    size_t index_uncompressed_cache_size = server_settings.index_uncompressed_cache_size;
    double index_uncompressed_cache_size_ratio = server_settings.index_uncompressed_cache_size_ratio;
    if (index_uncompressed_cache_size > max_cache_size)
    {
        index_uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered index uncompressed cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setIndexUncompressedCache(index_uncompressed_cache_policy, index_uncompressed_cache_size, index_uncompressed_cache_size_ratio);

    String index_mark_cache_policy = server_settings.index_mark_cache_policy;
    size_t index_mark_cache_size = server_settings.index_mark_cache_size;
    double index_mark_cache_size_ratio = server_settings.index_mark_cache_size_ratio;
    if (index_mark_cache_size > max_cache_size)
    {
        index_mark_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered index mark cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setIndexMarkCache(index_mark_cache_policy, index_mark_cache_size, index_mark_cache_size_ratio);

    size_t mmap_cache_size = server_settings.mmap_cache_size;
    if (mmap_cache_size > max_cache_size)
    {
        mmap_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered mmap file cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setMMappedFileCache(mmap_cache_size);

    /// Initialize a dummy query cache.
    global_context->setQueryCache(0, 0, 0, 0);

#if USE_EMBEDDED_COMPILER
    size_t compiled_expression_cache_max_size_in_bytes = server_settings.compiled_expression_cache_size;
    size_t compiled_expression_cache_max_elements = server_settings.compiled_expression_cache_elements_size;
    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_max_size_in_bytes, compiled_expression_cache_max_elements);
#endif

    /// NOTE: it is important to apply any overrides before
    /// setDefaultProfiles() calls since it will copy current context (i.e.
    /// there is separate context for Buffer tables).
    adjustSettings();
    applySettingsOverridesForLocal(global_context);
    applyCmdOptions(global_context);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(getClientConfiguration());

    /// Command-line parameters can override settings from the default profile.
    applyCmdSettings(global_context);

    /// We load temporary database first, because projections need it.
    DatabaseCatalog::instance().initializeAndLoadTemporaryDatabase();

    std::string default_database = server_settings.default_database;
    DatabaseCatalog::instance().attachDatabase(default_database, createClickHouseLocalDatabaseOverlay(default_database, global_context));
    global_context->setCurrentDatabase(default_database);

    if (getClientConfiguration().has("path"))
    {
        String path = global_context->getPath();
        fs::create_directories(fs::path(path));

        /// Lock path directory before read
        status.emplace(fs::path(path) / "status", StatusFile::write_full_info);

        LOG_DEBUG(log, "Loading metadata from {}", path);
        auto startup_system_tasks = loadMetadataSystem(global_context);
        attachSystemTablesServer(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE), false);
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
        waitLoad(TablesLoaderForegroundPoolId, startup_system_tasks);

        if (!getClientConfiguration().has("only-system-tables"))
        {
            DatabaseCatalog::instance().createBackgroundTasks();
            waitLoad(loadMetadata(global_context));
            DatabaseCatalog::instance().startupBackgroundTasks();
        }

        /// For ClickHouse local if path is not set the loader will be disabled.
        global_context->getUserDefinedSQLObjectsStorage().loadObjects();

        LOG_DEBUG(log, "Loaded metadata.");
    }
    else if (!getClientConfiguration().has("no-system-tables"))
    {
        attachSystemTablesServer(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE), false);
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
    }

    server_display_name = getClientConfiguration().getString("display_name", "");
    prompt_by_server_display_name = getClientConfiguration().getRawString("prompt_by_server_display_name.default", ":) ");
}


[[ maybe_unused ]] static std::string getHelpHeader()
{
    return
        "usage: clickhouse-local [initial table definition] [--query <query>]\n"

        "clickhouse-local allows to execute SQL queries on your data files via single command line call."
        " To do so, initially you need to define your data source and its format."
        " After you can execute your SQL queries in usual manner.\n"

        "There are two ways to define initial table keeping your data."
        " Either just in first query like this:\n"
        "    CREATE TABLE <table> (<structure>) ENGINE = File(<input-format>, <file>);\n"
        "Either through corresponding command line parameters --table --structure --input-format and --file.";
}


[[ maybe_unused ]] static std::string getHelpFooter()
{
    return
        "Example printing memory used by each Unix user:\n"
        "ps aux | tail -n +2 | awk '{ printf(\"%s\\t%s\\n\", $1, $4) }' | "
        "clickhouse-local -S \"user String, mem Float64\" -q"
            " \"SELECT user, round(sum(mem), 2) as mem_total FROM table GROUP BY user ORDER"
            " BY mem_total DESC FORMAT PrettyCompact\"";
}


void LocalServer::printHelpMessage(const OptionsDescription & options_description, bool verbose)
{
    std::cout << getHelpHeader() << "\n";
    std::cout << options_description.main_description.value() << "\n";
    if (verbose)
        std::cout << "All settings are documented at https://clickhouse.com/docs/en/operations/settings/settings.\n\n";
    std::cout << getHelpFooter() << "\n";
    std::cout << "In addition, --param_name=value can be specified for substitution of parameters for parametrized queries.\n";
    std::cout << "\nSee also: https://clickhouse.com/docs/en/operations/utilities/clickhouse-local/\n";
}


void LocalServer::addOptions(OptionsDescription & options_description)
{
    options_description.main_description->add_options()
        ("table,N", po::value<std::string>(), "name of the initial table")

        /// If structure argument is omitted then initial query is not generated
        ("structure,S", po::value<std::string>(), "structure of the initial table (list of column and type names)")
        ("file,F", po::value<std::string>(), "path to file with data of the initial table (stdin if not specified)")

        ("input-format", po::value<std::string>(), "input format of the initial table data")

        ("logger.console", po::value<bool>()->implicit_value(true), "Log to console")
        ("logger.log", po::value<std::string>(), "Log file name")
        ("logger.level", po::value<std::string>(), "Log level")

        ("no-system-tables", "do not attach system tables (better startup time)")
        ("path", po::value<std::string>(), "Storage path")
        ("only-system-tables", "attach only system tables from specified path")
        ("top_level_domains_path", po::value<std::string>(), "Path to lists with custom TLDs")
        ;
}


void LocalServer::applyCmdSettings(ContextMutablePtr context)
{
    context->applySettingsChanges(cmd_settings.changes());
}


void LocalServer::applyCmdOptions(ContextMutablePtr context)
{
    context->setDefaultFormat(getClientConfiguration().getString("output-format", getClientConfiguration().getString("format", is_interactive ? "PrettyCompact" : "TSV")));
    applyCmdSettings(context);
}


void LocalServer::createClientContext()
{
    /// In case of clickhouse-local it's necessary to use a separate context for client-related purposes.
    /// We can't just change the global context because it is used in background tasks (for example, in merges)
    /// which don't expect that the global context can suddenly change.
    client_context = Context::createCopy(global_context);
    initClientContext();
}


void LocalServer::processOptions(const OptionsDescription &, const CommandLineOptions & options, const std::vector<Arguments> &, const std::vector<Arguments> &)
{
    if (options.count("table"))
        getClientConfiguration().setString("table-name", options["table"].as<std::string>());
    if (options.count("file"))
        getClientConfiguration().setString("table-file", options["file"].as<std::string>());
    if (options.count("structure"))
        getClientConfiguration().setString("table-structure", options["structure"].as<std::string>());
    if (options.count("no-system-tables"))
        getClientConfiguration().setBool("no-system-tables", true);
    if (options.count("only-system-tables"))
        getClientConfiguration().setBool("only-system-tables", true);
    if (options.count("database"))
        getClientConfiguration().setString("default_database", options["database"].as<std::string>());

    if (options.count("input-format"))
        getClientConfiguration().setString("table-data-format", options["input-format"].as<std::string>());
    if (options.count("output-format"))
        getClientConfiguration().setString("output-format", options["output-format"].as<std::string>());

    if (options.count("logger.console"))
        getClientConfiguration().setBool("logger.console", options["logger.console"].as<bool>());
    if (options.count("logger.log"))
        getClientConfiguration().setString("logger.log", options["logger.log"].as<std::string>());
    if (options.count("logger.level"))
        getClientConfiguration().setString("logger.level", options["logger.level"].as<std::string>());
    if (options.count("send_logs_level"))
        getClientConfiguration().setString("send_logs_level", options["send_logs_level"].as<std::string>());
    if (options.count("wait_for_suggestions_to_load"))
        getClientConfiguration().setBool("wait_for_suggestions_to_load", true);
}

void LocalServer::readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &, std::vector<Arguments> &)
{
    for (int arg_num = 1; arg_num < argc; ++arg_num)
    {
        std::string_view arg = argv[arg_num];

        /// Parameter arg after underline.
        if (arg.starts_with("--param_"))
        {
            auto param_continuation = arg.substr(strlen("--param_"));
            auto equal_pos = param_continuation.find_first_of('=');

            if (equal_pos == std::string::npos)
            {
                /// param_name value
                ++arg_num;
                if (arg_num >= argc)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter requires value");
                arg = argv[arg_num];
                query_parameters.emplace(String(param_continuation), String(arg));
            }
            else
            {
                if (equal_pos == 0)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter name cannot be empty");

                /// param_name=value
                query_parameters.emplace(param_continuation.substr(0, equal_pos), param_continuation.substr(equal_pos + 1));
            }
        }
        else
        {
            common_arguments.emplace_back(arg);
        }
    }
}

}

#pragma clang diagnostic ignored "-Wunused-function"
#pragma clang diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseLocal(int argc, char ** argv)
{
    try
    {
        DB::LocalServer app;
        app.init(argc, argv);
        return app.run();
    }
    catch (const DB::Exception & e)
    {
        std::cerr << DB::getExceptionMessage(e, false) << std::endl;
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return DB::ErrorCodes::BAD_ARGUMENTS;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
