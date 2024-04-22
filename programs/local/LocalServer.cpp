#include "LocalServer.h"

#include <sys/resource.h>
#include <Common/logger_useful.h>
#include <Common/formatReadable.h>
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
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>
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
#include <Formats/FormatFactory.h>
#include <boost/algorithm/string/replace.hpp>
#include <boost/program_options/options_description.hpp>
#include <base/argsToConfig.h>
#include <filesystem>

#include "config.h"

#if USE_AZURE_BLOB_STORAGE
#   include <azure/storage/common/internal/xml_wrapper.hpp>
#endif

namespace fs = std::filesystem;


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
    Settings settings = context->getSettings();

    settings.allow_introspection_functions = true;
    settings.storage_file_read_method = LocalFSReadMethod::mmap;

    context->setSettings(settings);
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

    /// Load config files if exists
    if (config().has("config-file") || fs::exists("config.xml"))
    {
        const auto config_path = config().getString("config-file", "config.xml");
        ConfigProcessor config_processor(config_path, false, true);
        ConfigProcessor::setConfigPath(fs::path(config_path).parent_path());
        auto loaded_config = config_processor.loadConfig();
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    GlobalThreadPool::initialize(
        config().getUInt("max_thread_pool_size", 10000),
        config().getUInt("max_thread_pool_free_size", 1000),
        config().getUInt("thread_pool_queue_size", 10000)
    );

#if USE_AZURE_BLOB_STORAGE
    /// See the explanation near the same line in Server.cpp
    GlobalThreadPool::instance().addOnDestroyCallback([]
    {
        Azure::Storage::_internal::XmlGlobalDeinitialize();
    });
#endif

    getIOThreadPool().initialize(
        config().getUInt("max_io_thread_pool_size", 100),
        config().getUInt("max_io_thread_pool_free_size", 0),
        config().getUInt("io_thread_pool_queue_size", 10000));


    const size_t active_parts_loading_threads = config().getUInt("max_active_parts_loading_thread_pool_size", 64);
    getActivePartsLoadingThreadPool().initialize(
        active_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        active_parts_loading_threads);

    const size_t outdated_parts_loading_threads = config().getUInt("max_outdated_parts_loading_thread_pool_size", 32);
    getOutdatedPartsLoadingThreadPool().initialize(
        outdated_parts_loading_threads,
        0, // We don't need any threads one all the parts will be loaded
        outdated_parts_loading_threads);

    getOutdatedPartsLoadingThreadPool().setMaxTurboThreads(active_parts_loading_threads);

    const size_t cleanup_threads = config().getUInt("max_parts_cleaning_thread_pool_size", 128);
    getPartsCleaningThreadPool().initialize(
        cleanup_threads,
        0, // We don't need any threads one all the parts will be deleted
        cleanup_threads);
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

    if (config().has("path"))
    {
        // User-supplied path.
        path = config().getString("path");
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
        // (or in the current dir if temporary don't exist)
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

        default_path = parent_folder / fmt::format("clickhouse-local-{}-{}-{}", getpid(), time(nullptr), randomSeed());

        if (exists(default_path))
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Unsuccessful attempt to create working directory: {} already exists.", default_path.string());

        create_directory(default_path);
        temporary_directory_to_delete = default_path;

        path = default_path.string();
        LOG_DEBUG(log, "Working directory created: {}", path);
    }

    if (path.back() != '/')
        path += '/';

    fs::create_directories(fs::path(path) / "user_defined/");
    fs::create_directories(fs::path(path) / "data/");
    fs::create_directories(fs::path(path) / "metadata/");
    fs::create_directories(fs::path(path) / "metadata_dropped/");

    global_context->setPath(path);

    global_context->setTemporaryStoragePath(path + "tmp/", 0);
    global_context->setFlagsPath(path + "flags");

    global_context->setUserFilesPath(""); // user's files are everywhere

    std::string user_scripts_path = config().getString("user_scripts_path", fs::path(path) / "user_scripts/");
    global_context->setUserScriptsPath(user_scripts_path);
    fs::create_directories(user_scripts_path);

    /// top_level_domains_lists
    const std::string & top_level_domains_path = config().getString("top_level_domains_path", path + "top_level_domains/");
    if (!top_level_domains_path.empty())
        TLDListsHolder::getInstance().parseConfig(fs::path(top_level_domains_path) / "", config());
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
            const auto dir = *temporary_directory_to_delete;
            temporary_directory_to_delete.reset();
            LOG_DEBUG(&logger(), "Removing temporary directory: {}", dir.string());
            remove_all(dir);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


std::string LocalServer::getInitialCreateTableQuery()
{
    if (!config().has("table-structure") && !config().has("table-file") && !config().has("table-data-format") && (!isRegularFile(STDIN_FILENO) || queries.empty()))
        return {};

    auto table_name = backQuoteIfNeed(config().getString("table-name", "table"));
    auto table_structure = config().getString("table-structure", "auto");

    String table_file;
    if (!config().has("table-file") || config().getString("table-file") == "-")
    {
        /// Use Unix tools stdin naming convention
        table_file = "stdin";
    }
    else
    {
        /// Use regular file
        auto file_name = config().getString("table-file");
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
        "        </default>"
        "    </users>"
        "    <quotas>"
        "        <default></default>"
        "    </quotas>"
        "</clickhouse>";

    ConfigurationPtr users_config;
    auto & access_control = global_context->getAccessControl();
    access_control.setNoPasswordAllowed(config().getBool("allow_no_password", true));
    access_control.setPlaintextPasswordAllowed(config().getBool("allow_plaintext_password", true));
    if (config().has("config-file") || fs::exists("config.xml"))
    {
        String config_path = config().getString("config-file", "");
        bool has_user_directories = config().has("user_directories");
        const auto config_dir = fs::path{config_path}.remove_filename().string();
        String users_config_path = config().getString("users_config", "");

        if (users_config_path.empty() && has_user_directories)
        {
            users_config_path = config().getString("user_directories.users_xml.path");
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
    connection_parameters = ConnectionParameters(config(), "localhost");

    ReadBuffer * in;
    auto table_file = config().getString("table-file", "-");
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
        connection_parameters, global_context, in, need_render_progress, need_render_profile_events, server_display_name);
}


int LocalServer::main(const std::vector<std::string> & /*args*/)
try
{
    UseSSL use_ssl;
    thread_status.emplace();

    StackTrace::setShowAddresses(config().getBool("show_addresses_in_stack_traces", true));

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
            rlim.rlim_cur = config().getUInt("max_open_files", static_cast<unsigned>(rlim.rlim_max));
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                std::cerr << fmt::format("Cannot set max number of file descriptors to {}. Try to specify max_open_files according to your system limits. error: {}", rlim.rlim_cur, errnoToString()) << '\n';
        }
    }

    is_interactive = stdin_is_a_tty
        && (config().hasOption("interactive")
            || (queries.empty() && !config().has("table-structure") && queries_files.empty() && !config().has("table-file")));

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
    adjustSettings();
    initTTYBuffer(toProgressOption(config().getString("progress", "default")));
    ASTAlterCommand::setFormatAlterCommandsWithParentheses(true);

    applyCmdSettings(global_context);

    /// try to load user defined executable functions, throw on error and die
    try
    {
        global_context->loadOrReloadUserDefinedExecutableFunctions(config());
    }
    catch (...)
    {
        tryLogCurrentException(&logger(), "Caught exception while loading user defined executable functions.");
        throw;
    }

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
    cleanup();

    bool need_print_stack_trace = config().getBool("stacktrace", false);
    std::cerr << getExceptionMessage(e, need_print_stack_trace, true) << std::endl;
    return e.code() ? e.code() : -1;
}
catch (...)
{
    cleanup();

    std::cerr << getCurrentExceptionMessage(false) << std::endl;
    return getCurrentExceptionCode();
}

void LocalServer::updateLoggerLevel(const String & logs_level)
{
    config().setString("logger.level", logs_level);
    updateLevels(config(), logger());
}

void LocalServer::processConfig()
{
    if (!queries.empty() && config().has("queries-file"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Options '--query' and '--queries-file' cannot be specified at the same time");

    if (config().has("multiquery"))
        is_multiquery = true;

    pager = config().getString("pager", "");

    delayed_interactive = config().has("interactive") && (!queries.empty() || config().has("queries-file"));
    if (!is_interactive || delayed_interactive)
    {
        echo_queries = config().hasOption("echo") || config().hasOption("verbose");
        ignore_error = config().getBool("ignore-error", false);
    }

    print_stack_trace = config().getBool("stacktrace", false);
    const std::string clickhouse_dialect{"clickhouse"};
    load_suggestions = (is_interactive || delayed_interactive) && !config().getBool("disable_suggestion", false)
        && config().getString("dialect", clickhouse_dialect) == clickhouse_dialect;
    wait_for_suggestions_to_load = config().getBool("wait_for_suggestions_to_load", false);

    auto logging = (config().has("logger.console")
                    || config().has("logger.level")
                    || config().has("log-level")
                    || config().has("send_logs_level")
                    || config().has("logger.log"));

    auto level = config().getString("log-level", "trace");

    if (config().has("server_logs_file"))
    {
        auto poco_logs_level = Poco::Logger::parseLevel(level);
        Poco::Logger::root().setLevel(poco_logs_level);
        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter;
        Poco::AutoPtr<OwnFormattingChannel> log = new OwnFormattingChannel(pf, new Poco::SimpleFileChannel(server_logs_file));
        Poco::Logger::root().setChannel(log);
    }
    else
    {
        config().setString("logger", "logger");
        auto log_level_default = logging ? level : "fatal";
        config().setString("logger.level", config().getString("log-level", config().getString("send_logs_level", log_level_default)));
        buildLoggers(config(), logger(), "clickhouse-local");
    }

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);

    tryInitPath();

    LoggerRawPtr log = &logger();

    /// Maybe useless
    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    setDefaultFormatsFromConfiguration();

    /// Sets external authenticators config (LDAP, Kerberos).
    global_context->setExternalAuthenticatorsConfig(config());

    setupUsers();

    /// Limit on total number of concurrently executing queries.
    /// There is no need for concurrent queries, override max_concurrent_queries.
    global_context->getProcessList().setMaxSize(0);

    const size_t physical_server_memory = getMemoryAmount();
    const double cache_size_to_ram_max_ratio = config().getDouble("cache_size_to_ram_max_ratio", 0.5);
    const size_t max_cache_size = static_cast<size_t>(physical_server_memory * cache_size_to_ram_max_ratio);

    String uncompressed_cache_policy = config().getString("uncompressed_cache_policy", DEFAULT_UNCOMPRESSED_CACHE_POLICY);
    size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", DEFAULT_UNCOMPRESSED_CACHE_MAX_SIZE);
    double uncompressed_cache_size_ratio = config().getDouble("uncompressed_cache_size_ratio", DEFAULT_UNCOMPRESSED_CACHE_SIZE_RATIO);
    if (uncompressed_cache_size > max_cache_size)
    {
        uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered uncompressed cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setUncompressedCache(uncompressed_cache_policy, uncompressed_cache_size, uncompressed_cache_size_ratio);

    String mark_cache_policy = config().getString("mark_cache_policy", DEFAULT_MARK_CACHE_POLICY);
    size_t mark_cache_size = config().getUInt64("mark_cache_size", DEFAULT_MARK_CACHE_MAX_SIZE);
    double mark_cache_size_ratio = config().getDouble("mark_cache_size_ratio", DEFAULT_MARK_CACHE_SIZE_RATIO);
    if (!mark_cache_size)
        LOG_ERROR(log, "Too low mark cache size will lead to severe performance degradation.");
    if (mark_cache_size > max_cache_size)
    {
        mark_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered mark cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(mark_cache_size));
    }
    global_context->setMarkCache(mark_cache_policy, mark_cache_size, mark_cache_size_ratio);

    String index_uncompressed_cache_policy = config().getString("index_uncompressed_cache_policy", DEFAULT_INDEX_UNCOMPRESSED_CACHE_POLICY);
    size_t index_uncompressed_cache_size = config().getUInt64("index_uncompressed_cache_size", DEFAULT_INDEX_UNCOMPRESSED_CACHE_MAX_SIZE);
    double index_uncompressed_cache_size_ratio = config().getDouble("index_uncompressed_cache_size_ratio", DEFAULT_INDEX_UNCOMPRESSED_CACHE_SIZE_RATIO);
    if (index_uncompressed_cache_size > max_cache_size)
    {
        index_uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered index uncompressed cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setIndexUncompressedCache(index_uncompressed_cache_policy, index_uncompressed_cache_size, index_uncompressed_cache_size_ratio);

    String index_mark_cache_policy = config().getString("index_mark_cache_policy", DEFAULT_INDEX_MARK_CACHE_POLICY);
    size_t index_mark_cache_size = config().getUInt64("index_mark_cache_size", DEFAULT_INDEX_MARK_CACHE_MAX_SIZE);
    double index_mark_cache_size_ratio = config().getDouble("index_mark_cache_size_ratio", DEFAULT_INDEX_MARK_CACHE_SIZE_RATIO);
    if (index_mark_cache_size > max_cache_size)
    {
        index_mark_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered index mark cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setIndexMarkCache(index_mark_cache_policy, index_mark_cache_size, index_mark_cache_size_ratio);

    size_t mmap_cache_size = config().getUInt64("mmap_cache_size", DEFAULT_MMAP_CACHE_MAX_SIZE);
    if (mmap_cache_size > max_cache_size)
    {
        mmap_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered mmap file cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setMMappedFileCache(mmap_cache_size);

    /// Initialize a dummy query cache.
    global_context->setQueryCache(0, 0, 0, 0);

#if USE_EMBEDDED_COMPILER
    size_t compiled_expression_cache_max_size_in_bytes = config().getUInt64("compiled_expression_cache_size", DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_SIZE);
    size_t compiled_expression_cache_max_elements = config().getUInt64("compiled_expression_cache_elements_size", DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_ENTRIES);
    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_max_size_in_bytes, compiled_expression_cache_max_elements);
#endif

    /// NOTE: it is important to apply any overrides before
    /// setDefaultProfiles() calls since it will copy current context (i.e.
    /// there is separate context for Buffer tables).
    applySettingsOverridesForLocal(global_context);
    applyCmdOptions(global_context);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());

    /// We load temporary database first, because projections need it.
    DatabaseCatalog::instance().initializeAndLoadTemporaryDatabase();

    std::string default_database = config().getString("default_database", "default");
    DatabaseCatalog::instance().attachDatabase(default_database, createClickHouseLocalDatabaseOverlay(default_database, global_context));
    global_context->setCurrentDatabase(default_database);

    if (config().has("path"))
    {
        String path = global_context->getPath();

        /// Lock path directory before read
        status.emplace(fs::path(path) / "status", StatusFile::write_full_info);

        LOG_DEBUG(log, "Loading metadata from {}", path);
        auto startup_system_tasks = loadMetadataSystem(global_context);
        attachSystemTablesServer(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE), false);
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
        waitLoad(TablesLoaderForegroundPoolId, startup_system_tasks);

        if (!config().has("only-system-tables"))
        {
            DatabaseCatalog::instance().createBackgroundTasks();
            waitLoad(loadMetadata(global_context));
            DatabaseCatalog::instance().startupBackgroundTasks();
        }

        /// For ClickHouse local if path is not set the loader will be disabled.
        global_context->getUserDefinedSQLObjectsStorage().loadObjects();

        LOG_DEBUG(log, "Loaded metadata.");
    }
    else if (!config().has("no-system-tables"))
    {
        attachSystemTablesServer(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE), false);
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
    }

    server_display_name = config().getString("display_name", getFQDNOrHostName());
    prompt_by_server_display_name = config().getRawString("prompt_by_server_display_name.default", "{display_name} :) ");
    std::map<String, String> prompt_substitutions{{"display_name", server_display_name}};
    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);

    global_context->setQueryKindInitial();
    global_context->setQueryKind(query_kind);
    global_context->setQueryParameters(query_parameters);
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


void LocalServer::printHelpMessage([[maybe_unused]] const OptionsDescription & options_description)
{
    std::cout << getHelpHeader() << "\n";
    std::cout << options_description.main_description.value() << "\n";
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
    context->setDefaultFormat(config().getString("output-format", config().getString("format", is_interactive ? "PrettyCompact" : "TSV")));
    applyCmdSettings(context);
}


void LocalServer::processOptions(const OptionsDescription &, const CommandLineOptions & options, const std::vector<Arguments> &, const std::vector<Arguments> &)
{
    if (options.count("table"))
        config().setString("table-name", options["table"].as<std::string>());
    if (options.count("file"))
        config().setString("table-file", options["file"].as<std::string>());
    if (options.count("structure"))
        config().setString("table-structure", options["structure"].as<std::string>());
    if (options.count("no-system-tables"))
        config().setBool("no-system-tables", true);
    if (options.count("only-system-tables"))
        config().setBool("only-system-tables", true);
    if (options.count("database"))
        config().setString("default_database", options["database"].as<std::string>());

    if (options.count("input-format"))
        config().setString("table-data-format", options["input-format"].as<std::string>());
    if (options.count("output-format"))
        config().setString("output-format", options["output-format"].as<std::string>());

    if (options.count("logger.console"))
        config().setBool("logger.console", options["logger.console"].as<bool>());
    if (options.count("logger.log"))
        config().setString("logger.log", options["logger.log"].as<std::string>());
    if (options.count("logger.level"))
        config().setString("logger.level", options["logger.level"].as<std::string>());
    if (options.count("send_logs_level"))
        config().setString("send_logs_level", options["send_logs_level"].as<std::string>());
    if (options.count("wait_for_suggestions_to_load"))
        config().setBool("wait_for_suggestions_to_load", true);
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
        else if (arg == "--multiquery" && (arg_num + 1) < argc && !std::string_view(argv[arg_num + 1]).starts_with('-'))
        {
            /// Transform the abbreviated syntax '--multiquery <SQL>' into the full syntax '--multiquery -q <SQL>'
            ++arg_num;
            arg = argv[arg_num];
            addMultiquery(arg, common_arguments);
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
