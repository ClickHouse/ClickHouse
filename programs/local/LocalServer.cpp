#include "LocalServer.h"

#include <Poco/Util/XMLConfiguration.h>
#include <Poco/String.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Poco/SimpleFileChannel.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/DatabaseCatalog.h>
#include <base/getFQDNOrHostName.h>
#include <Common/scope_guard_safe.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/Session.h>
#include <Access/AccessControl.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/ThreadStatus.h>
#include <Common/TLDListsHolder.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <Loggers/Loggers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/UseSSL.h>
#include <IO/IOThreadPool.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Common/ErrorHandlers.h>
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

#if defined(FUZZING_MODE)
    #include <Functions/getFuzzerData.h>
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
        config_processor.setConfigPath(fs::path(config_path).parent_path());
        auto loaded_config = config_processor.loadConfig();
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    GlobalThreadPool::initialize(
        config().getUInt("max_thread_pool_size", 10000),
        config().getUInt("max_thread_pool_free_size", 1000),
        config().getUInt("thread_pool_queue_size", 10000)
    );

    IOThreadPool::initialize(
        config().getUInt("max_io_thread_pool_size", 100),
        config().getUInt("max_io_thread_pool_free_size", 0),
        config().getUInt("io_thread_pool_queue_size", 10000));
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
        Poco::Logger * log = &logger();
        std::filesystem::path parent_folder;
        std::filesystem::path default_path;

        try
        {
            // try to guess a tmp folder name, and check if it's a directory (throw exception otherwise)
            parent_folder = std::filesystem::temp_directory_path();

        }
        catch (const fs::filesystem_error& e)
        {
            // tmp folder don't exists? misconfiguration? chroot?
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
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Unsuccessful attempt to create working directory: {} exist!", default_path.string());

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

    global_context->setTemporaryStorage(path + "tmp");
    global_context->setFlagsPath(path + "flags");

    global_context->setUserFilesPath(""); // user's files are everywhere

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

        if (global_context)
        {
            global_context->shutdown();
            global_context.reset();
        }

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


static bool checkIfStdinIsRegularFile()
{
    struct stat file_stat;
    return fstat(STDIN_FILENO, &file_stat) == 0 && S_ISREG(file_stat.st_mode);
}

std::string LocalServer::getInitialCreateTableQuery()
{
    if (!config().has("table-structure") && !config().has("table-file") && !config().has("table-data-format") && (!checkIfStdinIsRegularFile() || !config().has("query")))
        return {};

    auto table_name = backQuoteIfNeed(config().getString("table-name", "table"));
    auto table_structure = config().getString("table-structure", "auto");

    String table_file;
    String format_from_file_name;
    if (!config().has("table-file") || config().getString("table-file") == "-")
    {
        /// Use Unix tools stdin naming convention
        table_file = "stdin";
        format_from_file_name = FormatFactory::instance().getFormatFromFileDescriptor(STDIN_FILENO);
    }
    else
    {
        /// Use regular file
        auto file_name = config().getString("table-file");
        table_file = quoteString(file_name);
        format_from_file_name = FormatFactory::instance().getFormatFromFileName(file_name, false);
    }

    auto data_format = backQuoteIfNeed(
        config().getString("table-data-format", config().getString("format", format_from_file_name.empty() ? "TSV" : format_from_file_name)));


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
    if (config().has("users_config") || config().has("config-file") || fs::exists("config.xml"))
    {
        const auto users_config_path = config().getString("users_config", config().getString("config-file", "config.xml"));
        ConfigProcessor config_processor(users_config_path);
        const auto loaded_config = config_processor.loadConfig();
        users_config = loaded_config.configuration;
    }
    else
        users_config = getConfigurationFromXMLString(minimal_default_user_xml);
    if (users_config)
        global_context->setUsersConfig(users_config);
    else
        throw Exception("Can't load config for users", ErrorCodes::CANNOT_LOAD_CONFIG);
}


void LocalServer::connect()
{
    connection_parameters = ConnectionParameters(config());
    connection = LocalConnection::createConnection(
        connection_parameters, global_context, need_render_progress, need_render_profile_events, server_display_name);
}


int LocalServer::main(const std::vector<std::string> & /*args*/)
try
{
    UseSSL use_ssl;
    ThreadStatus thread_status;
    setupSignalHandler();

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

#if defined(FUZZING_MODE)
    static bool first_time = true;
    if (first_time)
    {

    if (queries_files.empty() && !config().has("query"))
    {
        std::cerr << "\033[31m" << "ClickHouse compiled in fuzzing mode." << "\033[0m" << std::endl;
        std::cerr << "\033[31m" << "You have to provide a query with --query or --queries-file option." << "\033[0m" << std::endl;
        std::cerr << "\033[31m" << "The query have to use function getFuzzerData() inside." << "\033[0m" << std::endl;
        exit(1);
    }

    is_interactive = false;
#else
    is_interactive = stdin_is_a_tty
        && (config().hasOption("interactive")
            || (!config().has("query") && !config().has("table-structure") && queries_files.empty() && !config().has("table-file")));
#endif
    if (!is_interactive)
    {
        /// We will terminate process on error
        static KillingErrorHandler error_handler;
        Poco::ErrorHandler::set(&error_handler);
    }

    /// Don't initialize DateLUT
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();
    registerDictionaries();
    registerDisks();
    registerFormats();

    processConfig();
    applyCmdSettings(global_context);

    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
        std::cerr << std::endl;
    }

    connect();

#ifdef FUZZING_MODE
    first_time = false;
    }
#endif

    String initial_query = getInitialCreateTableQuery();
    if (!initial_query.empty())
        processQueryText(initial_query);

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

#ifndef FUZZING_MODE
    cleanup();
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
    if (!logging_initialized)
        return;

    config().setString("logger.level", logs_level);
    updateLevels(config(), logger());
}

void LocalServer::processConfig()
{
    delayed_interactive = config().has("interactive") && (config().has("query") || config().has("queries-file"));
    if (is_interactive && !delayed_interactive)
    {
        if (config().has("query") && config().has("queries-file"))
            throw Exception("Specify either `query` or `queries-file` option", ErrorCodes::BAD_ARGUMENTS);

        if (config().has("multiquery"))
            is_multiquery = true;
    }
    else
    {
        need_render_progress = config().getBool("progress", false);
        echo_queries = config().hasOption("echo") || config().hasOption("verbose");
        ignore_error = config().getBool("ignore-error", false);
        is_multiquery = true;
    }

    print_stack_trace = config().getBool("stacktrace", false);
    load_suggestions = (is_interactive || delayed_interactive) && !config().getBool("disable_suggestion", false);

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
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::SimpleFileChannel>(new Poco::SimpleFileChannel(server_logs_file)));
        logging_initialized = true;
    }
    else if (logging || is_interactive)
    {
        config().setString("logger", "logger");
        auto log_level_default = is_interactive && !logging ? "none" : level;
        config().setString("logger.level", config().getString("log-level", config().getString("send_logs_level", log_level_default)));
        buildLoggers(config(), logger(), "clickhouse-local");
        logging_initialized = true;
    }
    else
    {
        Poco::Logger::root().setLevel("none");
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
        logging_initialized = false;
    }

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);

    tryInitPath();

    Poco::Logger * log = &logger();

    /// Maybe useless
    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    format = config().getString("output-format", config().getString("format", is_interactive ? "PrettyCompact" : "TSV"));
    insert_format = "Values";

    /// Setting value from cmd arg overrides one from config
    if (global_context->getSettingsRef().max_insert_block_size.changed)
        insert_format_max_block_size = global_context->getSettingsRef().max_insert_block_size;
    else
        insert_format_max_block_size = config().getInt("insert_format_max_block_size", global_context->getSettingsRef().max_insert_block_size);

    /// Sets external authenticators config (LDAP, Kerberos).
    global_context->setExternalAuthenticatorsConfig(config());

    setupUsers();

    /// Limit on total number of concurrently executing queries.
    /// There is no need for concurrent queries, override max_concurrent_queries.
    global_context->getProcessList().setMaxSize(0);

    /// Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", 0);
    if (uncompressed_cache_size)
        global_context->setUncompressedCache(uncompressed_cache_size);

    /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
    /// Specify default value for mark_cache_size explicitly!
    size_t mark_cache_size = config().getUInt64("mark_cache_size", 5368709120);
    if (mark_cache_size)
        global_context->setMarkCache(mark_cache_size);

    /// Size of cache for uncompressed blocks of MergeTree indices. Zero means disabled.
    size_t index_uncompressed_cache_size = config().getUInt64("index_uncompressed_cache_size", 0);
    if (index_uncompressed_cache_size)
        global_context->setIndexUncompressedCache(index_uncompressed_cache_size);

    /// Size of cache for index marks (index of MergeTree skip indices). It is necessary.
    /// Specify default value for index_mark_cache_size explicitly!
    size_t index_mark_cache_size = config().getUInt64("index_mark_cache_size", 0);
    if (index_mark_cache_size)
        global_context->setIndexMarkCache(index_mark_cache_size);

    /// A cache for mmapped files.
    size_t mmap_cache_size = config().getUInt64("mmap_cache_size", 1000);   /// The choice of default is arbitrary.
    if (mmap_cache_size)
        global_context->setMMappedFileCache(mmap_cache_size);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());

    /// We load temporary database first, because projections need it.
    DatabaseCatalog::instance().initializeAndLoadTemporaryDatabase();

    /** Init dummy default DB
      * NOTE: We force using isolated default database to avoid conflicts with default database from server environment
      * Otherwise, metadata of temporary File(format, EXPLICIT_PATH) tables will pollute metadata/ directory;
      *  if such tables will not be dropped, clickhouse-server will not be able to load them due to security reasons.
      */
    std::string default_database = config().getString("default_database", "_local");
    DatabaseCatalog::instance().attachDatabase(default_database, std::make_shared<DatabaseMemory>(default_database, global_context));
    global_context->setCurrentDatabase(default_database);
    applyCmdOptions(global_context);

    bool enable_objects_loader = false;

    if (config().has("path"))
    {
        String path = global_context->getPath();

        /// Lock path directory before read
        status.emplace(fs::path(path) / "status", StatusFile::write_full_info);

        LOG_DEBUG(log, "Loading user defined objects from {}", path);
        Poco::File(path + "user_defined/").createDirectories();
        UserDefinedSQLObjectsLoader::instance().loadObjects(global_context);
        enable_objects_loader = true;
        LOG_DEBUG(log, "Loaded user defined objects.");

        LOG_DEBUG(log, "Loading metadata from {}", path);
        loadMetadataSystem(global_context);
        attachSystemTablesLocal(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
        loadMetadata(global_context);
        startupSystemTables();
        DatabaseCatalog::instance().loadDatabases();

        LOG_DEBUG(log, "Loaded metadata.");
    }
    else if (!config().has("no-system-tables"))
    {
        attachSystemTablesLocal(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
    }

    /// Persist SQL user defined objects only if user_defined folder was created
    UserDefinedSQLObjectsLoader::instance().enable(enable_objects_loader);

    server_display_name = config().getString("display_name", getFQDNOrHostName());
    prompt_by_server_display_name = config().getRawString("prompt_by_server_display_name.default", "{display_name} :) ");
    std::map<String, String> prompt_substitutions{{"display_name", server_display_name}};
    for (const auto & [key, value] : prompt_substitutions)
        boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);

    ClientInfo & client_info = global_context->getClientInfo();
    client_info.setInitialQuery();
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
#if defined(FUZZING_MODE)
    std::cout <<
        "usage: clickhouse <clickhouse-local arguments> -- <libfuzzer arguments>\n"
        "Note: It is important not to use only one letter keys with single dash for \n"
        "for clickhouse-local arguments. It may work incorrectly.\n"

        "ClickHouse is build with coverage guided fuzzer (libfuzzer) inside it.\n"
        "You have to provide a query which contains getFuzzerData function.\n"
        "This will take the data from fuzzing engine, pass it to getFuzzerData function and execute a query.\n"
        "Each time the data will be different, and it will last until some segfault or sanitizer assertion is found. \n";
#else
    std::cout << getHelpHeader() << "\n";
    std::cout << options_description.main_description.value() << "\n";
    std::cout << getHelpFooter() << "\n";
#endif
}


void LocalServer::addOptions(OptionsDescription & options_description)
{
    options_description.main_description->add_options()
        ("table,N", po::value<std::string>(), "name of the initial table")

        /// If structure argument is omitted then initial query is not generated
        ("structure,S", po::value<std::string>(), "structure of the initial table (list of column and type names)")
        ("file,f", po::value<std::string>(), "path to file with data of the initial table (stdin if not specified)")

        ("input-format", po::value<std::string>(), "input format of the initial table data")
        ("output-format", po::value<std::string>(), "default output format")

        ("logger.console", po::value<bool>()->implicit_value(true), "Log to console")
        ("logger.log", po::value<std::string>(), "Log file name")
        ("logger.level", po::value<std::string>(), "Log level")

        ("no-system-tables", "do not attach system tables (better startup time)")
        ("path", po::value<std::string>(), "Storage path")
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
}

void LocalServer::readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &, std::vector<Arguments> &)
{
    for (int arg_num = 1; arg_num < argc; ++arg_num)
    {
        const char * arg = argv[arg_num];
        common_arguments.emplace_back(arg);
    }
}

}

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

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

#if defined(FUZZING_MODE)

std::optional<DB::LocalServer> fuzz_app;

extern "C" int LLVMFuzzerInitialize(int * pargc, char *** pargv)
{
    int & argc = *pargc;
    char ** argv = *pargv;

    /// As a user you can add flags to clickhouse binary in fuzzing mode as follows
    /// clickhouse <set of clickhouse-local specific flag> -- <set of libfuzzer flags>

    /// Calculate the position of delimiter "--" that separates arguments
    /// of clickhouse-local and libfuzzer
    int pos_delim = argc;
    for (int i = 0; i < argc; ++i)
    {
        if (strcmp(argv[i], "--") == 0)
        {
            pos_delim = i;
            break;
        }
    }

    /// Initialize clickhouse-local app
    fuzz_app.emplace();
    fuzz_app->init(pos_delim, argv);

    /// We will leave clickhouse-local specific arguments as is, because libfuzzer will ignore
    /// all keys starting with --
    return 0;
}


extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
try
{
    auto input = String(reinterpret_cast<const char *>(data), size);
    DB::FunctionGetFuzzerData::update(input);
    fuzz_app->run();
    return 0;
}
catch (...)
{
    return 1;
}
#endif
