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
#include <base/scope_guard_safe.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/Session.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/ThreadStatus.h>
#include <Common/TLDListsHolder.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <loggers/Loggers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/UseSSL.h>
#include <Parsers/IAST.h>
#include <base/ErrorHandlers.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <boost/program_options/options_description.hpp>
#include <base/argsToConfig.h>
#include <filesystem>

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


bool LocalServer::executeMultiQuery(const String & all_queries_text)
{
    bool echo_query = echo_queries;

    /// Several queries separated by ';'.
    /// INSERT data is ended by the end of line, not ';'.
    /// An exception is VALUES format where we also support semicolon in
    /// addition to end of line.
    const char * this_query_begin = all_queries_text.data();
    const char * this_query_end;
    const char * all_queries_end = all_queries_text.data() + all_queries_text.size();

    String full_query; // full_query is the query + inline INSERT data + trailing comments (the latter is our best guess for now).
    String query_to_execute;
    ASTPtr parsed_query;
    std::optional<Exception> current_exception;

    while (true)
    {
        auto stage = analyzeMultiQueryText(this_query_begin, this_query_end, all_queries_end,
                                           query_to_execute, parsed_query, all_queries_text, current_exception);
        switch (stage)
        {
            case MultiQueryProcessingStage::QUERIES_END:
            case MultiQueryProcessingStage::PARSING_FAILED:
            {
                return true;
            }
            case MultiQueryProcessingStage::CONTINUE_PARSING:
            {
                continue;
            }
            case MultiQueryProcessingStage::PARSING_EXCEPTION:
            {
                if (current_exception)
                    current_exception->rethrow();
                return true;
            }
            case MultiQueryProcessingStage::EXECUTE_QUERY:
            {
                full_query = all_queries_text.substr(this_query_begin - all_queries_text.data(), this_query_end - this_query_begin);

                try
                {
                    processParsedSingleQuery(full_query, query_to_execute, parsed_query, echo_query, false);
                }
                catch (...)
                {
                    if (!is_interactive && !ignore_error)
                        throw;

                    // Surprisingly, this is a client error. A server error would
                    // have been reported w/o throwing (see onReceiveSeverException()).
                    client_exception = std::make_unique<Exception>(getCurrentExceptionMessage(print_stack_trace), getCurrentExceptionCode());
                    have_error = true;
                }

                // For INSERTs with inline data: use the end of inline data as
                // reported by the format parser (it is saved in sendData()).
                // This allows us to handle queries like:
                //   insert into t values (1); select 1
                // , where the inline data is delimited by semicolon and not by a
                // newline.
                auto * insert_ast = parsed_query->as<ASTInsertQuery>();
                if (insert_ast && insert_ast->data)
                {
                    this_query_end = insert_ast->end;
                    adjustQueryEnd(this_query_end, all_queries_end, global_context->getSettingsRef().max_parser_depth);
                }

                // Report error.
                if (have_error)
                    processError(full_query);

                // Stop processing queries if needed.
                if (have_error && !ignore_error)
                    return is_interactive;

                this_query_begin = this_query_end;
                break;
            }
        }
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


std::string LocalServer::getInitialCreateTableQuery()
{
    if (!config().has("table-structure"))
        return {};

    auto table_name = backQuoteIfNeed(config().getString("table-name", "table"));
    auto table_structure = config().getString("table-structure");
    auto data_format = backQuoteIfNeed(config().getString("table-data-format", "TSV"));

    String table_file;
    if (!config().has("table-file") || config().getString("table-file") == "-")
    {
        /// Use Unix tools stdin naming convention
        table_file = "stdin";
    }
    else
    {
        /// Use regular file
        table_file = quoteString(config().getString("table-file"));
    }

    return fmt::format("CREATE TABLE {} ({}) ENGINE = File({}, {});",
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

    if (config().has("users_config") || config().has("config-file") || fs::exists("config.xml"))
    {
        const auto users_config_path = config().getString("users_config", config().getString("config-file", "config.xml"));
        ConfigProcessor config_processor(users_config_path);
        const auto loaded_config = config_processor.loadConfig();
        users_config = loaded_config.configuration;
    }
    else
    {
        users_config = getConfigurationFromXMLString(minimal_default_user_xml);
    }

    if (users_config)
        global_context->setUsersConfig(users_config);
    else
        throw Exception("Can't load config for users", ErrorCodes::CANNOT_LOAD_CONFIG);
}


String LocalServer::getQueryTextPrefix()
{
    return getInitialCreateTableQuery();
}


void LocalServer::connect()
{
    connection_parameters = ConnectionParameters(config());
    connection = LocalConnection::createConnection(connection_parameters, global_context, need_render_progress);
}


int LocalServer::main(const std::vector<std::string> & /*args*/)
try
{
    UseSSL use_ssl;
    ThreadStatus thread_status;
    setupSignalHandler();

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    is_interactive = stdin_is_a_tty && !config().has("query") && !config().has("table-structure") && queries_files.empty();
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
    connect();

    if (is_interactive)
    {
        clearTerminal();
        showClientVersion();
        std::cerr << std::endl;

        runInteractive();
    }
    else
    {
        runNonInteractive();
    }

    cleanup();
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


void LocalServer::processConfig()
{
    if (is_interactive)
    {
        if (config().has("query") && config().has("queries-file"))
            throw Exception("Specify either `query` or `queries-file` option", ErrorCodes::BAD_ARGUMENTS);

        if (config().has("multiquery"))
            is_multiquery = true;

        load_suggestions = true;
    }
    else
    {
        need_render_progress = config().getBool("progress", false);
        echo_queries = config().hasOption("echo") || config().hasOption("verbose");
        ignore_error = config().getBool("ignore-error", false);
        is_multiquery = true;
    }
    print_stack_trace = config().getBool("stacktrace", false);

    auto logging = (config().has("logger.console")
                    || config().has("logger.level")
                    || config().has("log-level")
                    || config().has("logger.log"));

    auto file_logging = config().has("server_logs_file");
    if (is_interactive && logging && !file_logging)
        throw Exception("For interactive mode logging is allowed only with --server_logs_file option",
                        ErrorCodes::BAD_ARGUMENTS);

    if (file_logging)
    {
        auto level = Poco::Logger::parseLevel(config().getString("log-level", "trace"));
        Poco::Logger::root().setLevel(level);
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::SimpleFileChannel>(new Poco::SimpleFileChannel(server_logs_file)));
    }
    else if (logging)
    {
        // force enable logging
        config().setString("logger", "logger");
        // sensitive data rules are not used here
        buildLoggers(config(), logger(), "clickhouse-local");
    }
    else
    {
        Poco::Logger::root().setLevel("none");
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
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

        fs::create_directories(fs::path(path) / "user_defined/");
        LOG_DEBUG(log, "Loading user defined objects from {}", path);
        Poco::File(path + "user_defined/").createDirectories();
        UserDefinedSQLObjectsLoader::instance().loadObjects(global_context);
        enable_objects_loader = true;
        LOG_DEBUG(log, "Loaded user defined objects.");

        LOG_DEBUG(log, "Loading metadata from {}", path);
        fs::create_directories(fs::path(path) / "data/");
        fs::create_directories(fs::path(path) / "metadata/");

        loadMetadataSystem(global_context);
        attachSystemTablesLocal(*createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
        loadMetadata(global_context);
        startupSystemTables();
        DatabaseCatalog::instance().loadDatabases();

        LOG_DEBUG(log, "Loaded metadata.");
    }
    else if (!config().has("no-system-tables"))
    {
        attachSystemTablesLocal(*createMemoryDatabaseIfNotExists(global_context, DatabaseCatalog::SYSTEM_DATABASE));
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


static std::string getHelpHeader()
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


static std::string getHelpFooter()
{
    return
        "Example printing memory used by each Unix user:\n"
        "ps aux | tail -n +2 | awk '{ printf(\"%s\\t%s\\n\", $1, $4) }' | "
        "clickhouse-local -S \"user String, mem Float64\" -q"
            " \"SELECT user, round(sum(mem), 2) as mem_total FROM table GROUP BY user ORDER"
            " BY mem_total DESC FORMAT PrettyCompact\"";
}


void LocalServer::printHelpMessage(const OptionsDescription & options_description)
{
    std::cout << getHelpHeader() << "\n";
    std::cout << options_description.main_description.value() << "\n";
    std::cout << getHelpFooter() << "\n";
}


void LocalServer::addOptions(OptionsDescription & options_description)
{
    options_description.main_description->add_options()
        ("database,d", po::value<std::string>(), "database")
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


void LocalServer::processOptions(const OptionsDescription &, const CommandLineOptions & options, const std::vector<Arguments> &)
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
}

}


#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseLocal(int argc, char ** argv)
{
    DB::LocalServer app;
    try
    {
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
