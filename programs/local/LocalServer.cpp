#include "LocalServer.h"

#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Util/OptionCallback.h>
#include <Poco/String.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/System/attachSystemTables.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/escapeForFileName.h>
#include <Common/ClickHouseRevision.h>
#include <Common/ThreadStatus.h>
#include <Common/UnicodeBar.h>
#include <Common/config_version.h>
#include <Common/quoteString.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/UseSSL.h>
#include <IO/ReadHelpers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>
#include <common/ErrorHandlers.h>
#include <Common/StatusFile.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options.hpp>
#include <common/argsToConfig.h>
#include <Common/TerminalSize.h>
#include <Common/randomSeed.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
    extern const int CANNOT_LOAD_CONFIG;
    extern const int FILE_ALREADY_EXISTS;
}


LocalServer::LocalServer() = default;

LocalServer::~LocalServer()
{
    if (global_context)
        global_context->shutdown(); /// required for properly exception handling
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
        config_processor.savePreprocessedConfig(loaded_config, loaded_config.configuration->getString("path", "."));
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    if (config().has("logger.console") || config().has("logger.level") || config().has("logger.log"))
    {
        // force enable logging
        config().setString("logger", "logger");
        // sensitive data rules are not used here
        buildLoggers(config(), logger(), "clickhouse-local");
    }
    else
    {
        // Turn off server logging to stderr
        if (!config().has("verbose"))
        {
            Poco::Logger::root().setLevel("none");
            Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
        }
    }
}

void LocalServer::applyCmdSettings(ContextMutablePtr context)
{
    context->applySettingsChanges(cmd_settings.changes());
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
        catch (const std::filesystem::filesystem_error& e)
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
}


static void attachSystemTables(ContextPtr context)
{
    DatabasePtr system_database = DatabaseCatalog::instance().tryGetDatabase(DatabaseCatalog::SYSTEM_DATABASE);
    if (!system_database)
    {
        /// TODO: add attachTableDelayed into DatabaseMemory to speedup loading
        system_database = std::make_shared<DatabaseMemory>(DatabaseCatalog::SYSTEM_DATABASE, context);
        DatabaseCatalog::instance().attachDatabase(DatabaseCatalog::SYSTEM_DATABASE, system_database);
    }

    attachSystemTablesLocal(*system_database);
}


int LocalServer::main(const std::vector<std::string> & /*args*/)
try
{
    Poco::Logger * log = &logger();
    ThreadStatus thread_status;
    UseSSL use_ssl;

    if (!config().has("query") && !config().has("table-structure") && !config().has("queries-file")) /// Nothing to process
    {
        if (config().hasOption("verbose"))
            std::cerr << "There are no queries to process." << '\n';

        return Application::EXIT_OK;
    }

    if (config().has("query") && config().has("queries-file"))
    {
        throw Exception("Specify either `query` or `queries-file` option", ErrorCodes::BAD_ARGUMENTS);
    }

    shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::LOCAL);
    tryInitPath();

    std::optional<StatusFile> status;

    /// Skip temp path installation

    /// We will terminate process on error
    static KillingErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Don't initialize DateLUT

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();
    registerDictionaries();
    registerDisks();
    registerFormats();

    /// Maybe useless
    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    /// Skip networking

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

    /// A cache for mmapped files.
    size_t mmap_cache_size = config().getUInt64("mmap_cache_size", 1000);   /// The choice of default is arbitrary.
    if (mmap_cache_size)
        global_context->setMMappedFileCache(mmap_cache_size);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());

    /** Init dummy default DB
      * NOTE: We force using isolated default database to avoid conflicts with default database from server environment
      * Otherwise, metadata of temporary File(format, EXPLICIT_PATH) tables will pollute metadata/ directory;
      *  if such tables will not be dropped, clickhouse-server will not be able to load them due to security reasons.
      */
    std::string default_database = config().getString("default_database", "_local");
    DatabaseCatalog::instance().attachDatabase(default_database, std::make_shared<DatabaseMemory>(default_database, global_context));
    global_context->setCurrentDatabase(default_database);
    applyCmdOptions(global_context);

    if (config().has("path"))
    {
        String path = global_context->getPath();

        /// Lock path directory before read
        status.emplace(path + "status", StatusFile::write_full_info);

        LOG_DEBUG(log, "Loading metadata from {}", path);
        fs::create_directories(fs::path(path) / "data/");
        fs::create_directories(fs::path(path) / "metadata/");
        loadMetadataSystem(global_context);
        attachSystemTables(global_context);
        loadMetadata(global_context);
        DatabaseCatalog::instance().loadDatabases();
        LOG_DEBUG(log, "Loaded metadata.");
    }
    else if (!config().has("no-system-tables"))
    {
        attachSystemTables(global_context);
    }

    processQueries();

    global_context->shutdown();
    global_context.reset();

    status.reset();
    cleanup();

    return Application::EXIT_OK;
}
catch (const Exception & e)
{
    try
    {
        cleanup();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    std::cerr << getCurrentExceptionMessage(config().hasOption("stacktrace")) << '\n';

    /// If exception code isn't zero, we should return non-zero return code anyway.
    return e.code() ? e.code() : -1;
}


std::string LocalServer::getInitialCreateTableQuery()
{
    if (!config().has("table-structure"))
        return {};

    auto table_name = backQuoteIfNeed(config().getString("table-name", "table"));
    auto table_structure = config().getString("table-structure");
    auto data_format = backQuoteIfNeed(config().getString("table-data-format", "TSV"));
    String table_file;
    if (!config().has("table-file") || config().getString("table-file") == "-") /// Use Unix tools stdin naming convention
        table_file = "stdin";
    else /// Use regular file
        table_file = quoteString(config().getString("table-file"));

    return
    "CREATE TABLE " + table_name +
        " (" + table_structure + ") " +
    "ENGINE = "
        "File(" + data_format + ", " + table_file + ")"
    "; ";
}


void LocalServer::processQueries()
{
    String initial_create_query = getInitialCreateTableQuery();
    String queries_str = initial_create_query;

    if (config().has("query"))
        queries_str += config().getRawString("query");
    else
    {
        String queries_from_file;
        ReadBufferFromFile in(config().getString("queries-file"));
        readStringUntilEOF(queries_from_file, in);
        queries_str += queries_from_file;
    }

    const auto & settings = global_context->getSettingsRef();

    std::vector<String> queries;
    auto parse_res = splitMultipartQuery(queries_str, queries, settings.max_query_size, settings.max_parser_depth);

    if (!parse_res.second)
        throw Exception("Cannot parse and execute the following part of query: " + String(parse_res.first), ErrorCodes::SYNTAX_ERROR);

    /// we can't mutate global global_context (can lead to races, as it was already passed to some background threads)
    /// so we can't reuse it safely as a query context and need a copy here
    auto context = Context::createCopy(global_context);

    context->makeSessionContext();
    context->makeQueryContext();

    context->setUser("default", "", Poco::Net::SocketAddress{});
    context->setCurrentQueryId("");
    applyCmdSettings(context);

    /// Use the same query_id (and thread group) for all queries
    CurrentThread::QueryScope query_scope_holder(context);

    ///Set progress show
    need_render_progress = config().getBool("progress", false);

    if (need_render_progress)
    {
        context->setProgressCallback([&](const Progress & value)
        {
            /// Write progress only if progress was updated
            if (progress_indication.updateProgress(value))
                progress_indication.writeProgress();
        });
    }

    bool echo_queries = config().hasOption("echo") || config().hasOption("verbose");

    if (need_render_progress)
        progress_indication.setFileProgressCallback(context);

    std::exception_ptr exception;

    for (const auto & query : queries)
    {
        written_first_block = false;
        progress_indication.resetProgress();

        ReadBufferFromString read_buf(query);
        WriteBufferFromFileDescriptor write_buf(STDOUT_FILENO);

        if (echo_queries)
        {
            writeString(query, write_buf);
            writeChar('\n', write_buf);
            write_buf.next();
        }

        try
        {
            executeQuery(read_buf, write_buf, /* allow_into_outfile = */ true, context, {});
        }
        catch (...)
        {
            if (!config().hasOption("ignore-error"))
                throw;

            if (!exception)
                exception = std::current_exception();

            std::cerr << getCurrentExceptionMessage(config().hasOption("stacktrace")) << '\n';
        }
    }

    if (exception)
        std::rethrow_exception(exception);
}

static const char * minimal_default_user_xml =
"<yandex>"
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
"</yandex>";


static ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};    // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}


void LocalServer::setupUsers()
{
    ConfigurationPtr users_config;

    if (config().has("users_config") || config().has("config-file") || fs::exists("config.xml"))
    {
        const auto users_config_path = config().getString("users_config", config().getString("config-file", "config.xml"));
        ConfigProcessor config_processor(users_config_path);
        const auto loaded_config = config_processor.loadConfig();
        config_processor.savePreprocessedConfig(loaded_config, config().getString("path", DBMS_DEFAULT_PATH));
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

void LocalServer::cleanup()
{
    // Delete the temporary directory if needed.
    if (temporary_directory_to_delete)
    {
        const auto dir = *temporary_directory_to_delete;
        temporary_directory_to_delete.reset();
        LOG_DEBUG(&logger(), "Removing temporary directory: {}", dir.string());
        remove_all(dir);
    }
}

static void showClientVersion()
{
    std::cout << DBMS_NAME << " client version " << VERSION_STRING << VERSION_OFFICIAL << "." << '\n';
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

void LocalServer::init(int argc, char ** argv)
{
    namespace po = boost::program_options;

    /// Don't parse options with Poco library, we prefer neat boost::program_options
    stopOptionsProcessing();

    po::options_description description = createOptionsDescription("Main options", getTerminalWidth());
    description.add_options()
        ("help", "produce help message")
        ("config-file,c", po::value<std::string>(), "config-file path")
        ("query,q", po::value<std::string>(), "query")
        ("queries-file, qf", po::value<std::string>(), "file path with queries to execute")
        ("database,d", po::value<std::string>(), "database")

        ("table,N", po::value<std::string>(), "name of the initial table")
        /// If structure argument is omitted then initial query is not generated
        ("structure,S", po::value<std::string>(), "structure of the initial table (list of column and type names)")
        ("file,f", po::value<std::string>(), "path to file with data of the initial table (stdin if not specified)")
        ("input-format", po::value<std::string>(), "input format of the initial table data")
        ("format,f", po::value<std::string>(), "default output format (clickhouse-client compatibility)")
        ("output-format", po::value<std::string>(), "default output format")

        ("stacktrace", "print stack traces of exceptions")
        ("echo", "print query before execution")
        ("verbose", "print query and other debugging info")
        ("logger.console", po::value<bool>()->implicit_value(true), "Log to console")
        ("logger.log", po::value<std::string>(), "Log file name")
        ("logger.level", po::value<std::string>(), "Log level")
        ("ignore-error", "do not stop processing if a query failed")
        ("no-system-tables", "do not attach system tables (better startup time)")
        ("version,V", "print version information and exit")
        ("progress", "print progress of queries execution")
        ;

    cmd_settings.addProgramOptions(description);

    /// Parse main commandline options.
    po::parsed_options parsed = po::command_line_parser(argc, argv).options(description).run();
    po::variables_map options;
    po::store(parsed, options);
    po::notify(options);

    if (options.count("version") || options.count("V"))
    {
        showClientVersion();
        exit(0);
    }

    if (options.empty() || options.count("help"))
    {
        std::cout << getHelpHeader() << "\n";
        std::cout << description << "\n";
        std::cout << getHelpFooter() << "\n";
        exit(0);
    }

    /// Save received data into the internal config.
    if (options.count("config-file"))
        config().setString("config-file", options["config-file"].as<std::string>());
    if (options.count("query"))
        config().setString("query", options["query"].as<std::string>());
    if (options.count("queries-file"))
        config().setString("queries-file", options["queries-file"].as<std::string>());
    if (options.count("database"))
        config().setString("default_database", options["database"].as<std::string>());

    if (options.count("table"))
        config().setString("table-name", options["table"].as<std::string>());
    if (options.count("file"))
        config().setString("table-file", options["file"].as<std::string>());
    if (options.count("structure"))
        config().setString("table-structure", options["structure"].as<std::string>());
    if (options.count("input-format"))
        config().setString("table-data-format", options["input-format"].as<std::string>());
    if (options.count("format"))
        config().setString("format", options["format"].as<std::string>());
    if (options.count("output-format"))
        config().setString("output-format", options["output-format"].as<std::string>());

    if (options.count("stacktrace"))
        config().setBool("stacktrace", true);
    if (options.count("progress"))
        config().setBool("progress", true);
    if (options.count("echo"))
        config().setBool("echo", true);
    if (options.count("verbose"))
        config().setBool("verbose", true);
    if (options.count("logger.console"))
        config().setBool("logger.console", options["logger.console"].as<bool>());
    if (options.count("logger.log"))
        config().setString("logger.log", options["logger.log"].as<std::string>());
    if (options.count("logger.level"))
        config().setString("logger.level", options["logger.level"].as<std::string>());
    if (options.count("ignore-error"))
        config().setBool("ignore-error", true);
    if (options.count("no-system-tables"))
        config().setBool("no-system-tables", true);

    std::vector<std::string> arguments;
    for (int arg_num = 1; arg_num < argc; ++arg_num)
        arguments.emplace_back(argv[arg_num]);
    argsToConfig(arguments, config(), 100);
}

void LocalServer::applyCmdOptions(ContextMutablePtr context)
{
    context->setDefaultFormat(config().getString("output-format", config().getString("format", "TSV")));
    applyCmdSettings(context);
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
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << '\n';
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
