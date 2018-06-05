#include "LocalServer.h"

#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Util/OptionCallback.h>
#include <Poco/String.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Databases/DatabaseMemory.h>
#include <Storages/System/attachSystemTables.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/loadMetadata.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/escapeForFileName.h>
#include <Common/ClickHouseRevision.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>
#include <common/ErrorHandlers.h>
#include <Common/StatusFile.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int CANNOT_LOAD_CONFIG;
}


LocalServer::LocalServer() = default;

LocalServer::~LocalServer()
{
    if (context)
        context->shutdown(); /// required for properly exception handling
}


void LocalServer::initialize(Poco::Util::Application & self)
{
    Poco::Util::Application::initialize(self);

    // Turn off server logging to stderr
    if (!config().has("verbose"))
    {
        Poco::Logger::root().setLevel("none");
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
    }
}

void LocalServer::applyCmdSettings(Context & context)
{
#define EXTRACT_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) \
        if (cmd_settings.NAME.changed) \
            context.getSettingsRef().NAME = cmd_settings.NAME;
        APPLY_FOR_SETTINGS(EXTRACT_SETTING)
#undef EXTRACT_SETTING
}

/// If path is specified and not empty, will try to setup server environment and load existing metadata
void LocalServer::tryInitPath()
{
    std::string path = config().getString("path", "");
    Poco::trimInPlace(path);

    if (!path.empty())
    {
        if (path.back() != '/')
            path += '/';

        context->setPath(path);
        return;
    }

    /// In case of empty path set paths to helpful directories
    std::string cd = Poco::Path::current();
    context->setTemporaryPath(cd + "tmp");
    context->setFlagsPath(cd + "flags");
    context->setUserFilesPath(""); // user's files are everywhere
}


int LocalServer::main(const std::vector<std::string> & /*args*/)
try
{
    Logger * log = &logger();

    if (!config().has("query") && !config().has("table-structure")) /// Nothing to process
    {
        if (!config().hasOption("silent"))
            std::cerr << "There are no queries to process." << std::endl;

        return Application::EXIT_OK;
    }

    /// Load config files if exists
    if (config().has("config-file") || Poco::File("config.xml").exists())
    {
        ConfigProcessor config_processor(config().getString("config-file", "config.xml"), false, true);
        auto loaded_config = config_processor.loadConfig();
        config_processor.savePreprocessedConfig(loaded_config);
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    context = std::make_unique<Context>(Context::createGlobal());
    context->setGlobalContext(*context);
    context->setApplicationType(Context::ApplicationType::LOCAL);
    tryInitPath();

    std::optional<StatusFile> status;

    /// Skip temp path installation

    /// We will terminate process on error
    static KillingErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Don't initilaize DateLUT

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();

    /// Maybe useless
    if (config().has("macros"))
        context->setMacros(std::make_unique<Macros>(config(), "macros"));

    /// Skip networking

    setupUsers();

    /// Limit on total number of concurrently executing queries.
    /// Threre are no need for concurrent threads, override max_concurrent_queries.
    context->getProcessList().setMaxSize(0);

    /// Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", 0);
    if (uncompressed_cache_size)
        context->setUncompressedCache(uncompressed_cache_size);

    /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
    /// Specify default value for mark_cache_size explicitly!
    size_t mark_cache_size = config().getUInt64("mark_cache_size", 5368709120);
    if (mark_cache_size)
        context->setMarkCache(mark_cache_size);

    /// Load global settings from default_profile and system_profile.
    context->setDefaultProfiles(config());

    /** Init dummy default DB
      * NOTE: We force using isolated default database to avoid conflicts with default database from server enviroment
      * Otherwise, metadata of temporary File(format, EXPLICIT_PATH) tables will pollute metadata/ directory;
      *  if such tables will not be dropped, clickhouse-server will not be able to load them due to security reasons.
      */
    std::string default_database = config().getString("default_database", "_local");
    context->addDatabase(default_database, std::make_shared<DatabaseMemory>(default_database));
    context->setCurrentDatabase(default_database);
    applyCmdOptions(*context);

    if (!context->getPath().empty())
    {
        /// Lock path directory before read
        status.emplace(context->getPath() + "status");

        LOG_DEBUG(log, "Loading metadata from " << context->getPath());
        loadMetadataSystem(*context);
        attachSystemTables();
        loadMetadata(*context);
        LOG_DEBUG(log, "Loaded metadata.");
    }
    else
    {
        attachSystemTables();
    }

    processQueries();

    context->shutdown();
    context.reset();

    return Application::EXIT_OK;
}
catch (const Exception & e)
{
    if (!config().hasOption("silent"))
        std::cerr << getCurrentExceptionMessage(config().hasOption("stacktrace"));

    /// If exception code isn't zero, we should return non-zero return code anyway.
    return e.code() ? e.code() : -1;
}


inline String getQuotedString(const String & s)
{
    WriteBufferFromOwnString buf;
    writeQuotedString(s, buf);
    return buf.str();
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
        table_file = getQuotedString(config().getString("table-file"));

    return
    "CREATE TABLE " + table_name +
        " (" + table_structure + ") " +
    "ENGINE = "
        "File(" + data_format + ", " + table_file + ")"
    "; ";
}


void LocalServer::attachSystemTables()
{
    DatabasePtr system_database = context->tryGetDatabase("system");
    if (!system_database)
    {
        /// TODO: add attachTableDelayed into DatabaseMemory to speedup loading
        system_database = std::make_shared<DatabaseMemory>("system");
        context->addDatabase("system", system_database);
    }

    attachSystemTablesLocal(*system_database);
}


void LocalServer::processQueries()
{
    String initial_create_query = getInitialCreateTableQuery();
    String queries_str = initial_create_query + config().getString("query");

    std::vector<String> queries;
    auto parse_res = splitMultipartQuery(queries_str, queries);

    if (!parse_res.second)
        throw Exception("Cannot parse and execute the following part of query: " + String(parse_res.first), ErrorCodes::SYNTAX_ERROR);

    context->setSessionContext(*context);
    context->setQueryContext(*context);

    context->setUser("default", "", Poco::Net::SocketAddress{}, "");
    context->setCurrentQueryId("");
    applyCmdSettings(*context);

    bool echo_query = config().hasOption("echo") || config().hasOption("verbose");
    std::exception_ptr exception;

    for (const auto & query : queries)
    {
        ReadBufferFromString read_buf(query);
        WriteBufferFromFileDescriptor write_buf(STDOUT_FILENO);

        if (echo_query)
            std::cerr << query << "\n";

        try
        {
            executeQuery(read_buf, write_buf, /* allow_into_outfile = */ true, *context, {});
        }
        catch (...)
        {
            if (!config().hasOption("ignore-error"))
                throw;

            if (!exception)
                exception = std::current_exception();

            if (!config().has("silent"))
                std::cerr << getCurrentExceptionMessage(config().hasOption("stacktrace"));
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
    std::stringstream ss{std::string{xml_data}};
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}


void LocalServer::setupUsers()
{
    ConfigurationPtr users_config;

    if (config().has("users_config") || config().has("config-file") || Poco::File("config.xml").exists())
    {
        const auto users_config_path = config().getString("users_config", config().getString("config-file", "config.xml"));
        ConfigProcessor config_processor(users_config_path);
        const auto loaded_config = config_processor.loadConfig();
        config_processor.savePreprocessedConfig(loaded_config);
        users_config = loaded_config.configuration;
    }
    else
    {
        users_config = getConfigurationFromXMLString(minimal_default_user_xml);
    }

    if (users_config)
        context->setUsersConfig(users_config);
    else
        throw Exception("Can't load config for users", ErrorCodes::CANNOT_LOAD_CONFIG);
}

static void showClientVersion()
{
    std::cout << "ClickHouse client version " << DBMS_VERSION_MAJOR
        << "." << DBMS_VERSION_MINOR
        << "." << ClickHouseRevision::get()
        << "." << std::endl;
}

std::string LocalServer::getHelpHeader() const
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

std::string LocalServer::getHelpFooter() const
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

    unsigned line_length = po::options_description::m_default_line_length;
    unsigned min_description_length = line_length / 2;
    if (isatty(STDIN_FILENO))
    {
        winsize terminal_size{};
        ioctl(0, TIOCGWINSZ, &terminal_size);
        line_length = std::max(3U, static_cast<unsigned>(terminal_size.ws_col));
        min_description_length = std::min(min_description_length, line_length - 2);
    }

#define DECLARE_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) (#NAME, po::value<std::string> (), DESCRIPTION)
    po::options_description description("Main options", line_length, min_description_length);
    description.add_options()
        ("help", "produce help message")
        ("config-file,c", po::value<std::string>(), "config-file path")
        ("query,q", po::value<std::string>(), "query")
        ("database,d", po::value<std::string>(), "database")

        ("table,N", po::value<std::string>(), "name of the initial table")
        /// If structure argument is omitted then initial query is not generated
        ("structure,S", po::value<std::string>(), "structure of the initial table (list of column and type names)")
        ("file,f", po::value<std::string>(), "path to file with data of the initial table (stdin if not specified)")
        ("input-format", po::value<std::string>(), "input format of the initial table data")
        ("format,f", po::value<std::string>(), "default output format (clickhouse-client compatibility)")
        ("output-format", po::value<std::string>(), "default output format")

        ("silent,s", "quiet mode, do not print errors")
        ("stacktrace", "print stack traces of exceptions")
        ("echo", "print query before execution")
        ("verbose", "print query and other debugging info")
        ("ignore-error", "do not stop processing if a query failed")
        ("version,V", "print version information and exit")
        APPLY_FOR_SETTINGS(DECLARE_SETTING);
#undef DECLARE_SETTING

    /// Parse main commandline options.
    po::parsed_options parsed = po::command_line_parser(argc, argv).options(description).run();
    po::variables_map options;
    po::store(parsed, options);

    if (options.count("version") || options.count("V"))
    {
        showClientVersion();
        exit(0);
    }

    if (options.count("help"))
    {
        std::cout << getHelpHeader() << "\n";
        std::cout << description << "\n";
        std::cout << getHelpFooter() << "\n";
        exit(0);
    }

    /// Extract settings and limits from the options.
#define EXTRACT_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) \
    if (options.count(#NAME)) \
        cmd_settings.set(#NAME, options[#NAME].as<std::string>());
    APPLY_FOR_SETTINGS(EXTRACT_SETTING)
#undef EXTRACT_SETTING

    /// Save received data into the internal config.
    if (options.count("config-file"))
        config().setString("config-file", options["config-file"].as<std::string>());
    if (options.count("query"))
        config().setString("query", options["query"].as<std::string>());
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

    if (options.count("silent"))
        config().setBool("silent", true);
    if (options.count("stacktrace"))
        config().setBool("stacktrace", true);
    if (options.count("echo"))
        config().setBool("echo", true);
    if (options.count("verbose"))
        config().setBool("verbose", true);
    if (options.count("ignore-error"))
        config().setBool("ignore-error", true);
}

void LocalServer::applyCmdOptions(Context & context)
{
    context.setDefaultFormat(config().getString("output-format", config().getString("format", "TSV")));
    applyCmdSettings(context);
}

}

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
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
