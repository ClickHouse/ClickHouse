#include "LocalServer.h"

#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Util/OptionCallback.h>
#include <Poco/String.h>
#include <Poco/Logger.h>
#include <Poco/NullChannel.h>
#include <Databases/DatabaseOrdinary.h>
#include <Storages/System/attachSystemTables.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/loadMetadata.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/ConfigProcessor/ConfigProcessor.h>
#include <Common/escapeForFileName.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Parsers/parseQuery.h>
#include <Parsers/IAST.h>
#include <common/ErrorHandlers.h>
#include "StatusFile.h"
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>


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
    if (config().has("silent"))
    {
        Poco::Logger::root().setLevel("none");
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::NullChannel>(new Poco::NullChannel()));
    }
}


void LocalServer::defineOptions(Poco::Util::OptionSet& _options)
{
    Poco::Util::Application::defineOptions (_options);

    _options.addOption(
        Poco::Util::Option("config-file", "", "Load configuration from a given file")
            .required(false)
            .repeatable(false)
            .argument("[config.xml]")
            .binding("config-file"));

    /// Arguments that define first query creating initial table:
    /// (If structure argument is omitted then initial query is not generated)
    _options.addOption(
        Poco::Util::Option("structure", "S", "Structure of initial table(list columns names with their types)")
            .required(false)
            .repeatable(false)
            .argument("[name Type]")
            .binding("table-structure"));

    /// Turn off logging
    _options.addOption(
        Poco::Util::Option("silent", "s", "Quiet mode, print only errors")
            .required(false)
            .repeatable(false)
            .binding("silent"));

    _options.addOption(
        Poco::Util::Option("table", "N", "Name of initial table")
            .required(false)
            .repeatable(false)
            .argument("[table]")
            .binding("table-name"));

    _options.addOption(
        Poco::Util::Option("file", "f", "Path to file with data of initial table (stdin if not specified)")
            .required(false)
            .repeatable(false)
            .argument(" stdin")
            .binding("table-file"));

    _options.addOption(
        Poco::Util::Option("input-format", "if", "Input format of initial table data")
            .required(false)
            .repeatable(false)
            .argument("<TSV>")
            .binding("table-data-format"));

    /// List of queries to execute
    _options.addOption(
        Poco::Util::Option("query", "q", "Queries to execute")
            .required(false)
            .repeatable(false)
            .argument("<query>")
            .binding("query"));

    /// Default Output format
    _options.addOption(
        Poco::Util::Option("output-format", "of", "Default output format")
            .required(false)
            .repeatable(false)
            .argument("[TSV]", true)
            .binding("output-format"));

    /// Alias for previous one, required for clickhouse-client compatibility
    _options.addOption(
        Poco::Util::Option("format", "", "Default output format")
            .required(false)
            .repeatable(false)
            .argument("[TSV]", true)
            .binding("format"));

    _options.addOption(
        Poco::Util::Option("stacktrace", "", "Print stack traces of exceptions")
            .required(false)
            .repeatable(false)
            .binding("stacktrace"));

    _options.addOption(
        Poco::Util::Option("verbose", "", "Print info about execution of queries")
            .required(false)
            .repeatable(false)
            .noArgument()
            .binding("verbose"));

    _options.addOption(
        Poco::Util::Option("help", "", "Display help information")
        .required(false)
        .repeatable(false)
        .noArgument()
        .binding("help")
        .callback(Poco::Util::OptionCallback<LocalServer>(this, &LocalServer::handleHelp)));

    /// These arrays prevent "variable tracking size limit exceeded" compiler notice.
    static const char * settings_names[] = {
#define DECLARE_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) #NAME,
    APPLY_FOR_SETTINGS(DECLARE_SETTING)
#undef DECLARE_SETTING
    nullptr};

    static const char * limits_names[] = {
#define DECLARE_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) #NAME,
    APPLY_FOR_LIMITS(DECLARE_SETTING)
#undef DECLARE_SETTING
    nullptr};

    for (const char ** name = settings_names; *name; ++name)
        _options.addOption(Poco::Util::Option(*name, "", "Settings.h").required(false).argument("<value>")
        .repeatable(false).binding(*name));

    for (const char ** name = limits_names; *name; ++name)
        _options.addOption(Poco::Util::Option(*name, "", "Limits.h").required(false).argument("<value>")
        .repeatable(false).binding(*name));
}


void LocalServer::applyOptions()
{
    context->setDefaultFormat(config().getString("output-format", config().getString("format", "TSV")));

    /// settings and limits could be specified in config file, but passed settings has higher priority
#define EXTRACT_SETTING(TYPE, NAME, DEFAULT, DESCRIPTION) \
        if (config().has(#NAME) && !context->getSettingsRef().NAME.changed) \
            context->setSetting(#NAME, config().getString(#NAME));
        APPLY_FOR_SETTINGS(EXTRACT_SETTING)
#undef EXTRACT_SETTING

#define EXTRACT_LIMIT(TYPE, NAME, DEFAULT, DESCRIPTION) \
        if (config().has(#NAME) && !context->getSettingsRef().limits.NAME.changed) \
            context->setSetting(#NAME, config().getString(#NAME));
        APPLY_FOR_LIMITS(EXTRACT_LIMIT)
#undef EXTRACT_LIMIT
}


void LocalServer::displayHelp()
{
    Poco::Util::HelpFormatter helpFormatter(options());
    helpFormatter.setCommand(commandName());
    helpFormatter.setUsage("[initial table definition] [--query <query>]");
    helpFormatter.setHeader("\n"
        "clickhouse-local allows to execute SQL queries on your data files via single command line call.\n"
        "To do so, intially you need to define your data source and its format.\n"
        "After you can execute your SQL queries in the usual manner.\n"
        "There are two ways to define initial table keeping your data:\n"
        "either just in first query like this:\n"
        "    CREATE TABLE <table> (<structure>) ENGINE = File(<input-format>, <file>);\n"
        "either through corresponding command line parameters."
    );
    helpFormatter.setWidth(132); /// 80 is ugly due to wide settings params

    helpFormatter.format(std::cerr);
    std::cerr << "Example printing memory used by each Unix user:\n"
    "ps aux | tail -n +2 | awk '{ printf(\"%s\\t%s\\n\", $1, $4) }' | "
    "clickhouse-local -S \"user String, mem Float64\" -q \"SELECT user, round(sum(mem), 2) as memTotal FROM table GROUP BY user ORDER BY memTotal DESC FORMAT Pretty\"\n";
}


void LocalServer::handleHelp(const std::string & /*name*/, const std::string & /*value*/)
{
    displayHelp();
    stopOptionsProcessing();
}


/// If path is specified and not empty, will try to setup server environment and load existing metadata
void LocalServer::tryInitPath()
{
    if (!config().has("path") || (path = config().getString("path")).empty())
        return;

    Poco::trimInPlace(path);
    if (path.empty())
        return;
    if (path.back() != '/')
        path += '/';

    context->setPath(path);

    StatusFile status{path + "status"};
}


int LocalServer::main(const std::vector<std::string> & /*args*/)
try
{
    Logger * log = &logger();

    if (!config().has("query") && !config().has("table-structure")) /// Nothing to process
    {
        if (!config().hasOption("help"))
        {
            std::cerr << "There are no queries to process." << std::endl;
            displayHelp();
        }

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

    applyOptions();

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
        context->setMacros(Macros(config(), "macros"));

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
    const std::string default_database = "_local";
    context->addDatabase(default_database, std::make_shared<DatabaseMemory>(default_database));
    context->setCurrentDatabase(default_database);

    if (!path.empty())
    {
        LOG_DEBUG(log, "Loading metadata from " << path);
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
    bool print_stack_trace = config().has("stacktrace");

    std::string text = e.displayText();

    auto embedded_stack_trace_pos = text.find("Stack trace");
    if (std::string::npos != embedded_stack_trace_pos && !print_stack_trace)
        text.resize(embedded_stack_trace_pos);

    std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

    if (print_stack_trace && std::string::npos == embedded_stack_trace_pos)
    {
        std::cerr << "Stack trace:" << std::endl << e.getStackTrace().toString();
    }

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
    Logger * log = &logger();

    String initial_create_query = getInitialCreateTableQuery();
    String queries_str = initial_create_query + config().getString("query");

    bool verbose = config().hasOption("verbose");

    std::vector<String> queries;
    auto parse_res = splitMultipartQuery(queries_str, queries);

    if (!parse_res.second)
        throw Exception("Cannot parse and execute the following part of query: " + String(parse_res.first), ErrorCodes::SYNTAX_ERROR);

    context->setUser("default", "", Poco::Net::SocketAddress{}, "");

    for (const auto & query : queries)
    {
        ReadBufferFromString read_buf(query);
        WriteBufferFromFileDescriptor write_buf(STDOUT_FILENO);

        if (verbose)
            LOG_INFO(log, "Executing query: " << query);

        executeQuery(read_buf, write_buf, /* allow_into_outfile = */ true, *context, {});
    }
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
