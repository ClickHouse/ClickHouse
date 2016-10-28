#include "LocalServer.h"
#include <Poco/Util/XMLConfiguration.h>

#include <DB/Databases/DatabaseOrdinary.h>

#include <DB/Storages/System/StorageSystemNumbers.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Interpreters/executeQuery.h>

#include <DB/Common/Macros.h>
#include <DB/Common/ConfigProcessor.h>
#include <DB/Common/escapeForFileName.h>

#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/IAST.h>

#include <common/ErrorHandlers.h>

namespace DB
{

void LocalServer::initialize(Poco::Util::Application & self)
{
	Poco::Util::Application::initialize(self);

	/// Load config files if exists
	if (config().has("config-file") || Poco::File("config.xml").exists())
	{
		ConfigurationPtr processed_config = ConfigProcessor(false, true).loadConfig(config().getString("config-file", "config.xml"));
		config().add(processed_config.duplicate(), PRIO_DEFAULT, false);
	}
}

void LocalServer::defineOptions(Poco::Util::OptionSet& _options)
{
	Poco::Util::Application::defineOptions (_options);

	_options.addOption(
		Poco::Util::Option ("config-file", "C", "load configuration from a given file")
			.required (false)
			.repeatable (false)
			.argument ("<file>")
			.binding("config-file")
			);

	/// Arguments that define first query creating initial table:
	/// (If structure argument is omitted then initial query is not generated)
	_options.addOption(
		Poco::Util::Option ("structure", "S", "Structe of initial table (i.e. list columns names with their types)")
			.required (false)
			.repeatable (false)
			.argument ("<string>")
			.binding("table-structure")
			);

	_options.addOption(
		Poco::Util::Option ("table", "N", "Name of intial table ('table' by default)")
			.required (false)
			.repeatable (false)
			.argument ("<file>")
			.binding("table-name")
			);

	_options.addOption(
		Poco::Util::Option ("file", "F", "Path to file with data of initial table (stdin if not specified)")
			.required (false)
			.repeatable (false)
			.argument ("<file>")
			.binding("table-file")
			);

	_options.addOption(
		Poco::Util::Option ("format", "F", "Format of intial table's data (TabSeparated by default)")
			.required (false)
			.repeatable (false)
			.argument ("<file>")
			.binding("table-data-format")
			);

	/// List of queries to execute
	_options.addOption(
		Poco::Util::Option ("query", "Q", "[Local mode] queries to execute")
			.required (false)
			.repeatable (false)
			.argument ("<string>", true)
			.binding("query")
			);
}

int LocalServer::main(const std::vector<std::string> & args)
{
	if (!config().has("query")) /// Nothing to process
		return Application::EXIT_OK;

	if (config().has("config-file") || Poco::File("config.xml").exists())
	{
		ConfigurationPtr processed_config = ConfigProcessor(false, true).loadConfig(config().getString("config-file", "config.xml"));
		config().add(processed_config.duplicate(), PRIO_DEFAULT, false);
	}

	context = std::make_unique<Context>();
	context->setGlobalContext(*context);

	/// Skip path, temp path, flag's path installation

	/// We will terminate process on error
	static KillingErrorHandler error_handler;
	Poco::ErrorHandler::set(&error_handler);

	/// Don't initilaize DateLUT

	/// Maybe useless
	if (config().has("macros"))
		context->setMacros(Macros(config(), "macros"));

	/// Skip networking

	setupUsers();

	/// Limit on total number of coucurrently executed queries.
	/// Threre are no need for concurrent threads, override max_concurrent_queries.
	context->getProcessList().setMaxSize(0);

	/// Size of cache for uncompressed blocks. Zero means disabled.
	size_t uncompressed_cache_size = parse<size_t>(config().getString("uncompressed_cache_size", "0"));
	if (uncompressed_cache_size)
		context->setUncompressedCache(uncompressed_cache_size);

	/// Size of cache for marks (index of MergeTree family of tables). It is necessary.
	/// Specify default value if mark_cache_size explicitly!
	size_t mark_cache_size = parse<size_t>(config().getString("mark_cache_size", "5368709120"));
	if (mark_cache_size)
		context->setMarkCache(mark_cache_size);

	/// Load global settings from default profile.
	context->setSetting("profile", config().getString("default_profile", "default"));

	/// Init dummy default DB
	const std::string default_database = "default";
	context->addDatabase(default_database, std::make_shared<DatabaseMemory>(default_database));
	context->setCurrentDatabase(default_database);
	attachSystemTables();

	processQueries();

	return Application::EXIT_OK;
}


std::string LocalServer::getInitialCreateTableQuery()
{
	if (!config().has("table-structure"))
		return std::string();

	auto table_name = backQuoteIfNeed(config().getString("table-name", "table"));
	auto table_structure = config().getString("table-structure"); /// TODO: add back quotes
	auto data_format = backQuoteIfNeed(config().getString("table-data-format", "TabSeparated"));
	auto table_file = backQuoteIfNeed(config().getString("table-file", "stdin"));

	return
	"CREATE TABLE " + table_name +
		" (" + table_structure + ") " +
	"ENGINE = "
		"File(" + data_format + ", '" + table_file + "')"
	"; ";
}


void LocalServer::attachSystemTables()
{
	/// Only numbers table make sense
	/// TODO: add attachTableDelayed into DatabaseMemory

	DatabasePtr system_database = std::make_shared<DatabaseMemory>("system");
	context->addDatabase("system", system_database);
	system_database->attachTable("numbers", StorageSystemNumbers::create("numbers"));
}


void LocalServer::processQueries()
{
	Logger * log = &logger();

	String initial_create_query = getInitialCreateTableQuery();
	String queries_str = initial_create_query + config().getString("query");
	LOG_INFO(log, "Executing queries: '" << queries_str << "'");

	std::vector<String> queries;
	auto parse_res = splitMultipartQuery(queries_str, queries);

	context->setUser("default", "", Poco::Net::IPAddress{}, 0, "");

	for (const auto & query : queries)
	{
		try
		{
			ReadBufferFromString read_buf(query);
			WriteBufferFromFileDescriptor write_buf(STDOUT_FILENO);
			BlockInputStreamPtr plan;

			LOG_INFO(log, "Executing query: " << query);
			executeQuery(read_buf, write_buf, *context, plan, nullptr);
		}
		catch (...)
		{
			tryLogCurrentException(log, "An error ocurred while executing query");
			throw;
		}
	}

	/// Execute while queries are valid
	if (!parse_res.second)
	{
		LOG_ERROR(log, "Cannot parse and execute the following part of query: '" << parse_res.first << "'");
	}
}

void LocalServer::setupUsers()
{
	ConfigurationPtr users_config;

	if (config().has("users_config") || config().has("config-file") || Poco::File("config.xml").exists())
	{
		auto users_config_path = config().getString("users_config", config().getString("config-file", "config.xml"));
		users_config = ConfigProcessor().loadConfig(users_config_path);
	}
	else
	{
		std::stringstream default_user_stream;
		default_user_stream <<
"<yandex>\n"
"	<profiles>\n"
"		<default>\n"
"			<max_memory_usage>10000000000</max_memory_usage>\n"
"			<use_uncompressed_cache>0</use_uncompressed_cache>\n"
"			<load_balancing>random</load_balancing>\n"
"		</default>\n"
"		<readonly>\n"
"			<readonly>1</readonly>\n"
"		</readonly>\n"
"	</profiles>\n"
"\n"
"	<users>\n"
"		<default>\n"
"			<password></password>\n"
"			<networks>\n"
"				<ip>::/0</ip>\n"
"			</networks>\n"
"			<profile>default</profile>\n"
"			<quota>default</quota>\n"
"		</default>\n"
"	</users>\n"
"\n"
"	<quotas>\n"
"		<default>\n"
"			<interval>\n"
"				<duration>3600</duration>\n"
"				<queries>0</queries>\n"
"				<errors>0</errors>\n"
"				<result_rows>0</result_rows>\n"
"				<read_rows>0</read_rows>\n"
"				<execution_time>0</execution_time>\n"
"			</interval>\n"
"		</default>\n"
"	</quotas>\n"
"</yandex>\n";

		Poco::XML::InputSource default_user_source(default_user_stream);
		users_config = ConfigurationPtr(new Poco::Util::XMLConfiguration(&default_user_source));
	}

	if (users_config)
		context->setUsersConfig(users_config);
	else
		throw Exception("Can't load config for users");
}

}
