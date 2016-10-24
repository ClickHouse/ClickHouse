#include <Poco/Util/Application.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/Util/XMLConfiguration.h>

#include <DB/Databases/DatabaseOrdinary.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Interpreters/executeQuery.h>

#include <DB/Common/Macros.h>
#include <DB/Common/ConfigProcessor.h>

#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>

#include <DB/Parsers/parseQuery.h>

#include <common/ErrorHandlers.h>

namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid-file, no dictionaries, no logging
/// Quiet mode by default
class LocalServer : public Poco::Util::ServerApplication
{
public:

	LocalServer() {}

	void initialize(Poco::Util::Application & self) override
	{
		Poco::Util::ServerApplication::initialize(self);
	}

	void defineOptions(Poco::Util::OptionSet& _options) override
	{
		Poco::Util::ServerApplication::defineOptions (_options);

		_options.addOption(
			Poco::Util::Option ("config-file", "C", "load configuration from a given file")
				.required (false)
				.repeatable (false)
				.argument ("<file>")
				.binding("config-file")
				);

		_options.addOption(
			Poco::Util::Option ("log-file", "L", "use given log file")
				.required (false)
				.repeatable (false)
				.argument ("<file>")
				.binding("logger.log")
				);

		_options.addOption(
			Poco::Util::Option ("errorlog-file", "E", "use given log file for errors only")
				.required (false)
				.repeatable (false)
				.argument ("<file>")
				.binding("logger.errorlog")
				);

		_options.addOption(
			Poco::Util::Option ("query", "Q", "[Local mode] queries to execute")
				.required (false)
				.repeatable (false)
				.argument ("<string>", true)
				.binding("query")
				);
	}

	int main(const std::vector<std::string> & args) override
	{
		if (!config().has("query")) /// Nothing to process
			return Application::EXIT_OK;

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

		processQueries();

		return Application::EXIT_OK;
	}

	void processQueries()
	{
		Logger * log = &logger();

		String queries_str = config().getString("query");
		LOG_DEBUG(log, "Executing queries: '" << queries_str << "'");
		std::vector<String> queries;
		splitMultipartQuery(queries_str, queries);

		context->setUser("default", "", Poco::Net::IPAddress{}, 0, "");

		for (const auto & query : queries)
		{
			LOG_DEBUG(log, "Executing query: '" << query << "'");
			try
			{
				ReadBufferFromString read_buf(query);
				WriteBufferFromFileDescriptor write_buf(STDOUT_FILENO);
				BlockInputStreamPtr plan;

				LOG_DEBUG(log, "executing query: " << query);
				executeQuery(read_buf, write_buf, *context, plan, nullptr);
			}
			catch (...)
			{
				tryLogCurrentException(log, "An error ocurred while executing query");
				throw;
			}
		}
	}

	void setupUsers()
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

protected:

	std::unique_ptr<Context> context;
};

}

#include <common/ApplicationServerExt.h>

YANDEX_APP_SERVER_MAIN(DB::LocalServer)
