#include <sys/resource.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/DNS.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/DirectoryIterator.h>

#include <common/ApplicationServerExt.h>
#include <common/ErrorHandlers.h>

#include <ext/scope_guard.hpp>

#include <memory>
#include <experimental/optional>

#include <DB/Common/Macros.h>
#include <DB/Common/getFQDNOrHostName.h>
#include <DB/Common/StringUtils.h>

#include <DB/Interpreters/loadMetadata.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Interpreters/AsynchronousMetrics.h>

#include <DB/Storages/System/StorageSystemNumbers.h>
#include <DB/Storages/System/StorageSystemTables.h>
#include <DB/Storages/System/StorageSystemParts.h>
#include <DB/Storages/System/StorageSystemDatabases.h>
#include <DB/Storages/System/StorageSystemProcesses.h>
#include <DB/Storages/System/StorageSystemEvents.h>
#include <DB/Storages/System/StorageSystemOne.h>
#include <DB/Storages/System/StorageSystemMerges.h>
#include <DB/Storages/System/StorageSystemSettings.h>
#include <DB/Storages/System/StorageSystemZooKeeper.h>
#include <DB/Storages/System/StorageSystemReplicas.h>
#include <DB/Storages/System/StorageSystemReplicationQueue.h>
#include <DB/Storages/System/StorageSystemDictionaries.h>
#include <DB/Storages/System/StorageSystemColumns.h>
#include <DB/Storages/System/StorageSystemFunctions.h>
#include <DB/Storages/System/StorageSystemClusters.h>
#include <DB/Storages/System/StorageSystemMetrics.h>
#include <DB/Storages/System/StorageSystemAsynchronousMetrics.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReshardingWorker.h>
#include <DB/Databases/DatabaseOrdinary.h>

#include <zkutil/ZooKeeper.h>

#include "Server.h"
#include "HTTPHandler.h"
#include "ReplicasStatusHandler.h"
#include "InterserverIOHTTPHandler.h"
#include "TCPHandler.h"
#include "MetricsTransmitter.h"
#include "ConfigReloader.h"
#include "StatusFile.h"


namespace DB
{

namespace ErrorCodes
{
	extern const int NO_ELEMENTS_IN_CONFIG;
}


/// Response with "Ok.\n". Used for availability checks.
class PingRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
	{
		try
		{
			const char * data = "Ok.\n";
			response.sendBuffer(data, strlen(data));
		}
		catch (...)
		{
			tryLogCurrentException("PingRequestHandler");
		}
	}
};


/// Response with 404 and verbose description.
class NotFoundHandler : public Poco::Net::HTTPRequestHandler
{
public:
	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response) override
	{
		try
		{
			response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
			response.send() << "There is no handle " << request.getURI() << "\n\n"
				<< "Use / or /ping for health checks.\n"
				<< "Or /replicas_status for more sophisticated health checks.\n\n"
				<< "Send queries from your program with POST method or GET /?query=...\n\n"
				<< "Use clickhouse-client:\n\n"
				<< "For interactive data analysis:\n"
				<< "    clickhouse-client\n\n"
				<< "For batch query processing:\n"
				<< "    clickhouse-client --query='SELECT 1' > result\n"
				<< "    clickhouse-client < query > result\n";
		}
		catch (...)
		{
			tryLogCurrentException("NotFoundHandler");
		}
	}
};


template <typename HandlerType>
class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
	Server & server;
	Logger * log;
	std::string name;

public:
	HTTPRequestHandlerFactory(Server & server_, const std::string & name_)
		: server(server_), log(&Logger::get(name_)), name(name_) {}

	Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request) override
	{
		LOG_TRACE(log, "HTTP Request for " << name << ". "
			<< "Method: " << request.getMethod()
			<< ", Address: " << request.clientAddress().toString()
			<< ", User-Agent: " << (request.has("User-Agent") ? request.get("User-Agent") : "none"));

		const auto & uri = request.getURI();

		if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
			|| request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD)
		{
			if (uri == "/" || uri == "/ping")
				return new PingRequestHandler;
			else if (startsWith(uri, "/replicas_status"))
				return new ReplicasStatusHandler(*server.global_context);
		}

		if (uri.find('?') != std::string::npos
			|| request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
		{
			return new HandlerType(server);
		}

		if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET
			|| request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD
			|| request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
		{
			return new NotFoundHandler;
		}

		return nullptr;
	}
};


class TCPConnectionFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
	Server & server;
	Logger * log;

public:
	TCPConnectionFactory(Server & server_) : server(server_), log(&Logger::get("TCPConnectionFactory")) {}

	Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket) override
	{
		LOG_TRACE(log, "TCP Request. " << "Address: " << socket.peerAddress().toString());

		return new TCPHandler(server, socket);
	}
};


static std::vector<String> splitMultipartQuery(const String & queries)
{
	std::vector<String> queries_list;

	ASTPtr ast;
	ParserQuery parser;

	const char * begin = queries.data();
	const char * end = begin + queries.size();

	while (begin < end)
	{
		const char * pos = begin;

		ast = parseQueryAndMovePosition(parser, pos, end, "", true);
		if (!ast)
			break;

		ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(&*ast);

		if (insert && insert->data)
		{
			pos = insert->data;
			while (*pos && *pos != '\n')
				++pos;
			insert->end = pos;
		}

		queries_list.emplace_back(queries.substr(begin - queries.data(), pos - begin));

		begin = pos;
		while (isWhitespaceASCII(*begin) || *begin == ';')
			++begin;
	}

	return queries_list;
}


int Server::main(const std::vector<std::string> & args)
{
	Logger * log = &logger();

	std::string path = config().getString("path", "");
	Poco::trimInPlace(path);
	if (path.empty())
	{
		if (!is_local_server)
			throw Exception("path configuration parameter is empty");
		else
		{
			std::string tmp_dir_template = Poco::Path::temp() + "clickhouse.XXXXXX";
			path = mkdtemp(&tmp_dir_template[0]);
			LOG_INFO(log, "Parameter path is empty, will use '" + path + "' as working dir");
		}
	}
	if (path.back() != '/')
		path += '/';

	LOG_INFO(log, "local-mode=" << is_local_server << ", is_daemon=" << config().getBool("application.runAsDaemon", false));
	LOG_INFO(log, "path=" << path << " application.path=" << config().getString("application.path", ""));

	/** Context contains all that query execution is dependent:
	  *  settings, available functions, data types, aggregate functions, databases...
	  */
	global_context = std::make_unique<Context>();

	global_context->setGlobalContext(*global_context);
	global_context->setPath(path);

	std::string default_database = config().getString("default_database", "default");

	/// Create directories for 'path' and for default database, if not exist.
	if (!is_local_server)
	{
		Poco::File(path + "data/" + default_database).createDirectories();
		Poco::File(path + "metadata/" + default_database).createDirectories();
	}

	StatusFile status{path + "status"};

	/// Try to increase limit on number of open files.
	{
		rlimit rlim;
		if (getrlimit(RLIMIT_NOFILE, &rlim))
			throw Poco::Exception("Cannot getrlimit");

		if (rlim.rlim_cur == rlim.rlim_max)
		{
			LOG_DEBUG(log, "rlimit on number of file descriptors is " << rlim.rlim_cur);
		}
		else
		{
			rlim_t old = rlim.rlim_cur;
			rlim.rlim_cur = config().getUInt("max_open_files", rlim.rlim_max);
			int rc = setrlimit(RLIMIT_NOFILE, &rlim);
			if (rc != 0)
				//throwFromErrno(std::string("Cannot setrlimit (tried rlim_cur = ") + std::to_string(rlim.rlim_cur) + "). Try to specify max_open_files according to your system limits");
				throw DB::Exception(std::string("Cannot setrlimit (tried rlim_cur = ") + std::to_string(rlim.rlim_cur) + "). Try to specify max_open_files according to your system limits. error: " + strerror(errno));

			LOG_DEBUG(log, "Set rlimit on number of file descriptors to " << rlim.rlim_cur << " (was " << old << ").");
		}
	}

	static ServerErrorHandler error_handler;
	Poco::ErrorHandler::set(&error_handler);

	/// Initialize DateLUT early, to not interfere with running time of first query.
	LOG_DEBUG(log, "Initializing DateLUT.");
	DateLUT::instance();
	LOG_TRACE(log, "Initialized DateLUT.");

	/// Directory with temporary data for processing of hard queries.
	{
		std::string tmp_path = config().getString("tmp_path", path + "tmp/");
		global_context->setTemporaryPath(tmp_path);
		Poco::File(tmp_path).createDirectories();

		/// Clearing old temporary files.
		Poco::DirectoryIterator dir_end;
		for (Poco::DirectoryIterator it(tmp_path); it != dir_end; ++it)
		{
			if (it->isFile() && startsWith(it.name(), "tmp"))
			{
				LOG_DEBUG(log, "Removing old temporary file " << it->path());
				it->remove();
			}
		}
	}

	/** Directory with 'flags': files indicating temporary settings for the server set by system administrator.
	  * Flags may be cleared automatically after being applied by the server.
	  * Examples: do repair of local data; clone all replicated tables from replica.
	  */
	{
		std::string flags_path = path + "flags/";
		Poco::File(flags_path).createDirectories();
		global_context->setFlagsPath(flags_path);
	}

	bool has_zookeeper = false;
	if (!is_local_server && config().has("zookeeper"))
	{
		global_context->setZooKeeper(std::make_shared<zkutil::ZooKeeper>(config(), "zookeeper"));
		has_zookeeper = true;
	}

	if (!is_local_server && config().has("interserver_http_port"))
	{
		String this_host = config().getString("interserver_http_host", "");

		if (this_host.empty())
		{
			this_host = getFQDNOrHostName();
			LOG_DEBUG(log, "Configuration parameter 'interserver_http_host' doesn't exist or exists and empty. Will use '" + this_host + "' as replica host.");
		}

		String port_str = config().getString("interserver_http_port");
		int port = parse<int>(port_str);

		if (port < 0 || port > 0xFFFF)
			throw Exception("Out of range 'interserver_http_port': " + toString(port), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

		global_context->setInterserverIOAddress(this_host, port);
	}

	if (config().has("macros"))
		global_context->setMacros(Macros(config(), "macros"));

	/// Initialize automatic config updater
	std::unique_ptr<ConfigReloader> config_reloader;
	if (!is_local_server)
	{
		std::string main_config_path = config().getString("config-file", "config.xml");
		std::string users_config_path = config().getString("users_config", main_config_path);
		std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");
		config_reloader = std::make_unique<ConfigReloader>(main_config_path, users_config_path, include_from_path, global_context.get());
	}
	else
	{
		ConfigurationPtr users_config;

		if ((config().has("users_config") || config().has("config-file")))
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
			global_context->setUsersConfig(users_config);
		else
			throw Exception("Can't load config for users");
	}


	/// Limit on total number of coucurrently executed queries.
	global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

	/// Size of cache for uncompressed blocks. Zero means disabled.
	size_t uncompressed_cache_size = parse<size_t>(config().getString("uncompressed_cache_size", "0"));
	if (uncompressed_cache_size)
		global_context->setUncompressedCache(uncompressed_cache_size);

	/// Workaround
	if (is_local_server && !config().has("mark_cache_size"))
		config().setString("mark_cache_size", "5368709120");

	/// Size of cache for marks (index of MergeTree family of tables). It is necessary.
	size_t mark_cache_size = parse<size_t>(config().getString("mark_cache_size"));
	if (mark_cache_size)
		global_context->setMarkCache(mark_cache_size);

	/// Load global settings from default profile.
	Settings & settings = global_context->getSettingsRef();
	LOG_DEBUG(log, "settings " << &settings);
	global_context->setSetting("profile", config().getString("default_profile", "default"));

	LOG_DEBUG(log, "profile done");

	if (!is_local_server)
	{
		LOG_INFO(log, "Loading metadata.");
		loadMetadata(*global_context);
		LOG_DEBUG(log, "Loaded metadata.");

		/// Create system tables.
		attachSystemTables(path, has_zookeeper);
	}
	else
	{
		/// Init dummy default DB
		global_context->addDatabase(default_database, std::make_shared<DatabaseMemory>(default_database));
	}

	global_context->setCurrentDatabase(default_database);

	bool has_resharding_worker = false;
	if (has_zookeeper && config().has("resharding"))
	{
		auto resharding_worker = std::make_shared<ReshardingWorker>(config(), "resharding", *global_context);
		global_context->setReshardingWorker(resharding_worker);
		resharding_worker->start();
		has_resharding_worker = true;
	}

	LOG_DEBUG(log, "zookeeper done");

	SCOPE_EXIT(
		/** Ask to cancel background jobs all table engines,
		  *  and also query_log.
		  * It is important to do early, not in destructor of Context, because
		  *  table engines could use Context on destroy.
		  */
		LOG_INFO(log, "Shutting down storages.");
		global_context->shutdown();
		LOG_DEBUG(log, "Shutted down storages.");

		/** Explicitly destroy Context. It is more convenient than in destructor of Server, becuase logger is still available.
		  * At this moment, no one could own shared part of Context.
		  */
		global_context.reset();

		LOG_DEBUG(log, "Destroyed global context.");
	);

	if (!is_local_server)
	{
		const std::string listen_host = config().getString("listen_host", "::");

		Poco::Timespan keep_alive_timeout(config().getInt("keep_alive_timeout", 10), 0);

		Poco::ThreadPool server_pool(3, config().getInt("max_connections", 1024));
		Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
		http_params->setTimeout(settings.receive_timeout);
		http_params->setKeepAliveTimeout(keep_alive_timeout);

		/// For testing purposes, user may omit tcp_port or http_port in configuration file.

		/// HTTP
		std::experimental::optional<Poco::Net::HTTPServer> http_server;
		if (config().has("http_port"))
		{
			Poco::Net::SocketAddress http_socket_address;

			try
			{
				http_socket_address = Poco::Net::SocketAddress(listen_host, config().getInt("http_port"));
			}
			catch (const Poco::Net::DNSException & e)
			{
				/// Better message when IPv6 is disabled on host.
				if (e.code() == EAI_ADDRFAMILY)
				{
					LOG_ERROR(log, "Cannot resolve listen_host (" << listen_host + "), error: " << e.message() << ". "
						"If it is an IPv6 address and your host has disabled IPv6, then consider to specify IPv4 address to listen in <listen_host> element of configuration file. Example: <listen_host>0.0.0.0</listen_host>");
				}

				throw;
			}

			Poco::Net::ServerSocket http_socket(http_socket_address);
			http_socket.setReceiveTimeout(settings.receive_timeout);
			http_socket.setSendTimeout(settings.send_timeout);
			http_server.emplace(
				new HTTPRequestHandlerFactory<HTTPHandler>(*this, "HTTPHandler-factory"),
				server_pool,
				http_socket,
				http_params);
		}

		/// TCP
		std::experimental::optional<Poco::Net::TCPServer> tcp_server;
		if (config().has("tcp_port"))
		{
			Poco::Net::ServerSocket tcp_socket(Poco::Net::SocketAddress(listen_host, config().getInt("tcp_port")));
			tcp_socket.setReceiveTimeout(settings.receive_timeout);
			tcp_socket.setSendTimeout(settings.send_timeout);
			tcp_server.emplace(
				new TCPConnectionFactory(*this),
				server_pool,
				tcp_socket,
				new Poco::Net::TCPServerParams);
		}

		/// At least one of TCP and HTTP servers must be created.
		if (!http_server && !tcp_server)
			throw Exception("No 'tcp_port' and 'http_port' is specified in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

		/// Interserver IO HTTP
		std::experimental::optional<Poco::Net::HTTPServer> interserver_io_http_server;
		if (config().has("interserver_http_port"))
		{
			Poco::Net::ServerSocket interserver_io_http_socket(Poco::Net::SocketAddress(listen_host, config().getInt("interserver_http_port")));
			interserver_io_http_socket.setReceiveTimeout(settings.receive_timeout);
			interserver_io_http_socket.setSendTimeout(settings.send_timeout);
			interserver_io_http_server.emplace(
				new HTTPRequestHandlerFactory<InterserverIOHTTPHandler>(*this, "InterserverIOHTTPHandler-factory"),
				server_pool,
				interserver_io_http_socket,
				http_params);
		}

		http_server->start();
		tcp_server->start();
		if (interserver_io_http_server)
			interserver_io_http_server->start();

		LOG_INFO(log, "Ready for connections.");

		SCOPE_EXIT(
			LOG_DEBUG(log, "Received termination signal.");

			if (has_resharding_worker)
			{
				LOG_INFO(log, "Shutting down resharding thread");
				auto & resharding_worker = global_context->getReshardingWorker();
				if (resharding_worker.isStarted())
					resharding_worker.shutdown();
				LOG_DEBUG(log, "Shut down resharding thread");
			}

			LOG_DEBUG(log, "Waiting for current connections to close.");

			is_cancelled = true;

			http_server->stop();
			tcp_server->stop();

			LOG_DEBUG(log, "Closed all connections.");

			config_reloader.reset();
		);

		/// try to load dictionaries immediately, throw on error and die
		try
		{
			if (!config().getBool("dictionaries_lazy_load", true))
			{
				global_context->tryCreateDictionaries();
				global_context->tryCreateExternalDictionaries();
			}
		}
		catch (...)
		{
			LOG_ERROR(log, "Caught exception while loading dictionaries.");
			throw;
		}

		/// This object will periodically calculate some metrics.
		AsynchronousMetrics async_metrics(*global_context);

		system_database->attachTable("asynchronous_metrics", StorageSystemAsynchronousMetrics::create("asynchronous_metrics", async_metrics));

		const auto metrics_transmitter = config().getBool("use_graphite", true)
			? std::make_unique<MetricsTransmitter>(async_metrics)
			: nullptr;

		waitForTerminationRequest();
	}
	else if (config().has("query"))
	{
		auto queries_str = config().getString("query");
		LOG_DEBUG(log, "Executing queries: '" << queries_str << "'");
		auto queries = splitMultipartQuery(queries_str);

		auto local_context = std::make_unique<Context>(*global_context);
		local_context->setGlobalContext(*global_context);
		local_context->setUser("default", "", Poco::Net::IPAddress{}, 0, "");
		local_context->setDefaultFormat("PrettyCompact");

		for (const auto & query : queries)
		{
			LOG_DEBUG(log, "Executing query: '" << query << "'");
			try
			{
				ReadBufferFromString read_buf(query);
				WriteBufferFromFileDescriptor write_buf(STDOUT_FILENO);
				BlockInputStreamPtr plan;

				LOG_DEBUG(log, "executing query: " << query);
				executeQuery(read_buf, write_buf, *local_context, plan, nullptr);
			}
			catch (...)
			{
				tryLogCurrentException(log, "An error ocurred while executing query");
				throw;
			}
		}
	}

	return Application::EXIT_OK;
}


void Server::attachSystemTables(const std::string & path, bool has_zookeeper) const
{
	if (!global_context->isDatabaseExist("system"))
	{
		Poco::File(path + "data/system").createDirectories();
		Poco::File(path + "metadata/system").createDirectories();

		auto system_database = std::make_shared<DatabaseOrdinary>("system", path + "metadata/system/");
		global_context->addDatabase("system", system_database);

		/// 'has_force_restore_data_flag' is true, to not fail on loading query_log table, if it is corrupted.
		system_database->loadTables(*global_context, nullptr, true);
	}

	DatabasePtr system_database = global_context->getDatabase("system");

	system_database->attachTable("one",			StorageSystemOne::create("one"));
	system_database->attachTable("numbers", 	StorageSystemNumbers::create("numbers"));
	system_database->attachTable("numbers_mt", 	StorageSystemNumbers::create("numbers_mt", true));
	system_database->attachTable("tables", 		StorageSystemTables::create("tables"));
	system_database->attachTable("parts", 		StorageSystemParts::create("parts"));
	system_database->attachTable("databases", 	StorageSystemDatabases::create("databases"));
	system_database->attachTable("processes", 	StorageSystemProcesses::create("processes"));
	system_database->attachTable("settings", 	StorageSystemSettings::create("settings"));
	system_database->attachTable("events", 		StorageSystemEvents::create("events"));
	system_database->attachTable("metrics", 	StorageSystemMetrics::create("metrics"));
	system_database->attachTable("merges",		StorageSystemMerges::create("merges"));
	system_database->attachTable("replicas",	StorageSystemReplicas::create("replicas"));
	system_database->attachTable("replication_queue", StorageSystemReplicationQueue::create("replication_queue"));
	system_database->attachTable("dictionaries", StorageSystemDictionaries::create("dictionaries"));
	system_database->attachTable("columns",   	StorageSystemColumns::create("columns"));
	system_database->attachTable("functions", 	StorageSystemFunctions::create("functions"));
	system_database->attachTable("clusters", 	StorageSystemClusters::create("clusters", *global_context));

	if (has_zookeeper)
		system_database->attachTable("zookeeper", StorageSystemZooKeeper::create("zookeeper"));
}

}

YANDEX_APP_SERVER_MAIN(DB::Server);
