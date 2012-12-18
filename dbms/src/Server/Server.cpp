#include <Poco/Net/HTTPServerRequest.h>

#include <Yandex/ApplicationServerExt.h>

#include <DB/Interpreters/loadMetadata.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemTables.h>
#include <DB/Storages/StorageSystemDatabases.h>
#include <DB/Storages/StorageSystemOne.h>

#include "Server.h"
#include "HTTPHandler.h"
#include "OLAPHTTPHandler.h"
#include "TCPHandler.h"


namespace DB
{

/// Отвечает "Ok.\n", если получен любой GET запрос. Используется для проверки живости.
class PingRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
	PingRequestHandler()
	{
	    LOG_TRACE((&Logger::get("PingRequestHandler")), "Ping request.");
	}

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
	{
		response.send() << "Ok." << std::endl;
	}
};


template<typename HandlerType>
class HTTPRequestHandlerFactory : public Poco::Net::HTTPRequestHandlerFactory
{
private:
	Server & server;
	Logger * log;
	std::string name;
	
public:
	HTTPRequestHandlerFactory(Server & server_, const std::string & name_)
		: server(server_), log(&Logger::get(name_)), name(name_) {}
	
	Poco::Net::HTTPRequestHandler * createRequestHandler(const Poco::Net::HTTPServerRequest & request)
	{
		LOG_TRACE(log, "HTTP Request for " << name << ". "
			<< "Method: " << request.getMethod()
			<< ", Address: " << request.clientAddress().toString()
			<< ", User-Agent: " << (request.has("User-Agent") ? request.get("User-Agent") : "none"));

		if (request.getURI().find('?') != std::string::npos || request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST)
			return new HandlerType(server);
		else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
			return new PingRequestHandler();
		else
			return 0;
	}
};


class TCPConnectionFactory : public Poco::Net::TCPServerConnectionFactory
{
private:
	Server & server;
	Logger * log;
	
public:
	TCPConnectionFactory(Server & server_) : server(server_), log(&Logger::get("TCPConnectionFactory")) {}

	Poco::Net::TCPServerConnection * createConnection(const Poco::Net::StreamSocket & socket)
	{
		LOG_TRACE(log, "TCP Request. " << "Address: " << socket.address().toString());
		
		return new TCPHandler(server, socket);
	}
};


int Server::main(const std::vector<std::string> & args)
{
	Logger * log = &logger();
	
	/// Заранее инициализируем DateLUT, чтобы первая инициализация потом не влияла на измеряемую скорость выполнения.
	LOG_DEBUG(log, "Initializing DateLUT.");
	Yandex::DateLUTSingleton::instance();
	LOG_TRACE(log, "Initialized DateLUT.");

	/** Контекст содержит всё, что влияет на обработку запроса:
	  *  настройки, набор функций, типов данных, агрегатных функций, баз данных...
	  */
	global_context.setGlobalContext(global_context);
	global_context.setPath(config.getString("path"));

	/// Загружаем настройки.
	Settings settings;

	settings.asynchronous 		= config.getBool("asynchronous", 	settings.asynchronous);
	settings.max_block_size 	= config.getInt("max_block_size", 	settings.max_block_size);
	settings.max_query_size 	= config.getInt("max_query_size", 	settings.max_query_size);
	settings.max_threads 		= config.getInt("max_threads", 		settings.max_threads);
	settings.interactive_delay 	= config.getInt("interactive_delay", settings.interactive_delay);
	settings.connect_timeout 	= Poco::Timespan(config.getInt("connect_timeout", DBMS_DEFAULT_CONNECT_TIMEOUT_SEC), 0);
	settings.connect_timeout_with_failover_ms
		= Poco::Timespan(config.getInt("connect_timeout_with_failover_ms", DBMS_DEFAULT_CONNECT_TIMEOUT_WITH_FAILOVER_MS) * 1000);
	settings.receive_timeout 	= Poco::Timespan(config.getInt("receive_timeout", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0);
	settings.send_timeout 		= Poco::Timespan(config.getInt("send_timeout", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0);
	settings.poll_interval		= config.getInt("poll_interval", 	settings.poll_interval);
	settings.max_distributed_connections = config.getInt("max_distributed_connections", settings.max_distributed_connections);
	settings.distributed_connections_pool_size =
		config.getInt("distributed_connections_pool_size", settings.distributed_connections_pool_size);
	settings.connections_with_failover_max_tries =
		config.getInt("connections_with_failover_max_tries", settings.connections_with_failover_max_tries);

	global_context.setSettings(settings);

	LOG_INFO(log, "Loading metadata.");
	loadMetadata(global_context);
	LOG_DEBUG(log, "Loaded metadata.");

	/// Создаём системные таблицы.
	global_context.addDatabase("system");
	
	global_context.addTable("system", "one",		new StorageSystemOne("one"));
	global_context.addTable("system", "numbers", 	new StorageSystemNumbers("numbers"));
	global_context.addTable("system", "tables", 	new StorageSystemTables("tables", global_context));
	global_context.addTable("system", "databases", 	new StorageSystemDatabases("databases", global_context));
		
	global_context.setCurrentDatabase(config.getString("default_database", "default"));

	bool use_olap_server = config.getBool("use_olap_http_server", false);
	
	Poco::ThreadPool server_pool(3, config.getInt("max_connections", 128));
	Poco::Net::HTTPServerParams * http_params = new Poco::Net::HTTPServerParams;
	http_params->setTimeout(settings.receive_timeout);
	
	/// HTTP
	Poco::Net::ServerSocket http_socket(Poco::Net::SocketAddress("[::]:" + config.getString("http_port")));
	http_socket.setReceiveTimeout(settings.receive_timeout);
	http_socket.setSendTimeout(settings.send_timeout);
	Poco::Net::HTTPServer http_server(
		new HTTPRequestHandlerFactory<HTTPHandler>(*this, "HTTPHandler-factory"),
		server_pool,
		http_socket,
		http_params);
	
	/// TCP
	Poco::Net::ServerSocket tcp_socket(Poco::Net::SocketAddress("[::]:" + config.getString("tcp_port")));
	tcp_socket.setReceiveTimeout(settings.receive_timeout);
	tcp_socket.setSendTimeout(settings.send_timeout);
	Poco::Net::TCPServer tcp_server(
		new TCPConnectionFactory(*this),
		server_pool,
		tcp_socket,
		new Poco::Net::TCPServerParams);
	
	/// OLAP HTTP
	Poco::SharedPtr<Poco::Net::HTTPServer> olap_http_server;
	if (use_olap_server)
	{
		olap_parser = new OLAP::QueryParser();
		olap_converter = new OLAP::QueryConverter(config);
		
		Poco::Net::ServerSocket olap_http_socket(Poco::Net::SocketAddress("[::]:" + config.getString("olap_http_port")));
		olap_http_socket.setReceiveTimeout(settings.receive_timeout);
		olap_http_socket.setSendTimeout(settings.send_timeout);
		olap_http_server = new Poco::Net::HTTPServer(
			new HTTPRequestHandlerFactory<OLAPHTTPHandler>(*this, "OLAPHTTPHandler-factory"),
			server_pool,
			olap_http_socket,
			http_params);
	}

	http_server.start();
	tcp_server.start();
	if (use_olap_server)
		olap_http_server->start();

	LOG_INFO(log, "Ready for connections.");
	
	waitForTerminationRequest();
	
	LOG_DEBUG(log, "Received termination signal.");
	is_cancelled = true;

	http_server.stop();
	tcp_server.stop();
	if (use_olap_server)
		olap_http_server->stop();
	
	return Application::EXIT_OK;
}

}


YANDEX_APP_SERVER_MAIN(DB::Server);
