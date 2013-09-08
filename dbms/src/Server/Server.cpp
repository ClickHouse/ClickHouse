#include <Poco/Net/HTTPServerRequest.h>

#include <Yandex/ApplicationServerExt.h>

#include <DB/Interpreters/loadMetadata.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemTables.h>
#include <DB/Storages/StorageSystemDatabases.h>
#include <DB/Storages/StorageSystemProcesses.h>
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
		LOG_TRACE(log, "TCP Request. " << "Address: " << socket.peerAddress().toString());
		
		return new TCPHandler(server, socket);
	}
};


int Server::main(const std::vector<std::string> & args)
{
	Logger * log = &logger();
	
	/// Заранее инициализируем DateLUT, чтобы первая инициализация потом не влияла на измеряемую скорость выполнения.
	LOG_DEBUG(log, "Initializing DateLUT.");
	DateLUTSingleton::instance();
	LOG_TRACE(log, "Initialized DateLUT.");

	/** Контекст содержит всё, что влияет на обработку запроса:
	  *  настройки, набор функций, типов данных, агрегатных функций, баз данных...
	  */
	global_context.setGlobalContext(global_context);
	global_context.setPath(config.getString("path"));

	/// Загружаем пользователей.
	global_context.initUsersFromConfig();

	/// Загружаем квоты.
	global_context.initQuotasFromConfig();

	/// Максимальное количество одновременно выполняющихся запросов.
	global_context.getProcessList().setMaxSize(config.getInt("max_concurrent_queries", 0));

	/// Размер кэша разжатых блоков. Если нулевой - кэш отключён.
	size_t uncompressed_cache_size = config.getInt("uncompressed_cache_size", 0);
	if (uncompressed_cache_size)
		global_context.setUncompressedCache(uncompressed_cache_size);

	/// Загружаем настройки.
	Settings & settings = global_context.getSettingsRef();
	settings.setProfile(config.getString("default_profile", "default"));

	LOG_INFO(log, "Loading metadata.");
	loadMetadata(global_context);
	LOG_DEBUG(log, "Loaded metadata.");

	/// Создаём системные таблицы.
	global_context.addDatabase("system");
	
	global_context.addTable("system", "one",		StorageSystemOne::create("one"));
	global_context.addTable("system", "numbers", 	StorageSystemNumbers::create("numbers"));
	global_context.addTable("system", "tables", 	StorageSystemTables::create("tables", global_context));
	global_context.addTable("system", "databases", 	StorageSystemDatabases::create("databases", global_context));
	global_context.addTable("system", "processes", 	StorageSystemProcesses::create("processes", global_context));
		
	global_context.setCurrentDatabase(config.getString("default_database", "default"));

	bool use_olap_server = config.getBool("use_olap_http_server", false);
	Poco::Timespan keep_alive_timeout(config.getInt("keep_alive_timeout", 10), 0);
	
	Poco::ThreadPool server_pool(3, config.getInt("max_connections", 1024));
	Poco::Net::HTTPServerParams * http_params = new Poco::Net::HTTPServerParams;
	http_params->setTimeout(settings.receive_timeout);
	http_params->setKeepAliveTimeout(keep_alive_timeout);
	
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

		Poco::Net::HTTPServerParams * olap_http_params = new Poco::Net::HTTPServerParams;
		olap_http_params->setTimeout(settings.receive_timeout);
		olap_http_params->setKeepAliveTimeout(keep_alive_timeout);
		
		Poco::Net::ServerSocket olap_http_socket(Poco::Net::SocketAddress("[::]:" + config.getString("olap_http_port")));
		olap_http_socket.setReceiveTimeout(settings.receive_timeout);
		olap_http_socket.setSendTimeout(settings.send_timeout);
		olap_http_server = new Poco::Net::HTTPServer(
			new HTTPRequestHandlerFactory<OLAPHTTPHandler>(*this, "OLAPHTTPHandler-factory"),
			server_pool,
			olap_http_socket,
			olap_http_params);
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
