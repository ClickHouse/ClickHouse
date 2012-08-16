#include <Poco/Net/HTTPServerRequest.h>

#include <Yandex/ApplicationServerExt.h>

#include <DB/Functions/FunctionsLibrary.h>
#include <DB/Interpreters/loadMetadata.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemTables.h>
#include <DB/Storages/StorageSystemDatabases.h>
#include <DB/Storages/StorageSystemOne.h>

#include "Server.h"
#include "HTTPHandler.h"
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


Poco::Net::HTTPRequestHandler * HTTPRequestHandlerFactory::createRequestHandler(
	const Poco::Net::HTTPServerRequest & request)
{
	LOG_TRACE(log, "HTTP Request. "
		<< "Method: " << request.getMethod()
		<< ", Address: " << request.clientAddress().toString()
		<< ", User-Agent: " << (request.has("User-Agent") ? request.get("User-Agent") : "none"));

	if (request.getURI().find('?') != std::string::npos)
		return new HTTPHandler(server);
	else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
		return new PingRequestHandler();
	else
		return 0;
}


Poco::Net::TCPServerConnection * TCPConnectionFactory::createConnection(const Poco::Net::StreamSocket & socket)
{
	LOG_TRACE(log, "TCP Request. " << "Address: " << socket.address().toString());

	return new TCPHandler(server, socket);
}


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

	Settings settings;
	
	settings.asynchronous 		= config.getBool("asynchronous", 	settings.asynchronous);
	settings.max_block_size 	= config.getInt("max_block_size", 	settings.max_block_size);
	settings.max_query_size 	= config.getInt("max_query_size", 	settings.max_query_size);
	settings.max_threads 		= config.getInt("max_threads", 		settings.max_threads);
	settings.interactive_delay 	= config.getInt("interactive_delay", settings.interactive_delay);
	settings.connect_timeout 	= Poco::Timespan(config.getInt("connect_timeout", DBMS_DEFAULT_CONNECT_TIMEOUT_SEC), 0);
	settings.receive_timeout 	= Poco::Timespan(config.getInt("receive_timeout", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0);
	settings.send_timeout 		= Poco::Timespan(config.getInt("send_timeout", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0);
	settings.poll_interval		= config.getInt("poll_interval", 	settings.poll_interval);

	global_context.setSettings(settings);
	
	Poco::Net::ServerSocket http_socket(Poco::Net::SocketAddress("[::]:" + config.getString("http_port")));
	Poco::Net::ServerSocket tcp_socket(Poco::Net::SocketAddress("[::]:" + config.getString("tcp_port")));

	http_socket.setReceiveTimeout(settings.receive_timeout);
	http_socket.setSendTimeout(settings.send_timeout);
	tcp_socket.setReceiveTimeout(settings.receive_timeout);
	tcp_socket.setSendTimeout(settings.send_timeout);

	Poco::ThreadPool server_pool(2, config.getInt("max_connections", 128));

	Poco::Net::HTTPServerParams * http_params = new Poco::Net::HTTPServerParams;
 	http_params->setTimeout(settings.receive_timeout);

	Poco::Net::HTTPServer http_server(
		new HTTPRequestHandlerFactory(*this),
		server_pool,
		http_socket,
		http_params);

	Poco::Net::TCPServer tcp_server(
		new TCPConnectionFactory(*this),
		server_pool,
		tcp_socket,
		new Poco::Net::TCPServerParams);

	http_server.start();
	tcp_server.start();

	LOG_INFO(log, "Ready for connections.");
	
	waitForTerminationRequest();

	http_server.stop();
	tcp_server.stop();
	
	return Application::EXIT_OK;
}

}


YANDEX_APP_SERVER_MAIN(DB::Server);
