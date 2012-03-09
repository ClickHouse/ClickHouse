#include <Poco/Net/HTTPServerRequest.h>

#include <Yandex/ApplicationServerExt.h>

#include <DB/Functions/FunctionsLibrary.h>
#include <DB/Interpreters/loadMetadata.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemOne.h>

#include "Server.h"
#include "Handler.h"


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
		return new HTTPRequestHandler(server);
	else if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
		return new PingRequestHandler();
	else
		return 0;
}


int Server::main(const std::vector<std::string> & args)
{
	/// Заранее инициализируем DateLUT, чтобы первая инициализация потом не влияла на измеряемую скорость выполнения.
	Yandex::DateLUTSingleton::instance();
	
	global_context.path = config.getString("path");
	global_context.functions = FunctionsLibrary::get();
	global_context.aggregate_function_factory	= new AggregateFunctionFactory;
	global_context.data_type_factory			= new DataTypeFactory;
	global_context.storage_factory				= new StorageFactory;

	loadMetadata(global_context);

	(*global_context.databases)["system"]["one"] 		= new StorageSystemOne("one");
	(*global_context.databases)["system"]["numbers"] 	= new StorageSystemNumbers("numbers");
		
	global_context.current_database = config.getString("default_database", "default");
	
	Poco::Net::ServerSocket socket(Poco::Net::SocketAddress("[::]:" + config.getString("http_port")));

	Poco::ThreadPool server_pool(2, config.getInt("max_threads", 128));

	Poco::Net::HTTPServer server(
		new HTTPRequestHandlerFactory(*this),
		server_pool,
		socket,
		new Poco::Net::HTTPServerParams);

	server.start();
	waitForTerminationRequest();
	server.stop();
	return Application::EXIT_OK;
}

}


YANDEX_APP_SERVER_MAIN(DB::Server);
