#pragma once

#include <Poco/URI.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/HTTPRequestHandlerFactory.h>
#include <Poco/Net/HTTPRequestHandler.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/HTMLForm.h>

#include <Poco/Net/TCPServer.h>
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <Poco/Net/TCPServerConnection.h>

#include <Yandex/logger_useful.h>
#include <statdaemons/daemon.h>
#include <statdaemons/HTMLForm.h>

#include <DB/Interpreters/Context.h>
#include "OLAPQueryParser.h"
#include "OLAPQueryConverter.h"

/** Сервер предоставляет три интерфейса:
  * 1. HTTP - простой интерфейс для доступа из любых приложений.
  * 2. TCP - интерфейс для доступа из родной библиотеки, родного клиента, и для межсерверного взаимодействия.
  *    Более эффективен, так как
  *     - данные передаются по столбцам;
  *     - данные передаются со сжатием;
  *    Позволяет тонко управлять настройками и получать более подробную информацию в ответах.
  * 3. OLAP-server HTTP - интерфейс для совместимости с устаревшим демоном OLAP-server.
  */


namespace DB
{

class Server : public Daemon
{
public:
	/// Глобальные настройки севрера
	std::unique_ptr<Context> global_context;
	
	std::unique_ptr<OLAP::QueryParser> olap_parser;
	std::unique_ptr<OLAP::QueryConverter> olap_converter;

protected:
	void initialize(Application & self)
	{
		Daemon::initialize(self);
		logger().information("starting up");
	}
		
	void uninitialize()
	{
		logger().information("shutting down");
		Daemon::uninitialize();
	}

	int main(const std::vector<std::string> & args);
};

}
