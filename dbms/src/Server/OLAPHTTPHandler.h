#pragma once

#include "Server.h"
#include <DB/Common/CurrentMetrics.h>


namespace DB
{

/// Обработчик http-запросов в формате OLAP-server.
class OLAPHTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
	OLAPHTTPHandler(Server & server_)
		: server(server_),
		log(&Logger::get("OLAPHTTPHandler")),
		profile(Poco::Util::Application::instance().config().getString("olap_compatibility.profile"))
	{
	}

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

private:
	Server & server;
	Logger * log;
	const String profile;
	CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

	void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};

}
