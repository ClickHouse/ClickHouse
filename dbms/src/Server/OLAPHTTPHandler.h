#pragma once

#include "Server.h"


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

	void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};

}
