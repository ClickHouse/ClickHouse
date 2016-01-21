#pragma once

#include "Server.h"
#include <DB/Common/CurrentMetrics.h>


namespace DB
{

class InterserverIOHTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
	InterserverIOHTTPHandler(Server & server_)
		: server(server_)
		, log(&Logger::get("InterserverIOHTTPHandler"))
	{
	}

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

private:
	Server & server;
	CurrentMetrics::Increment metric_increment{CurrentMetrics::InterserverConnection};
	Logger * log;

 	void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};

}
