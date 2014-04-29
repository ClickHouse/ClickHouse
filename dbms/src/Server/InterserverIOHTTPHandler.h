#pragma once

#include "Server.h"


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

	Logger * log;

 	void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
};

}
