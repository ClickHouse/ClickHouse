#pragma once

#include <Poco/Net/HTMLForm.h>

#include "Server.h"


namespace DB
{

class HTTPHandler : public Poco::Net::HTTPRequestHandler
{
public:
	HTTPHandler(Server & server_)
		: server(server_)
		, log(&Logger::get("HTTPHandler"))
	{
	}

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

private:
	Server & server;

	Logger * log;

 	void processQuery(Poco::Net::NameValueCollection & params, Poco::Net::HTTPServerResponse & response, std::istream & istr, bool readonly);
};

}
