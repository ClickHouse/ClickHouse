#pragma once

#include <Poco/Net/HTMLForm.h>

#include "Server.h"


namespace DB
{

class HTTPRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
	HTTPRequestHandler(Server & server_)
		: server(server_)
		, log(&Logger::get("HTTPRequestHandler"))
	{
	    LOG_TRACE(log, "In constructor.");
	}

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

private:
	Server & server;

	Logger * log;

	void processQuery(Poco::Net::NameValueCollection & params, std::ostream & ostr, std::istream & istr);
};

}
