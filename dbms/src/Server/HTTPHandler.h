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
	    LOG_TRACE(log, "In constructor.");
	}

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

private:
	Server & server;

	Logger * log;

 	void processQuery(Poco::Net::NameValueCollection & params, std::ostream & ostr, std::istream & istr);
};

}
