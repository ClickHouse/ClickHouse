#pragma once

#include "Server.h"


namespace DB
{
	
	/// Обработчик http-запросов в формате OLAP-server.
	class OLAPHTTPHandler : public Poco::Net::HTTPRequestHandler
	{
	public:
		OLAPHTTPHandler(Server & server_)
		: server(server_)
		, log(&Logger::get("OLAPHTTPHandler"))
		{
			LOG_TRACE(log, "In constructor.");
		}
		
		void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
		
	private:
		Server & server;
		
		Logger * log;
		
		void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);
	};
	
}
