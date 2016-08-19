#pragma once

#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
#include <DB/Common/CurrentMetrics.h>
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

	struct Output
	{
		std::shared_ptr<WriteBufferFromHTTPServerResponse> out;
		/// Используется для выдачи ответа. Равен либо out, либо CompressedWriteBuffer(*out), в зависимости от настроек.
		std::shared_ptr<WriteBuffer> out_maybe_compressed;
	};

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

	void trySendExceptionToClient(const std::string & s,
		Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
		Output & used_output);

private:
	Server & server;

	CurrentMetrics::Increment metric_increment{CurrentMetrics::HTTPConnection};

	Logger * log;

	/// Функция также инициализирует used_output.
	void processQuery(
		Poco::Net::HTTPServerRequest & request,
		HTMLForm & params,
		Poco::Net::HTTPServerResponse & response,
		Output & used_output);
};

}
