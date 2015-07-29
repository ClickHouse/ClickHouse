#pragma once

#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
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
		SharedPtr<WriteBufferFromHTTPServerResponse> out;
		/// Используется для выдачи ответа. Равен либо out, либо CompressedWriteBuffer(*out), в зависимости от настроек.
		SharedPtr<WriteBuffer> out_maybe_compressed;
	};

	void handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response);

	void trySendExceptionToClient(std::stringstream & s,
		Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response,
		Output & used_output);

private:
	Server & server;

	Logger * log;

	/// Функция также инициализирует used_output.
	void processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response, Output & used_output);
};

}
