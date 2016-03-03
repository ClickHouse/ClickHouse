#include "InterserverIOHTTPHandler.h"
#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/ReadBufferFromIStream.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int ABORTED;
	extern const int POCO_EXCEPTION;
	extern const int STD_EXCEPTION;
	extern const int UNKNOWN_EXCEPTION;
}


void InterserverIOHTTPHandler::processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	HTMLForm params(request);

	LOG_TRACE(log, "Request URI: " << request.getURI());

	/// NOTE: Тут можно сделать аутентификацию, если понадобится.

	String endpoint_name = params.get("endpoint");
	bool compress = params.get("compress") == "true";

	ReadBufferFromIStream body(request.stream());

	WriteBufferFromHTTPServerResponse out(response);

	auto endpoint = server.global_context->getInterserverIOHandler().getEndpoint(endpoint_name);

	if (compress)
	{
		CompressedWriteBuffer compressed_out(out);
		endpoint->processQuery(params, body, compressed_out);
	}
	else
	{
		endpoint->processQuery(params, body, out);
	}

	out.finalize();
}


void InterserverIOHTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	/// Для того, чтобы работал keep-alive.
	if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1)
		response.setChunkedTransferEncoding(true);

	try
	{
		processQuery(request, response);
		LOG_INFO(log, "Done processing query");
	}
	catch (Exception & e)
	{
		response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
		std::stringstream s;
		s << "Code: " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
		if (!response.sent())
			response.send() << s.str() << std::endl;

		if (e.code() == ErrorCodes::ABORTED)
			LOG_INFO(log, s.str());	/// Отдача куска на удалённый сервер была остановлена из-за остановки сервера или удаления таблицы.
		else
			LOG_ERROR(log, s.str());
	}
	catch (Poco::Exception & e)
	{
		response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
		std::stringstream s;
		s << "Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
		if (!response.sent())
			response.send() << s.str() << std::endl;
		LOG_ERROR(log, s.str());
	}
	catch (std::exception & e)
	{
		response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
		std::stringstream s;
		s << "Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what();
		if (!response.sent())
			response.send() << s.str() << std::endl;
		LOG_ERROR(log, s.str());
	}
	catch (...)
	{
		response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
		std::stringstream s;
		s << "Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ". Unknown exception.";
		if (!response.sent())
			response.send() << s.str() << std::endl;
		LOG_ERROR(log, s.str());
	}
}


}
