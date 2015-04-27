#include "InterserverIOHTTPHandler.h"
#include <DB/Interpreters/InterserverIOHandler.h>
#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
#include <DB/IO/CompressedWriteBuffer.h>


namespace DB
{


void InterserverIOHTTPHandler::processQuery(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	HTMLForm params(request);

	std::ostringstream request_ostream;
	request_ostream << request.stream().rdbuf();
	std::string request_string = request_ostream.str();

	LOG_TRACE(log, "Request URI: " << request.getURI());
	LOG_TRACE(log, "Request body: " << request_string);

	std::istringstream request_istream(request_string);

	/// NOTE: Тут можно сделать аутентификацию, если понадобится.

	String endpoint_name = params.get("endpoint");
	bool compress = params.get("compress") == "true";

	WriteBufferFromHTTPServerResponse out(response);

	auto endpoint = server.global_context->getInterserverIOHandler().getEndpoint(endpoint_name);

	if (compress)
	{
		CompressedWriteBuffer compressed_out(out);
		endpoint->processQuery(params, compressed_out);
	}
	else
	{
		endpoint->processQuery(params, out);
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
