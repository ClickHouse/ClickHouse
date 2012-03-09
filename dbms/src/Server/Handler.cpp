#include <Poco/URI.h>

#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Interpreters/executeQuery.h>

#include "Handler.h"


namespace DB
{


struct HTMLForm : public Poco::Net::HTMLForm
{
	HTMLForm(Poco::Net::HTTPRequest & request)
	{
		Poco::URI uri(request.getURI());
		std::istringstream istr(uri.getRawQuery());
		readUrl(istr);
	}
};


void HTTPRequestHandler::processQuery(Poco::Net::NameValueCollection & params, std::ostream & ostr, std::istream & istr)
{
	BlockInputStreamPtr query_plan;
	ReadBufferFromIStream in(istr);
	WriteBufferFromOStream out(ostr);
	Context context = server.global_context;

	executeQuery(in, out, context, query_plan);

	if (query_plan)
	{
		std::stringstream log_str;
		log_str << "Query plan:\n";
		query_plan->dumpTree(log_str);
		LOG_DEBUG(log, log_str.str());
	}
}


void HTTPRequestHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	std::ostream & ostr = response.send();
	try
	{
		LOG_TRACE(log, "Request URI: " << request.getURI());
		
		HTMLForm params(request);
		std::istream & istr = request.stream();
		processQuery(params, ostr, istr);

		LOG_INFO(log, "Done processing query");
	}
	catch (Poco::Exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.message() = " << e.message() << ", e.what() = " << e.what();
		ostr << s.str() << std::endl;
		LOG_ERROR(log, s.str());
	}
	catch (std::exception & e)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what();
		ostr << s.str() << std::endl;
		LOG_ERROR(log, s.str());
	}
	catch (...)
	{
		std::stringstream s;
		s << "Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ". Unknown exception.";
		ostr << s.str() << std::endl;
		LOG_ERROR(log, s.str());
	}
}


}
