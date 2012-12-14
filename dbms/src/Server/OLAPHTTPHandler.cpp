#include <DB/Interpreters/executeQuery.h>

#include "OLAPHTTPHandler.h"



namespace DB
{
	
	
	void OLAPHTTPHandler::processQuery(Poco::Net::HTTPServerResponse & response, std::istream & istr)
	{
		LOG_TRACE(log, "Doing nothing instead of processing query");
	}


	void OLAPHTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
	{
		try
		{
			LOG_TRACE(log, "Request URI: " << request.getURI());

			processQuery(response, request.stream());

			LOG_INFO(log, "Done processing query");
		}
		catch (DB::Exception & e)
		{
			response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
			std::ostream & ostr = response.send();
			std::stringstream s;
			s << "Code: " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
			ostr << s.str() << std::endl;
			LOG_ERROR(log, s.str());
		}
		catch (Poco::Exception & e)
		{
			response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
			std::ostream & ostr = response.send();
			std::stringstream s;
			s << "Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
			<< ", e.displayText() = " << e.displayText() << ", e.what() = " << e.what();
			ostr << s.str() << std::endl;
			LOG_ERROR(log, s.str());
		}
		catch (std::exception & e)
		{
			response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
			std::ostream & ostr = response.send();
			std::stringstream s;
			s << "Code: " << ErrorCodes::STD_EXCEPTION << ". " << e.what();
			ostr << s.str() << std::endl;
			LOG_ERROR(log, s.str());
		}
		catch (...)
		{
			response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR);
			std::ostream & ostr = response.send();
			std::stringstream s;
			s << "Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ". Unknown exception.";
			ostr << s.str() << std::endl;
			LOG_ERROR(log, s.str());
		}
	}
	
	
}
