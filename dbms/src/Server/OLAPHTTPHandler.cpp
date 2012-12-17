#include <DB/Interpreters/executeQuery.h>
#include <DB/IO/WriteBufferFromHTTPServerResponse.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include "OLAPQueryParser.h"
#include "OLAPQueryConverter.h"

#include "OLAPHTTPHandler.h"
#include <statdaemons/Stopwatch.h>
#include <iomanip>



namespace DB
{
	
	
	void OLAPHTTPHandler::processQuery(Poco::Net::HTTPServerResponse & response, std::istream & istr)
	{
		BlockInputStreamPtr query_plan;
		
		Context context = server.global_context;
		context.setGlobalContext(server.global_context);
		
		OLAP::QueryParseResult olap_query = server.olap_parser->parse(istr);
		
		std::string clickhouse_query;
		server.olap_converter->OLAPServerQueryToClickhouse(olap_query, context, clickhouse_query);
		
		LOG_TRACE(log, "Converted query: " << clickhouse_query);
		
		ReadBufferFromString in(clickhouse_query);
		WriteBufferFromHTTPServerResponse out(response);
		
		Stopwatch watch;
		executeQuery(in, out, context, query_plan);
		watch.stop();
		
		if (query_plan)
		{
			std::stringstream log_str;
			log_str << "Query pipeline:\n";
			query_plan->dumpTree(log_str);
			LOG_DEBUG(log, log_str.str());
			
			/// Выведем информацию о том, сколько считано строк и байт.
			size_t rows = 0;
			size_t bytes = 0;
			
			query_plan->getLeafRowsBytes(rows, bytes);
			
			if (rows != 0)
			{
				LOG_INFO(log, std::fixed << std::setprecision(3)
				<< "Read " << rows << " rows, " << bytes / 1048576.0 << " MiB in " << watch.elapsedSeconds() << " sec., "
				<< static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.");
			}
		}
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
