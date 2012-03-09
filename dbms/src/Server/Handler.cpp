#include <iomanip>

#include <Poco/URI.h>
#include <Poco/NumberParser.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

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

	/// Некоторые настройки могут быть переопределены в запросе.
	if (params.has("asynchronous"))
		context.settings.asynchronous = 0 != Poco::NumberParser::parseUnsigned(params.get("asynchronous"));
	if (params.has("max_block_size"))
		context.settings.max_block_size = Poco::NumberParser::parseUnsigned(params.get("max_block_size"));
	if (params.has("max_query_size"))
		context.settings.max_query_size = Poco::NumberParser::parseUnsigned(params.get("max_query_size"));
	if (params.has("max_threads"))
		context.settings.max_threads = Poco::NumberParser::parseUnsigned(params.get("max_threads"));

	Stopwatch watch;
	executeQuery(in, out, context, query_plan);
	watch.stop();

	if (query_plan)
	{
		std::stringstream log_str;
		log_str << "Query plan:\n";
		query_plan->dumpTree(log_str);
		LOG_DEBUG(log, log_str.str());

		/// Выведем информацию о том, сколько считано строк и байт.
		BlockInputStreams leaves = query_plan->getLeaves();
		size_t rows = 0;
		size_t bytes = 0;

		for (BlockInputStreams::const_iterator it = leaves.begin(); it != leaves.end(); ++it)
		{
			if (const IProfilingBlockInputStream * profiling = dynamic_cast<const IProfilingBlockInputStream *>(&**it))
			{
				const BlockStreamProfileInfo & info = profiling->getInfo();
				rows += info.rows;
				bytes += info.bytes;
			}
		}

		if (rows != 0)
		{
			LOG_INFO(log, std::fixed << std::setprecision(3)
				<< "Read " << rows << " rows, " << bytes / 1048576.0 << " MB in " << watch.elapsedSeconds() << " sec., "
				<< static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MB/sec.");
		}
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
