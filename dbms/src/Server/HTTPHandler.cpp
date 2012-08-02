#include <iomanip>

#include <Poco/URI.h>
#include <Poco/NumberParser.h>

#include <statdaemons/Stopwatch.h>

#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBufferFromIStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ConcatReadBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Interpreters/executeQuery.h>

#include "HTTPHandler.h"



namespace DB
{


/// Позволяет получать параметры URL даже если запрос POST.
struct HTMLForm : public Poco::Net::HTMLForm
{
	HTMLForm(Poco::Net::HTTPRequest & request)
	{
		Poco::URI uri(request.getURI());
		std::istringstream istr(uri.getRawQuery());
		readUrl(istr);
	}
};


void HTTPHandler::processQuery(Poco::Net::NameValueCollection & params, std::ostream & ostr, std::istream & istr)
{
	BlockInputStreamPtr query_plan;
	
	/** Часть запроса может быть передана в параметре query, а часть - POST-ом.
	  * В таком случае, считается, что запрос - параметр query, затем перевод строки, а затем - данные POST-а.
	  */
	std::string query_param = params.get("query", "");
	if (!query_param.empty())
		query_param += '\n';
	
	ReadBufferFromString in_param(query_param);
	SharedPtr<ReadBuffer> in_post = new ReadBufferFromIStream(istr);
	SharedPtr<ReadBuffer> in_post_maybe_compressed;

	/// Если указано decompress, то будем разжимать то, что передано POST-ом.
	if (0 != Poco::NumberParser::parseUnsigned(params.get("decompress", "0")))
		in_post_maybe_compressed = new CompressedReadBuffer(*in_post);
	else
		in_post_maybe_compressed = in_post;

	ConcatReadBuffer in(in_param, *in_post_maybe_compressed);

	/// Если указано compress, то будем сжимать результат.
	SharedPtr<WriteBuffer> out = new WriteBufferFromOStream(ostr);
	SharedPtr<WriteBuffer> out_maybe_compressed;

	if (0 != Poco::NumberParser::parseUnsigned(params.get("compress", "0")))
		out_maybe_compressed = new CompressedWriteBuffer(*out);
	else
		out_maybe_compressed = out;
	
	Context context = server.global_context;
	context.setGlobalContext(server.global_context);

	/// Некоторые настройки могут быть переопределены в запросе.
	Settings new_settings = context.getSettings();

	if (params.has("asynchronous"))
		new_settings.asynchronous = 0 != Poco::NumberParser::parseUnsigned(params.get("asynchronous"));
	if (params.has("max_block_size"))
		new_settings.max_block_size = Poco::NumberParser::parseUnsigned(params.get("max_block_size"));
	if (params.has("max_query_size"))
		new_settings.max_query_size = Poco::NumberParser::parseUnsigned(params.get("max_query_size"));
	if (params.has("max_threads"))
		new_settings.max_threads = Poco::NumberParser::parseUnsigned(params.get("max_threads"));
	if (params.has("database"))
		context.setCurrentDatabase(params.get("database"));

	context.setSettings(new_settings);

	Stopwatch watch;
	executeQuery(in, *out_maybe_compressed, context, query_plan);
	watch.stop();

	if (query_plan)
	{
		std::stringstream log_str;
		log_str << "Query pipeline:\n";
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
				<< "Read " << rows << " rows, " << bytes / 1048576.0 << " MiB in " << watch.elapsedSeconds() << " sec., "
				<< static_cast<size_t>(rows / watch.elapsedSeconds()) << " rows/sec., " << bytes / 1048576.0 / watch.elapsedSeconds() << " MiB/sec.");
		}
	}
}


void HTTPHandler::handleRequest(Poco::Net::HTTPServerRequest & request, Poco::Net::HTTPServerResponse & response)
{
	bool is_browser = false;
	if (request.has("Accept"))
	{
		String accept = request.get("Accept");
		if (0 == strncmp(accept.c_str(), "text/html", strlen("text/html")))
			is_browser = true;
	}

	if (is_browser)
		response.setContentType("text/plain; charset=UTF-8");
	
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
