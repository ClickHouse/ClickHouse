#include <DB/Common/ProfileEvents.h>
#include <DB/Common/formatReadable.h>

#include <DB/IO/ConcatReadBuffer.h>

#include <DB/DataStreams/BlockIO.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>

#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTShowProcesslistQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Interpreters/Quota.h>
#include <DB/Interpreters/InterpreterFactory.h>
#include <DB/Interpreters/ProcessList.h>
#include <DB/Interpreters/QueryLog.h>
#include <DB/Interpreters/executeQuery.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
	extern const int QUERY_IS_TOO_LARGE;
}


static void checkLimits(const IAST & ast, const Limits & limits)
{
	if (limits.max_ast_depth)
		ast.checkDepth(limits.max_ast_depth);
	if (limits.max_ast_elements)
		ast.checkSize(limits.max_ast_elements);
}


static String joinLines(const String & query)
{
	String res = query;
	std::replace(res.begin(), res.end(), '\n', ' ');
	return res;
}


/// Log query into text log (not into system table).
static void logQuery(const String & query, const Context & context)
{
	LOG_DEBUG(&Logger::get("executeQuery"), "(from " << context.getIPAddress().toString() << ") " << joinLines(query));
}


/// Call this inside catch block.
static void setExceptionStackTrace(QueryLogElement & elem)
{
	try
	{
		throw;
	}
	catch (const Exception & e)
	{
		elem.stack_trace = e.getStackTrace().toString();
	}
	catch (...) {}
}


/// Log exception (with query info) into text log (not into system table).
static void logException(Context & context, QueryLogElement & elem)
{
	LOG_ERROR(&Logger::get("executeQuery"), elem.exception
		<< " (from " << context.getIPAddress().toString() << ")"
		<< " (in query: " << joinLines(elem.query) << ")"
		<< (!elem.stack_trace.empty() ? ", Stack trace:\n\n" + elem.stack_trace : ""));
}


static void setClientInfo(QueryLogElement & elem, Context & context)
{
	elem.interface = context.getInterface();
	elem.http_method = context.getHTTPMethod();
	elem.ip_address = context.getIPAddress();
	elem.user = context.getUser();
	elem.query_id = context.getCurrentQueryId();
}


static void onExceptionBeforeStart(const String & query, Context & context, time_t current_time)
{
	/// Эксепшен до начала выполнения запроса.
	context.getQuota().addError(current_time);

	bool log_queries = context.getSettingsRef().log_queries;

	/// Логгируем в таблицу начало выполнения запроса, если нужно.
	if (log_queries)
	{
		QueryLogElement elem;

		elem.type = QueryLogElement::EXCEPTION_BEFORE_START;

		elem.event_time = current_time;
		elem.query_start_time = current_time;

		elem.query = query.substr(0, context.getSettingsRef().log_queries_cut_to_length);
		elem.exception = getCurrentExceptionMessage(false);

		setClientInfo(elem, context);
		setExceptionStackTrace(elem);
		logException(context, elem);

		context.getQueryLog().add(elem);
	}
}


static std::tuple<ASTPtr, BlockIO> executeQueryImpl(
	IParser::Pos begin,
	IParser::Pos end,
	Context & context,
	bool internal,
	QueryProcessingStage::Enum stage)
{
	ProfileEvents::increment(ProfileEvents::Query);
	time_t current_time = time(0);

	const Settings & settings = context.getSettingsRef();

	ParserQuery parser;
	ASTPtr ast;
	size_t query_size;
	size_t max_query_size = settings.max_query_size;

	try
	{
		ast = parseQuery(parser, begin, end, "");

		/// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
		query_size = ast->range.second - ast->range.first;

		if (max_query_size && query_size > max_query_size)
			throw Exception("Query is too large (" + toString(query_size) + ")."
				" max_query_size = " + toString(max_query_size), ErrorCodes::QUERY_IS_TOO_LARGE);
	}
	catch (...)
	{
		/// Anyway log query.
		if (!internal)
		{
			String query = String(begin, begin + std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)));
			logQuery(query.substr(0, settings.log_queries_cut_to_length), context);
			onExceptionBeforeStart(query, context, current_time);
		}

		throw;
	}

	String query(begin, query_size);
	BlockIO res;

	try
	{
		if (!internal)
			logQuery(query.substr(0, settings.log_queries_cut_to_length), context);

		/// Check the limits.
		checkLimits(*ast, settings.limits);

		QuotaForIntervals & quota = context.getQuota();

		quota.addQuery(current_time);
		quota.checkExceeded(current_time);

		/// Put query to process list. But don't put SHOW PROCESSLIST query itself.
		ProcessList::EntryPtr process_list_entry;
		if (!internal && nullptr == typeid_cast<const ASTShowProcesslistQuery *>(&*ast))
		{
			process_list_entry = context.getProcessList().insert(
				query,
				context.getUser(),
				context.getCurrentQueryId(),
				context.getIPAddress(),
				settings);

			context.setProcessListElement(&process_list_entry->get());
		}

		auto interpreter = InterpreterFactory::get(ast, context, stage);
		res = interpreter->execute();

		/// Hold element of process list till end of query execution.
		res.process_list_entry = process_list_entry;

		if (res.in)
		{
			if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(res.in.get()))
			{
				stream->setProgressCallback(context.getProgressCallback());
				stream->setProcessListElement(context.getProcessListElement());
			}
		}

		/// Everything related to query log.
		{
			QueryLogElement elem;

			elem.type = QueryLogElement::QUERY_START;

			elem.event_time = current_time;
			elem.query_start_time = current_time;

			elem.query = query.substr(0, settings.log_queries_cut_to_length);

			setClientInfo(elem, context);

			bool log_queries = settings.log_queries && !internal;

			/// Log into system table start of query execution, if need.
			if (log_queries)
				context.getQueryLog().add(elem);

			/// Also make possible for caller to log successful query finish and exception during execution.
			res.finish_callback = [elem, &context, log_queries] (IBlockInputStream * stream) mutable
			{
				ProcessListElement * process_list_elem = context.getProcessListElement();

				if (!process_list_elem)
					return;

				double elapsed_seconds = process_list_elem->watch.elapsedSeconds();

				elem.type = QueryLogElement::QUERY_FINISH;

				elem.event_time = time(0);
				elem.query_duration_ms = elapsed_seconds * 1000;

				elem.read_rows = process_list_elem->progress.rows;
				elem.read_bytes = process_list_elem->progress.bytes;

				auto memory_usage = process_list_elem->memory_tracker.getPeak();
				elem.memory_usage = memory_usage > 0 ? memory_usage : 0;

				if (stream)
				{
					if (IProfilingBlockInputStream * profiling_stream = dynamic_cast<IProfilingBlockInputStream *>(stream))
					{
						const BlockStreamProfileInfo & info = profiling_stream->getProfileInfo();

						elem.result_rows = info.rows;
						elem.result_bytes = info.bytes;
					}
				}

				if (elem.read_rows != 0)
				{
					LOG_INFO(&Logger::get("executeQuery"), std::fixed << std::setprecision(3)
						<< "Read " << elem.read_rows << " rows, "
						<< formatReadableSizeWithBinarySuffix(elem.read_bytes) << " in " << elapsed_seconds << " sec., "
						<< static_cast<size_t>(elem.read_rows / elapsed_seconds) << " rows/sec., "
						<< formatReadableSizeWithBinarySuffix(elem.read_bytes / elapsed_seconds) << "/sec.");
				}

				if (log_queries)
					context.getQueryLog().add(elem);
			};

			res.exception_callback = [elem, &context, log_queries, current_time] () mutable
			{
				context.getQuota().addError(current_time);

				elem.type = QueryLogElement::EXCEPTION_WHILE_PROCESSING;

				elem.event_time = time(0);
				elem.query_duration_ms = 1000 * (elem.event_time - elem.query_start_time);
				elem.exception = getCurrentExceptionMessage(false);

				ProcessListElement * process_list_elem = context.getProcessListElement();

				if (process_list_elem)
				{
					double elapsed_seconds = process_list_elem->watch.elapsedSeconds();

					elem.query_duration_ms = elapsed_seconds * 1000;

					elem.read_rows = process_list_elem->progress.rows;
					elem.read_bytes = process_list_elem->progress.bytes;

					auto memory_usage = process_list_elem->memory_tracker.getPeak();
					elem.memory_usage = memory_usage > 0 ? memory_usage : 0;
				}

				setExceptionStackTrace(elem);
				logException(context, elem);

				if (log_queries)
					context.getQueryLog().add(elem);
			};

			if (!internal && res.in)
			{
				std::stringstream log_str;
				log_str << "Query pipeline:\n";
				res.in->dumpTree(log_str);
				LOG_DEBUG(&Logger::get("executeQuery"), log_str.str());
			}
		}
	}
	catch (...)
	{
		if (!internal)
			onExceptionBeforeStart(query, context, current_time);

		throw;
	}

	return std::make_tuple(ast, res);
}


BlockIO executeQuery(
	const String & query,
	Context & context,
	bool internal,
	QueryProcessingStage::Enum stage)
{
	BlockIO streams;
	std::tie(std::ignore, streams) = executeQueryImpl(query.data(), query.data() + query.size(), context, internal, stage);
	return streams;
}


void executeQuery(
	ReadBuffer & istr,
	WriteBuffer & ostr,
	Context & context,
	BlockInputStreamPtr & query_plan,
	std::function<void(const String &)> set_content_type)
{
	PODArray<char> parse_buf;
	const char * begin;
	const char * end;

	/// If 'istr' is empty now, fetch next data into buffer.
	if (istr.buffer().size() == 0)
		istr.next();

	size_t max_query_size = context.getSettingsRef().max_query_size;

	if (istr.buffer().end() - istr.position() >= static_cast<ssize_t>(max_query_size))
	{
		/// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
		begin = istr.position();
		end = istr.buffer().end();
		istr.position() += end - begin;
	}
	else
	{
		/// If not - copy enough data into 'parse_buf'.
		parse_buf.resize(max_query_size);
		parse_buf.resize(istr.read(&parse_buf[0], max_query_size));
		begin = &parse_buf[0];
		end = begin + parse_buf.size();
	}

	ASTPtr ast;
	BlockIO streams;

	std::tie(ast, streams) = executeQueryImpl(begin, end, context, false, QueryProcessingStage::Complete);

	try
	{
		if (streams.out)
		{
			const ASTInsertQuery * ast_insert_query = dynamic_cast<const ASTInsertQuery *>(ast.get());

			if (!ast_insert_query)
				throw Exception("Logical error: query requires data to insert, but it is not INSERT query", ErrorCodes::LOGICAL_ERROR);

			String format = ast_insert_query->format;
			if (format.empty())
				format = "Values";

			/// Data could be in parsed (ast_insert_query.data) and in not parsed yet (istr) part of query.

			ConcatReadBuffer::ReadBuffers buffers;
			ReadBuffer buf1(const_cast<char *>(ast_insert_query->data), ast_insert_query->data ? ast_insert_query->end - ast_insert_query->data : 0, 0);

			if (ast_insert_query->data)
				buffers.push_back(&buf1);
			buffers.push_back(&istr);

			/** NOTE Must not read from 'istr' before read all between 'ast_insert_query.data' and 'ast_insert_query.end'.
			  * - because 'query.data' could refer to memory piece, used as buffer for 'istr'.
			  */

			ConcatReadBuffer data_istr(buffers);

			BlockInputStreamPtr in{
				context.getInputFormat(
					format, data_istr, streams.out_sample, context.getSettings().max_insert_block_size)};

			copyData(*in, *streams.out);
		}

		if (streams.in)
		{
			const ASTQueryWithOutput * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());

			String format_name = ast_query_with_output && (ast_query_with_output->getFormat() != nullptr)
				? typeid_cast<const ASTIdentifier &>(*ast_query_with_output->getFormat()).name
				: context.getDefaultFormat();

			BlockOutputStreamPtr out = context.getOutputFormat(format_name, ostr, streams.in_sample);

			if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(streams.in.get()))
			{
				/// NOTE Progress callback takes shared ownership of 'out'.
				stream->setProgressCallback([out] (const Progress & progress) { out->onProgress(progress); });
			}

			if (set_content_type)
				set_content_type(out->getContentType());

			copyData(*streams.in, *out);
		}
	}
	catch (...)
	{
		streams.onException();
		throw;
	}

	streams.onFinish();
}

}
