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


/// Логгировать запрос в обычный лог (не в таблицу).
static void logQuery(const String & query, const Context & context)
{
	String logged_query = query;
	std::replace(logged_query.begin(), logged_query.end(), '\n', ' ');
	LOG_DEBUG(&Logger::get("executeQuery"), "(from " << context.getIPAddress().toString() << ") " << logged_query);
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

		elem.query = query;
		elem.exception = getCurrentExceptionMessage(false);

		setClientInfo(elem, context);

		try
		{
			throw;
		}
		catch (const Exception & e)
		{
			elem.stack_trace = e.getStackTrace().toString();
		}
		catch (...) {}

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

	ParserQuery parser;
	ASTPtr ast;
	size_t query_size;
	size_t max_query_size = context.getSettingsRef().max_query_size;

	try
	{
		ast = parseQuery(parser, begin, end, "");

		/// Засунем запрос в строку. Она выводится в лог и в processlist. Если запрос INSERT, то не будем включать данные для вставки.
		query_size = ast->range.second - ast->range.first;

		if (max_query_size && query_size > max_query_size)
			throw Exception("Query is too large (" + toString(query_size) + ")."
				" max_query_size = " + toString(max_query_size), ErrorCodes::QUERY_IS_TOO_LARGE);
	}
	catch (...)
	{
		/// Всё равно логгируем запрос.
		if (!internal)
		{
			String query = String(begin, begin + std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)));
			logQuery(query, context);
			onExceptionBeforeStart(query, context, current_time);
		}

		throw;
	}

	String query(begin, query_size);
	BlockIO res;

	try
	{
		if (!internal)
			logQuery(query, context);

		/// Проверка ограничений.
		checkLimits(*ast, context.getSettingsRef().limits);

		QuotaForIntervals & quota = context.getQuota();

		quota.addQuery(current_time);
		quota.checkExceeded(current_time);

		const Settings & settings = context.getSettingsRef();

		/// Положим запрос в список процессов. Но запрос SHOW PROCESSLIST класть не будем.
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

		/// Держим элемент списка процессов до конца обработки запроса.
		res.process_list_entry = process_list_entry;

		if (res.in)
		{
			if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(res.in.get()))
			{
				stream->setProgressCallback(context.getProgressCallback());
				stream->setProcessListElement(context.getProcessListElement());
			}
		}

		/// Всё, что связано с логом запросов.
		{
			QueryLogElement elem;

			elem.type = QueryLogElement::QUERY_START;

			elem.event_time = current_time;
			elem.query_start_time = current_time;

			elem.query = query;

			setClientInfo(elem, context);

			bool log_queries = settings.log_queries && !internal;

			/// Логгируем в таблицу начало выполнения запроса, если нужно.
			if (log_queries)
				context.getQueryLog().add(elem);

			/// Также дадим вызывающему коду в дальнейшем логгировать завершение запроса и эксепшен.
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
						const BlockStreamProfileInfo & info = profiling_stream->getInfo();

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

				/// Достаём стек трейс, если возможно.
				try
				{
					throw;
				}
				catch (const Exception & e)
				{
					elem.stack_trace = e.getStackTrace().toString();

					LOG_ERROR(&Logger::get("executeQuery"), elem.exception << ", Stack trace:\n\n" << elem.stack_trace);
				}
				catch (...)
				{
					LOG_ERROR(&Logger::get("executeQuery"), elem.exception);
				}

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

	/// Если в istr ещё ничего нет, то считываем кусок данных
	if (istr.buffer().size() == 0)
		istr.next();

	size_t max_query_size = context.getSettingsRef().max_query_size;

	if (istr.buffer().end() - istr.position() >= static_cast<ssize_t>(max_query_size))
	{
		/// Если оставшийся размер буфера istr достаточен, чтобы распарсить запрос до max_query_size, то парсим прямо в нём
		begin = istr.position();
		end = istr.buffer().end();
		istr.position() += end - begin;
	}
	else
	{
		/// Если нет - считываем достаточное количество данных в parse_buf
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

			/// Данные могут содержаться в распарсенной (ast_insert_query.data) и ещё не распарсенной (istr) части запроса.

			ConcatReadBuffer::ReadBuffers buffers;
			ReadBuffer buf1(const_cast<char *>(ast_insert_query->data), ast_insert_query->data ? ast_insert_query->end - ast_insert_query->data : 0, 0);

			if (ast_insert_query->data)
				buffers.push_back(&buf1);
			buffers.push_back(&istr);

			/** NOTE Нельзя читать из istr до того, как прочтём всё между ast_insert_query.data и ast_insert_query.end.
			* - потому что query.data может ссылаться на кусок памяти, использующийся в качестве буфера в istr.
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
