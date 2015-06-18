#include <DB/Common/ProfileEvents.h>

#include <DB/IO/ConcatReadBuffer.h>

#include <DB/DataStreams/BlockIO.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTShowProcesslistQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/parseQuery.h>

#include <DB/Interpreters/Quota.h>
#include <DB/Interpreters/InterpreterFactory.h>
#include <DB/Interpreters/executeQuery.h>


namespace DB
{


static void checkLimits(const IAST & ast, const Limits & limits)
{
	if (limits.max_ast_depth)
		ast.checkDepth(limits.max_ast_depth);
	if (limits.max_ast_elements)
		ast.checkSize(limits.max_ast_elements);
}


static void logQuery(const String & query, const Context & context)
{
	String logged_query = query;
	std::replace(logged_query.begin(), logged_query.end(), '\n', ' ');
	LOG_DEBUG(&Logger::get("executeQuery"), "(from " << context.getIPAddress().toString() << ") " << logged_query);
}


static std::tuple<ASTPtr, BlockIO> executeQueryImpl(
	IParser::Pos begin,
	IParser::Pos end,
	Context & context,
	bool internal,
	QueryProcessingStage::Enum stage)
{
	ProfileEvents::increment(ProfileEvents::Query);

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
		logQuery(String(begin, begin + std::min(end - begin, static_cast<ptrdiff_t>(max_query_size))), context);

		throw;
	}

	String query(begin, query_size);

	if (!internal)
		logQuery(query, context);

	/// Проверка ограничений.
	checkLimits(*ast, context.getSettingsRef().limits);

	QuotaForIntervals & quota = context.getQuota();
	time_t current_time = time(0);

	quota.checkExceeded(current_time);

	/// Положим запрос в список процессов. Но запрос SHOW PROCESSLIST класть не будем.
	ProcessList::EntryPtr process_list_entry;
	if (!internal && nullptr == typeid_cast<const ASTShowProcesslistQuery *>(&*ast))
	{
		process_list_entry = context.getProcessList().insert(
			query, context.getUser(), context.getCurrentQueryId(), context.getIPAddress(),
			context.getSettingsRef().limits.max_memory_usage,
			context.getSettingsRef().queue_max_wait_ms.totalMilliseconds(),
			context.getSettingsRef().replace_running_query);

		context.setProcessListElement(&process_list_entry->get());
	}

	BlockIO res;

	try
	{
		auto interpreter = InterpreterFactory::get(ast, context, stage);
		res = interpreter->execute();

		/// Держим элемент списка процессов до конца обработки запроса.
		res.process_list_entry = process_list_entry;
	}
	catch (...)
	{
		quota.addError(current_time);
		throw;
	}

	quota.addQuery(current_time);

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
	bool internal,
	QueryProcessingStage::Enum stage)
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

	std::tie(ast, streams) = executeQueryImpl(begin, end, context, internal, stage);

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
			context.getFormatFactory().getInput(
				format, data_istr, streams.out_sample, context.getSettings().max_insert_block_size)};

		copyData(*in, *streams.out);
	}

	if (streams.in)
	{
		const ASTQueryWithOutput * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());

		String format_name = ast_query_with_output && ast_query_with_output->format
			? typeid_cast<const ASTIdentifier &>(*ast_query_with_output->format).name
			: context.getDefaultFormat();

		BlockOutputStreamPtr out = context.getFormatFactory().getOutput(format_name, ostr, streams.in_sample);

		copyData(*streams.in, *out);
	}
}

}
