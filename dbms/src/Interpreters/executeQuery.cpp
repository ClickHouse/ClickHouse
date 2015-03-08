#include <DB/Common/ProfileEvents.h>

#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/BlockIO.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTShowProcesslistQuery.h>
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


void executeQuery(
	ReadBuffer & istr,
	WriteBuffer & ostr,
	Context & context,
	BlockInputStreamPtr & query_plan,
	bool internal,
	QueryProcessingStage::Enum stage)
{
	ProfileEvents::increment(ProfileEvents::Query);

	ParserQuery parser;
	ASTPtr ast;
	Expected expected = "";

	PODArray<char> parse_buf;
	const char * begin;
	const char * end;

	/// Если в istr ещё ничего нет, то считываем кусок данных
	if (istr.buffer().size() == 0)
		istr.next();

	size_t max_query_size = context.getSettings().max_query_size;

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

	const char * pos = begin;

	bool parse_res = parser.parse(pos, end, ast, expected);

	if (pos == begin && (end == begin || *pos == ';'))
		throw Exception("Empty query", ErrorCodes::EMPTY_QUERY);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw Exception(getSyntaxErrorMessage(parse_res, begin, end, pos, expected),
			ErrorCodes::SYNTAX_ERROR);

	/// Засунем запрос в строку. Она выводится в лог и в processlist. Если запрос INSERT, то не будем включать данные для вставки.
	auto insert = typeid_cast<const ASTInsertQuery *>(&*ast);
	size_t query_size = (insert && insert->data) ? (insert->data - begin) : (pos - begin);

	if (query_size > max_query_size)
		throw Exception("Query is too large (" + toString(query_size) + ")."
			" max_query_size = " + toString(max_query_size), ErrorCodes::QUERY_IS_TOO_LARGE);

	String query(begin, query_size);

	LOG_DEBUG(&Logger::get("executeQuery"), query);

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

	/// Проверка ограничений.
	checkLimits(*ast, context.getSettingsRef().limits);

	QuotaForIntervals & quota = context.getQuota();
	time_t current_time = time(0);

	quota.checkExceeded(current_time);

	try
	{
		InterpreterQuery interpreter(ast, context, stage);
		interpreter.execute(ostr, &istr, query_plan);
	}
	catch (...)
	{
		quota.addError(current_time);
		throw;
	}

	quota.addQuery(current_time);
}


BlockIO executeQuery(
	const String & query,
	Context & context,
	bool internal,
	QueryProcessingStage::Enum stage)
{
	ProfileEvents::increment(ProfileEvents::Query);

	ParserQuery parser;
	ASTPtr ast;
	Expected expected = "";

	const char * begin = query.data();
	const char * end = begin + query.size();
	const char * pos = begin;

	bool parse_res = parser.parse(pos, end, ast, expected);

	if (pos == begin && (end == begin || *pos == ';'))
		throw Exception("Empty query", ErrorCodes::EMPTY_QUERY);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw Exception(getSyntaxErrorMessage(parse_res, begin, end, pos, expected),
			ErrorCodes::SYNTAX_ERROR);

	/// Проверка ограничений.
	checkLimits(*ast, context.getSettingsRef().limits);

	QuotaForIntervals & quota = context.getQuota();
	time_t current_time = time(0);

	quota.checkExceeded(current_time);

	BlockIO res;

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

	try
	{
		InterpreterQuery interpreter(ast, context, stage);
		res = interpreter.execute();

		/// Держим элемент списка процессов до конца обработки запроса.
		res.process_list_entry = process_list_entry;
	}
	catch (...)
	{
		quota.addError(current_time);
		throw;
	}

	quota.addQuery(current_time);
	return res;
}


}
