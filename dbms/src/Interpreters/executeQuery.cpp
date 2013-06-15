#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/BlockIO.h>
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
	QueryProcessingStage::Enum stage)
{
	ParserQuery parser;
	ASTPtr ast;
	std::string expected;

	std::vector<char> parse_buf;
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

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw Exception("Syntax error: failed at position "
			+ Poco::NumberFormatter::format(pos - begin) + ": "
			+ std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
			+ ", expected " + (parse_res ? "end of query" : expected) + ".",
			ErrorCodes::SYNTAX_ERROR);

	LOG_DEBUG(&Logger::get("executeQuery"), std::string(begin, pos - begin));

//	formatAST(*ast, std::cerr);
//	std::cerr << std::endl;

	/// Проверка ограничений.
	checkLimits(*ast, context.getSettingsRef().limits);

	InterpreterQuery interpreter(ast, context, stage);
	interpreter.execute(ostr, &istr, query_plan);
}


BlockIO executeQuery(
	const String & query,
	Context & context,
	QueryProcessingStage::Enum stage)
{
	ParserQuery parser;
	ASTPtr ast;
	std::string expected;

	const char * begin = query.data();
	const char * end = begin + query.size();
	const char * pos = begin;

	bool parse_res = parser.parse(pos, end, ast, expected);

	/// Распарсенный запрос должен заканчиваться на конец входных данных или на точку с запятой.
	if (!parse_res || (pos != end && *pos != ';'))
		throw Exception("Syntax error: failed at position "
			+ Poco::NumberFormatter::format(pos - begin) + ": "
			+ std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
			+ ", expected " + (parse_res ? "end of query" : expected) + ".",
			ErrorCodes::SYNTAX_ERROR);

//	formatAST(*ast, std::cerr);
//	std::cerr << std::endl;

	/// Проверка ограничений.
	checkLimits(*ast, context.getSettingsRef().limits);

	InterpreterQuery interpreter(ast, context, stage);
	return interpreter.execute();
}


}
