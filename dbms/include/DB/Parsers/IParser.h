#pragma once

#include <list>
#include <memory>

#include <DB/Core/Defines.h>
#include <DB/Core/Types.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

using Poco::SharedPtr;

typedef const char * Expected;


/** Интерфейс для классов-парсеров
  */
class IParser
{
public:
	typedef const char * Pos;

	/** Получить текст о том, что парсит этот парсер. */
	virtual const char * getName() const = 0;

	/** Распарсить кусок текста с позиции pos, но не дальше конца строки (end - позиция после конца строки),
	  * переместить указатель pos на максимальное место, до которого удалось распарсить,
	  * вернуть в случае успеха true и результат в node, если он нужен, иначе false,
	  * в expected записать, что ожидалось в максимальной позиции,
	  *  до которой удалось распарсить, если парсинг был неуспешным,
	  *  или что парсит этот парсер, если парсинг был успешным.
	  * Строка, в которую входит диапазон [begin, end) может быть не 0-terminated.
	  */
	virtual bool parse(Pos & pos, Pos end, ASTPtr & node, Expected & expected) = 0;

	bool ignore(Pos & pos, Pos end, Expected & expected)
	{
		ASTPtr ignore_node;
		return parse(pos, end, ignore_node, expected);
	}

	bool ignore(Pos & pos, Pos end)
	{
		Expected expected;
		return ignore(pos, end, expected);
	}

	/** То же самое, но не двигать позицию и не записывать результат в node.
	  */
	bool check(Pos & pos, Pos end, Expected & expected)
	{
		Pos begin = pos;
		ASTPtr node;
		if (!parse(pos, end, node, expected))
		{
			pos = begin;
			return false;
		}
		else
			return true;
	}

	virtual ~IParser() {}
};

typedef std::unique_ptr<IParser> ParserPtr;


/** Из позиции в (возможно многострочном) запросе получить номер строки и номер столбца в строке.
  * Используется в сообщении об ошибках.
  */
inline std::pair<size_t, size_t> getLineAndCol(IParser::Pos begin, IParser::Pos pos)
{
	size_t line = 0;

	IParser::Pos nl;
	while (nullptr != (nl = reinterpret_cast<IParser::Pos>(memchr(begin, '\n', pos - begin))))
	{
		++line;
		begin = nl + 1;
	}

	/// Нумеруются с единицы.
	return { line + 1, pos - begin + 1 };
}


/** Получить сообщение о синтаксической ошибке.
  */
inline std::string getSyntaxErrorMessage(
	bool parse_res,							/// false, если не удалось распарсить; true, если удалось, но не до конца строки или точки с запятой.
	IParser::Pos begin,
	IParser::Pos end,
	IParser::Pos pos,
	Expected expected,
	const std::string & description = "")
{
	std::stringstream message;

	message << "Syntax error";

	if (!description.empty())
		message << " (" << description << ")";

	message << ": failed at position " << (pos - begin + 1);

	/// Если запрос многострочный.
	IParser::Pos nl = reinterpret_cast<IParser::Pos>(memchr(begin, '\n', end - begin));
	if (nullptr != nl && nl + 1 != end)
	{
		size_t line = 0;
		size_t col = 0;
		std::tie(line, col) = getLineAndCol(begin, pos);

		message << " (line " << line << ", col " << col << ")";
	}

	message << ": " << std::string(pos, std::min(SHOW_CHARS_ON_SYNTAX_ERROR, end - pos))
		<< ", expected " << (parse_res ? "end of query" : expected) << ".";

	return message.str();
}

}
