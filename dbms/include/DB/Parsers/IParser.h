#pragma once

#include <list>
#include <Poco/SharedPtr.h>

#include <DB/Core/Types.h>
#include <DB/Parsers/IAST.h>


namespace DB
{

using Poco::SharedPtr;


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
	  * Везде предполагается, что строка, в которую входит диапазон [begin, end) 0-terminated.
	  */
	virtual bool parse(Pos & pos, Pos end, ASTPtr & node, const char *& expected) = 0;

	bool ignore(Pos & pos, Pos end, const char *& expected)
	{
		ASTPtr ignore_node;
		return parse(pos, end, ignore_node, expected);
	}
	
	bool ignore(Pos & pos, Pos end)
	{
		const char * expected;
		return ignore(pos, end, expected);
	}

	/** То же самое, но не двигать позицию и не записывать результат в node.
	  */
	bool check(Pos & pos, Pos end, const char *& expected)
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

typedef SharedPtr<IParser> ParserPtr;

}
