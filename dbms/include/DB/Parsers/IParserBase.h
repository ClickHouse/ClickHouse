#ifndef DBMS_PARSERS_IPARSERBASE_H
#define DBMS_PARSERS_IPARSERBASE_H

#include <list>
#include <Poco/SharedPtr.h>

#include <DB/Core/Types.h>
#include <DB/Parsers/IParser.h>

#include <iostream>

namespace DB
{

/** Базовый класс для большинства парсеров
  */
class IParserBase : public IParser
{
public:
	bool parse(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		expected = getName();

		Pos begin = pos;
		bool res = parseImpl(pos, end, node, expected);

		return res;
	}
protected:
	virtual bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected) = 0;
};

}

#endif
