#pragma once

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
	bool parse(Pos & pos, Pos end, ASTPtr & node, Expected & expected)
	{
		expected = getName();
		bool res = parseImpl(pos, end, node, expected);

		if (!res)
			node = nullptr;

		if (pos > end)
			throw Exception("Logical error: pos > end.", ErrorCodes::LOGICAL_ERROR);

		return res;
	}
protected:
	virtual bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected) = 0;
};

}
