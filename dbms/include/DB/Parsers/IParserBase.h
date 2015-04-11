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
	bool parse(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
	{
		Pos new_max_parsed_pos = pos;
		Expected new_expected = getName();

		bool res = parseImpl(pos, end, node, new_max_parsed_pos, new_expected);

		if (new_max_parsed_pos > max_parsed_pos)
			max_parsed_pos = new_max_parsed_pos;

		if (new_max_parsed_pos >= max_parsed_pos)
			expected = new_expected;

		if (!res)
			node = nullptr;

		if (pos > end)
			throw Exception("Logical error: pos > end.", ErrorCodes::LOGICAL_ERROR);

		return res;
	}

protected:
	virtual bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected) = 0;
};

}
