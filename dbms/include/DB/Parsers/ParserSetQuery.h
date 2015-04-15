#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Запрос типа такого:
  * SET [GLOBAL] name1 = value1, name2 = value2, ...
  */
class ParserSetQuery : public IParserBase
{
protected:
	const char * getName() const { return "SET query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
