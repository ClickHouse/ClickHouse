#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{


class ParserJoin : public IParserBase
{
protected:
	const char * getName() const { return "JOIN"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Expected & expected);
};

}
