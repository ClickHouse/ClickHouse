#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
protected:
	String getName() { return "SELECT query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};

}
