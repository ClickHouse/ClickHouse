#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{


class ParserQuery : public IParserBase
{
protected:
	String getName() { return "Query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};

}
