#pragma once

#include <DB/Parsers/IParserBase.h>


namespace DB
{


class ParserQuery : public IParserBase
{
protected:
	const char * getName() const { return "Query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
