#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Запрос OPTIMIZE TABLE [db.]name [PARTITION partition] [FINAL]
  */
class ParserOptimizeQuery : public IParserBase
{
protected:
	const char * getName() const { return "OPTIMIZE query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
