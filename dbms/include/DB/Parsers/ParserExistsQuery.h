#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Запрос EXISTS TABLE [db.]name
  */
class ParserExistsQuery : public IParserBase
{
protected:
	String getName() { return "EXISTS query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};

}
