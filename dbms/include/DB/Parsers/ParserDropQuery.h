#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Запрос типа такого:
  * DROP|DETACH TABLE [IF EXISTS] [db.]name
  *
  * Или:
  * DROP DATABASE [IF EXISTS] db
  */
class ParserDropQuery : public IParserBase
{
protected:
	String getName() { return "DROP query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected);
};

}
