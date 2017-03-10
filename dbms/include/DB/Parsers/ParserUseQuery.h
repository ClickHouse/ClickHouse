#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTUseQuery.h>


namespace DB
{

/** Запрос USE db
  */
class ParserUseQuery : public IParserBase
{
protected:
	const char * getName() const { return "USE query"; }
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);
};

}
