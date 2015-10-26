#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ASTQueryWithOutput.h>

namespace DB
{

/** Парсер для запросов поддерживающих секцию FORMAT.
  */
class ParserQueryWithOutput : public IParserBase
{
protected:
	bool parseFormat(ASTQueryWithOutput & query, Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected);

protected:
	ParserWhiteSpaceOrComments ws;
};

}
