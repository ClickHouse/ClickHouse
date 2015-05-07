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
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
	{
		Pos begin = pos;

		ParserWhiteSpaceOrComments ws;
		ParserString s_use("USE", true, true);
		ParserIdentifier name_p;

		ASTPtr database;

		ws.ignore(pos, end);

		if (!s_use.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!name_p.parse(pos, end, database, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		ASTUseQuery * query = new ASTUseQuery(StringRange(begin, pos));
		node = query;

		query->database = typeid_cast<ASTIdentifier &>(*database).name;

		return true;
	}
};

}
