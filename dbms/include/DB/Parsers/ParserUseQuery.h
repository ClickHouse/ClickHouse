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
	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, const char *& expected)
	{
		Pos begin = pos;

		ParserWhiteSpaceOrComments ws;
		ParserString s_use("USE", true, true);
		ParserIdentifier name_p;

		ASTPtr database;

		ws.ignore(pos, end);

		if (!s_use.ignore(pos, end, expected))
			return false;

		ws.ignore(pos, end);

		if (!name_p.parse(pos, end, database, expected))
			return false;

		ws.ignore(pos, end);

		ASTUseQuery * query = new ASTUseQuery(StringRange(begin, pos));
		node = query;

		query->database = dynamic_cast<ASTIdentifier &>(*database).name;

		return true;
	}
};

}
