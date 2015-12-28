#pragma once

#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/ParserQueryWithOutput.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionElementParsers.h>
#include <DB/Parsers/ASTShowProcesslistQuery.h>


namespace DB
{

/** Запрос SHOW PROCESSLIST
  */
class ParserShowProcesslistQuery : public ParserQueryWithOutput
{
protected:
	const char * getName() const { return "SHOW PROCESSLIST query"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
	{
		Pos begin = pos;

		ParserString s_show("SHOW", true, true);
		ParserString s_processlist("PROCESSLIST", true, true);

		ASTShowProcesslistQuery * query = new ASTShowProcesslistQuery;
		ASTPtr query_ptr = query;

		ws.ignore(pos, end);

		if (!s_show.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		if (!s_processlist.ignore(pos, end, max_parsed_pos, expected))
			return false;

		ws.ignore(pos, end);

		/// FORMAT format_name
		if (!parseFormat(*query, pos, end, node, max_parsed_pos, expected))
			return false;

		query->range = StringRange(begin, pos);
		node = query_ptr;

		return true;
	}
};

}
