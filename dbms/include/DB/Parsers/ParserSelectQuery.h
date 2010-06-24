#ifndef DBMS_PARSERS_PARSERSELECTQUERY_H
#define DBMS_PARSERS_PARSERSELECTQUERY_H

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/IParserBase.h>
#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
protected:
	String getName() { return "SELECT query"; }

	bool parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
	{
		Pos begin = pos;

		ASTPtr select_expression_list;

		ParserWhiteSpaceOrComments ws;
		ParserString s("SELECT");
		ParserNotWordCharOrEnd nw;
		ParserNotEmptyExpressionList exp_list;
	
		ws.ignore(pos, end);

		if (!(s.ignore(pos, end, expected)
			&& nw.check(pos, end, expected)))
			return false;

		ws.ignore(pos, end);

		if (!exp_list.parse(pos, end, select_expression_list, expected))
			return false;

		ASTSelectQuery * select_query = new ASTSelectQuery(StringRange(begin, pos));
		node = select_query;
		select_query->select = select_expression_list;

		return true;
	}
};

}

#endif
