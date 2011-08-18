#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTNameTypePair.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTCreateQuery.h>

#include <DB/Parsers/CommonParsers.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/ParserCreateQuery.h>


namespace DB
{


bool ParserIdentifierWithOptionalParameters::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	ParserIdentifier non_parametric;
	ParserFunction parametric;
	
	Pos begin = pos;

	if (parametric.parse(pos, end, node, expected))
	{
		return true;
	}
	pos = begin;

	ASTPtr ident;
	if (non_parametric.parse(pos, end, ident, expected))
	{
		ASTFunction * func = new ASTFunction(StringRange(begin, pos));
		node = func;
		func->name = dynamic_cast<ASTIdentifier &>(*ident).name;
		return true;
	}
	pos = begin;

	return false;
}


bool ParserNameTypePair::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	ParserIdentifier name_parser;
	ParserIdentifierWithOptionalParameters type_parser;
	ParserWhiteSpaceOrComments ws_parser;
	
	Pos begin = pos;

	ASTPtr name, type;
	if (name_parser.parse(pos, end, name, expected)
		&& ws_parser.ignore(pos, end, expected)
		&& type_parser.parse(pos, end, type, expected))
	{
		ASTNameTypePair * name_type_pair = new ASTNameTypePair(StringRange(begin, pos));
		node = name_type_pair;
		name_type_pair->name = dynamic_cast<ASTIdentifier &>(*name).name;
		name_type_pair->type = type;
		name_type_pair->children.push_back(type);
		return true;
	}
	
	pos = begin;
	return false;
}


bool ParserCreateQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	Pos begin = pos;

	ParserWhiteSpaceOrComments ws;
	ParserString s_create("CREATE", true, true);
	ParserString s_attach("ATTACH", true, true);
	ParserString s_table("TABLE", true, true);
	ParserString s_lparen("(");
	ParserString s_rparen(")");
	ParserString s_engine("ENGINE", true);
	ParserString s_eq("=");
	ParserIdentifier name_p;
	ParserList columns_p(new ParserNameTypePair, new ParserString(","), false);
	ParserIdentifierWithOptionalParameters storage_p;

	ASTPtr name;
	ASTPtr columns;
	ASTPtr storage;
	bool attach = false;

	ws.ignore(pos, end);

	if (!s_create.ignore(pos, end, expected))
	{
		if (s_attach.ignore(pos, end, expected))
			attach = true;
		else
			return false;
	}

	ws.ignore(pos, end);

	if (!s_table.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!name_p.parse(pos, end, name, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_lparen.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!columns_p.parse(pos, end, columns, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_rparen.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_engine.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!s_eq.ignore(pos, end, expected))
		return false;

	ws.ignore(pos, end);

	if (!storage_p.parse(pos, end, storage, expected))
		return false;

	ASTCreateQuery * query = new ASTCreateQuery(StringRange(begin, pos));
	node = query;

	query->attach = attach;
	query->name = dynamic_cast<ASTIdentifier &>(*name).name;
	query->columns = columns;
	query->storage = storage;

	query->children.push_back(columns);
	query->children.push_back(storage);

	return true;
}


}
