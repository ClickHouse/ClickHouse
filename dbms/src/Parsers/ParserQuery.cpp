#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ParserInsertQuery.h>
#include <DB/Parsers/ParserDropQuery.h>
#include <DB/Parsers/ParserRenameQuery.h>
#include <DB/Parsers/ParserShowTablesQuery.h>
#include <DB/Parsers/ParserQuery.h>


namespace DB
{


bool ParserQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, String & expected)
{
	ParserShowTablesQuery show_tables_p;
	ParserSelectQuery select_p;
	ParserInsertQuery insert_p;
	ParserCreateQuery create_p;
	ParserRenameQuery rename_p;
	ParserDropQuery drop_p;

	return show_tables_p.parse(pos, end, node, expected)
		|| select_p.parse(pos, end, node, expected)
		|| insert_p.parse(pos, end, node, expected)
		|| create_p.parse(pos, end, node, expected)
		|| rename_p.parse(pos, end, node, expected)
		|| drop_p.parse(pos, end, node, expected);
}

}