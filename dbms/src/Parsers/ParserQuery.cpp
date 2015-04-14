#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/ParserInsertQuery.h>
#include <DB/Parsers/ParserDropQuery.h>
#include <DB/Parsers/ParserRenameQuery.h>
#include <DB/Parsers/ParserShowTablesQuery.h>
#include <DB/Parsers/ParserOptimizeQuery.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>
#include <DB/Parsers/ParserUseQuery.h>
#include <DB/Parsers/ParserSetQuery.h>
#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/ParserTablePropertiesQuery.h>
#include <DB/Parsers/ParserAlterQuery.h>
#include <DB/Parsers/ParserShowProcesslistQuery.h>
#include <DB/Parsers/ParserCheckQuery.h>
//#include <DB/Parsers/ParserMultiQuery.h>


namespace DB
{


bool ParserQuery::parseImpl(Pos & pos, Pos end, ASTPtr & node, Pos & max_parsed_pos, Expected & expected)
{
	ParserShowTablesQuery show_tables_p;
	ParserSelectQuery select_p;
	ParserInsertQuery insert_p;
	ParserCreateQuery create_p;
	ParserRenameQuery rename_p;
	ParserDropQuery drop_p;
	ParserAlterQuery alter_p;
	ParserUseQuery use_p;
	ParserSetQuery set_p;
	ParserOptimizeQuery optimize_p;
	ParserTablePropertiesQuery table_p;
	ParserShowProcesslistQuery show_processlist_p;
	ParserCheckQuery check_p;
//	ParserMultiQuery multi_p;

	bool res = show_tables_p.parse(pos, end, node, max_parsed_pos, expected)
		|| select_p.parse(pos, end, node, max_parsed_pos, expected)
		|| insert_p.parse(pos, end, node, max_parsed_pos, expected)
		|| create_p.parse(pos, end, node, max_parsed_pos, expected)
		|| rename_p.parse(pos, end, node, max_parsed_pos, expected)
		|| drop_p.parse(pos, end, node, max_parsed_pos, expected)
		|| alter_p.parse(pos, end, node, max_parsed_pos, expected)
		|| use_p.parse(pos, end, node, max_parsed_pos, expected)
		|| set_p.parse(pos, end, node, max_parsed_pos, expected)
		|| optimize_p.parse(pos, end, node, max_parsed_pos, expected)
		|| table_p.parse(pos, end, node, max_parsed_pos, expected)
		|| show_processlist_p.parse(pos, end, node, max_parsed_pos, expected)
		|| check_p.parse(pos, end, node, max_parsed_pos, expected);
	/*	|| multi_p.parse(pos, end, node, max_parsed_pos, expected)*/;

	if (!res && (!expected || !*expected))
		expected = "One of: SHOW TABLES, SHOW DATABASES, SHOW CREATE TABLE, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE, EXISTS, DESCRIBE, DESC, ALTER, SHOW PROCESSLIST, CHECK, opening curly brace";

	return res;
}

}
