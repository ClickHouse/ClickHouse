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
	ParserAlterQuery alter_p;
	ParserUseQuery use_p;
	ParserSetQuery set_p;
	ParserOptimizeQuery optimize_p;
	ParserTablePropertiesQuery table_p;
	
	bool res = show_tables_p.parse(pos, end, node, expected)
		|| select_p.parse(pos, end, node, expected)
		|| insert_p.parse(pos, end, node, expected)
		|| create_p.parse(pos, end, node, expected)
		|| rename_p.parse(pos, end, node, expected)
		|| drop_p.parse(pos, end, node, expected)
		|| alter_p.parse(pos, end, node, expected)
		|| use_p.parse(pos, end, node, expected)
		|| set_p.parse(pos, end, node, expected)
		|| optimize_p.parse(pos, end, node, expected)
		|| table_p.parse(pos, end, node, expected);

	if (!res)
		expected = "One of: SHOW TABLES, SHOW DATABASES, SHOW CREATE TABLE, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE, EXISTS, DESCRIBE, DESC, ALTER";

	return res;
}

}
