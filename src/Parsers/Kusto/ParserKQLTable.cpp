#include <unordered_set>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTable.h>
#include <Parsers/ParserTablesInSelectQuery.h>
namespace DB
{

bool ParserKQLTable ::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::unordered_set<String> sql_keywords({"SELECT",   "INSERT", "CREATE",   "ALTER",    "SYSTEM", "SHOW",   "GRANT",  "REVOKE",
                                             "ATTACH",   "CHECK",  "DESCRIBE", "DESC",     "DETACH", "DROP",   "EXISTS", "KILL",
                                             "OPTIMIZE", "RENAME", "SET",      "TRUNCATE", "USE",    "EXPLAIN"});

    ASTPtr tables;
    String table_name(pos->begin, pos->end);
    String table_name_upcase(table_name);

    std::transform(table_name_upcase.begin(), table_name_upcase.end(), table_name_upcase.begin(), toupper);

    if (sql_keywords.find(table_name_upcase) != sql_keywords.end())
        return false;

    if (!ParserTablesInSelectQuery().parse(pos, tables, expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    return true;
}

}
