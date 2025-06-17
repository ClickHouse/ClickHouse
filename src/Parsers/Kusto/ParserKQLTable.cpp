#include <unordered_set>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTable.h>
#include <Parsers/ParserTablesInSelectQuery.h>
namespace DB
{

bool ParserKQLTable ::parseImpl(IKQLParser::KQLPos & pos, ASTPtr & node, [[maybe_unused]] KQLExpected & expected)
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

    String expr;
    Expected sql_expected;
    expr = getExprFromToken(pos);
    Tokens tokens(expr.data(), expr.data() + expr.size());
    IParser::Pos new_pos(tokens, pos.max_depth, pos.max_backtracks);

    if (!ParserTablesInSelectQuery().parse(new_pos, tables, sql_expected))
        return false;

    node->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables));

    return true;
}

}
