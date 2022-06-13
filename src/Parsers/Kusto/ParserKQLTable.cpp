#include <Parsers/ASTLiteral.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/Kusto/ParserKQLQuery.h>
#include <Parsers/Kusto/ParserKQLTable.h>

namespace DB
{

bool ParserKQLTable :: parsePrepare(Pos & pos)
{
    if (!op_pos.empty())
        return false;

    op_pos.push_back(pos);
    return true;
}

bool ParserKQLTable :: parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    std::unordered_set<String> sql_keywords
    ({
        "SELECT",
        "INSERT",
        "CREATE",
        "ALTER",
        "SYSTEM",
        "SHOW",
        "GRANT",
        "REVOKE",
        "ATTACH",
        "CHECK",
        "DESCRIBE",
        "DESC",
        "DETACH",
        "DROP",
        "EXISTS",
        "KILL",
        "OPTIMIZE",
        "RENAME",
        "SET",
        "TRUNCATE",
        "USE",
        "EXPLAIN"
    });

    if (op_pos.empty())
        return false;

    auto begin = pos;
    pos = op_pos.back();

    String table_name(pos->begin,pos->end);
    String table_name_upcase(table_name);

    std::transform(table_name_upcase.begin(), table_name_upcase.end(),table_name_upcase.begin(), toupper);

    if (sql_keywords.find(table_name_upcase) != sql_keywords.end())
        return false;

    if (!ParserTablesInSelectQuery().parse(pos, node, expected))
        return false;
    pos = begin;

    return true;
}

}
