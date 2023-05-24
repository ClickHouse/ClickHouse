#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>


namespace DB
{

bool ParserDeleteQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTDeleteQuery>();
    node = query;

    ParserKeyword s_delete("DELETE");
    ParserKeyword s_from("FROM");
    ParserKeyword s_where("WHERE");
    ParserExpression parser_exp_elem;

    if (s_delete.ignore(pos, expected))
    {
        if (!s_from.ignore(pos, expected))
            return false;

        if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
            return false;

        if (!s_where.ignore(pos, expected))
            return false;

        if (!parser_exp_elem.parse(pos, query->predicate, expected))
            return false;
    }
    else
        return false;

    if (query->predicate)
        query->children.push_back(query->predicate);

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    return true;
}

}
