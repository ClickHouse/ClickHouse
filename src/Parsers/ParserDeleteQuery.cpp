#include <memory>
#include <Parsers/ParserDeleteQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/parseDatabaseAndTableName.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
bool ParserDeleteQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    node = std::make_shared<ASTDeleteQuery>();
    auto& query = node->as<ASTDeleteQuery&>();

    if (!"DELETE FROM"_kw.ignore(pos, expected)
        || !parseDatabaseAndTableName(pos, expected, query.database, query.table)
        || !"WHERE"_kw.ignore(pos, expected))
        return false;

    return ParserExpression().parse(pos, query.predicate, expected);
}
}
