#include <Parsers/ASTDropDataTypeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropDataTypeQuery.h>

namespace DB
{

bool ParserDropDataTypeQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop("DROP");
    ParserKeyword s_type("TYPE");
    ParserIdentifier type_name_p;

    ASTPtr type_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (!type_name_p.parse(pos, type_name, expected))
        return false;

    auto drop_function_query = std::make_shared<ASTDropDataTypeQuery>();
    node = drop_function_query;

    drop_function_query->type_name = type_name->as<ASTIdentifier &>().name();

    return true;
}

}
