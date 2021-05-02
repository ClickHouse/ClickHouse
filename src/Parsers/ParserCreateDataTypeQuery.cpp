#include "ASTCreateDataTypeQuery.h"
#include "CommonParsers.h"
#include "ExpressionElementParsers.h"
#include "ParserCreateDataTypeQuery.h"
#include "ParserDataType.h"
#include "ASTIdentifier.h"

namespace DB
{
bool ParserCreateDataTypeQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create("CREATE");
    ParserKeyword s_type("TYPE");
    ParserIdentifier type_name_p;
    ParserKeyword s_as("AS");
    ParserDataType nested_p;

    ASTPtr type_name;
    ASTPtr nested;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (!type_name_p.parse(pos, type_name, expected))
        return false;

    if (!s_as.ignore(pos, expected))
        return false;

    if (!nested_p.parse(pos, nested, expected))
        return false;

    auto query = std::make_shared<ASTCreateDataTypeQuery>();
    node = query;
    query->type_name = type_name->as<ASTIdentifier &>().name();
    query->nested = nested;

    return true;
}
}
