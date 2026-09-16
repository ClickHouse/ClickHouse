#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserCreateTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_or_replace(Keyword::OR_REPLACE);
    ParserKeyword s_as(Keyword::AS);

    ParserIdentifier name_p;
    ParserDataType type_p;
    ParserExpressionList params_p(false);

    ASTPtr name;
    ASTPtr base_type;
    ASTPtr params_ast;
    bool if_not_exists = false;
    bool or_replace = false;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;
    else if (s_or_replace.ignore(pos, expected))
        or_replace = true;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
    {
        if (!params_p.parse(pos, params_ast, expected))
            return false;
        if (!ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;
    }

    if (!s_as.ignore(pos, expected))
        return false;

    if (!type_p.parse(pos, base_type, expected))
        return false;

    auto query = make_intrusive<ASTCreateTypeQuery>();
    query->name = typeid_cast<ASTIdentifier &>(*name).name();
    query->base_type = base_type;
    query->type_parameters = params_ast;
    query->if_not_exists = if_not_exists;
    query->or_replace = or_replace;

    if (query->base_type)
        query->children.push_back(query->base_type);
    if (query->type_parameters)
        query->children.push_back(query->type_parameters);

    node = query;
    return true;
}

}
