#include <Parsers/ParserDropTypeQuery.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ASTIdentifier.h> // Для парсинга имени
#include <Parsers/ExpressionElementParsers.h> // Для ParserIdentifier

namespace DB
{

bool ParserDropTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier type_name_p; // Используем ParserIdentifier для имени типа

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    bool if_exists = false;
    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    ASTPtr type_name_ast;
    if (!type_name_p.parse(pos, type_name_ast, expected))
        return false;

    auto query = std::make_shared<ASTDropTypeQuery>();
    query->type_name = type_name_ast->as<ASTIdentifier &>().name();
    query->if_exists = if_exists;

    node = query;
    return true;
}

}
