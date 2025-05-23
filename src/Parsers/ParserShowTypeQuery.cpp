#include <Parsers/ParserShowTypeQuery.h>
#include <Parsers/ASTShowTypeQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/String.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserShowTypeQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show(Keyword::SHOW);
    ParserKeyword s_type(Keyword::TYPE);
    ParserIdentifier type_name_parser;

    ASTPtr type_name_ast;

    if (!s_show.ignore(pos, expected))
        return false;

    if (!s_type.ignore(pos, expected))
        return false;

    if (!type_name_parser.parse(pos, type_name_ast, expected))
        return false;
    
    const auto * type_ident = type_name_ast->as<ASTIdentifier>();
    if (!type_ident)
        return false;

    auto query = std::make_shared<ASTShowTypeQuery>();
    query->type_name = type_ident->name();

    node = query;
    return true;
}

}
