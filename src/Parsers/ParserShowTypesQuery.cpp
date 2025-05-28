#include <Parsers/ParserShowTypesQuery.h>
#include <Parsers/ASTShowTypesQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/String.h>
#include <Parsers/ExpressionElementParsers.h>

namespace DB
{

bool ParserShowTypesQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_show(Keyword::SHOW);
    ParserIdentifier types_identifier_p;

    ASTPtr types_identifier_ast;

    if (!s_show.ignore(pos, expected))
        return false;

    if (!types_identifier_p.parse(pos, types_identifier_ast, expected))
        return false;
    
    const auto * types_ident = types_identifier_ast->as<ASTIdentifier>();
    if (!types_ident || Poco::toUpper(types_ident->name()) != "TYPES")
    {
        return false; 
    }

    auto query = std::make_shared<ASTShowTypesQuery>();

    node = query;
    return true;
}

}
