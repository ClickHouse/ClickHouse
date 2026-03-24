#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateHandlerQuery.h>
#include <Parsers/ASTCreateHandlerQuery.h>


namespace DB
{

bool ParserCreateHandlerQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_handler(Keyword::HANDLER);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_url(Keyword::URL);
    ParserKeyword s_prefix(Keyword::PREFIX);
    ParserKeyword s_regexp(Keyword::REGEXP);
    ParserKeyword s_methods(Keyword::METHODS);
    ParserKeyword s_as(Keyword::AS);
    ParserIdentifier name_p;
    ParserStringLiteral string_literal_p;
    ParserToken s_open_paren(TokenType::OpeningRoundBracket);
    ParserToken s_close_paren(TokenType::ClosingRoundBracket);
    ParserToken s_comma(TokenType::Comma);

    String cluster_str;
    bool if_not_exists = false;

    ASTPtr handler_name;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_handler.ignore(pos, expected))
        return false;

    if (s_if_not_exists.ignore(pos, expected))
        if_not_exists = true;

    if (!name_p.parse(pos, handler_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    if (!s_url.ignore(pos, expected))
        return false;

    std::string url_type = "exact";
    if (s_prefix.ignore(pos, expected))
        url_type = "prefix";
    else if (s_regexp.ignore(pos, expected))
        url_type = "regexp";

    ASTPtr url_ast;
    if (!string_literal_p.parse(pos, url_ast, expected))
        return false;
    String url = url_ast->as<ASTLiteral &>().value.safeGet<String>();

    std::vector<std::string> methods;
    if (s_methods.ignore(pos, expected))
    {
        if (!s_open_paren.ignore(pos, expected))
            return false;

        while (true)
        {
            ASTPtr method_ast;
            if (!name_p.parse(pos, method_ast, expected))
                return false;

            methods.push_back(getIdentifierName(method_ast));

            if (!s_comma.ignore(pos))
                break;
        }

        if (!s_close_paren.ignore(pos, expected))
            return false;
    }

    if (!s_as.ignore(pos, expected))
        return false;

    ASTPtr query_ast;
    if (!string_literal_p.parse(pos, query_ast, expected))
        return false;
    String query_str = query_ast->as<ASTLiteral &>().value.safeGet<String>();

    auto query = make_intrusive<ASTCreateHandlerQuery>();
    tryGetIdentifierNameInto(handler_name, query->handler_name);
    query->if_not_exists = if_not_exists;
    query->cluster = std::move(cluster_str);
    query->url = std::move(url);
    query->url_type = std::move(url_type);
    query->methods = std::move(methods);
    query->query = std::move(query_str);

    node = query;
    return true;
}

}
