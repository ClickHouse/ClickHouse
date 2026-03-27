#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserAlterHandlerQuery.h>
#include <Parsers/ASTAlterHandlerQuery.h>


namespace DB
{

bool ParserAlterHandlerQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_handler(Keyword::HANDLER);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
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
    bool if_exists = false;

    ASTPtr handler_name;

    if (!s_alter.ignore(pos, expected))
        return false;

    if (!s_handler.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!name_p.parse(pos, handler_name, expected))
        return false;

    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
    }

    auto query = make_intrusive<ASTAlterHandlerQuery>();
    tryGetIdentifierNameInto(handler_name, query->handler_name);
    query->if_exists = if_exists;
    query->cluster = std::move(cluster_str);

    if (s_url.ignore(pos, expected))
    {
        HandlerURLType url_type = HandlerURLType::Exact;
        if (s_prefix.ignore(pos, expected))
            url_type = HandlerURLType::Prefix;
        else if (s_regexp.ignore(pos, expected))
            url_type = HandlerURLType::Regexp;

        ASTPtr url_ast;
        if (!string_literal_p.parse(pos, url_ast, expected))
            return false;

        query->url = url_ast->as<ASTLiteral &>().value.safeGet<String>();
        query->url_type = url_type;
    }

    if (s_methods.ignore(pos, expected))
    {
        if (!s_open_paren.ignore(pos, expected))
            return false;

        std::vector<std::string> methods;
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

        query->methods = std::move(methods);
    }

    if (s_as.ignore(pos, expected))
    {
        ASTPtr query_ast;
        if (!string_literal_p.parse(pos, query_ast, expected))
            return false;

        query->query = query_ast->as<ASTLiteral &>().value.safeGet<String>();
    }

    node = query;
    return true;
}

}
