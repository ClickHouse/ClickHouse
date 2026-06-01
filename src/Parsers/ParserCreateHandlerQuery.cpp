#include <Parsers/ParserCreateHandlerQuery.h>

#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Common/StringUtils.h>
#include <Poco/String.h>

#include <unordered_set>


namespace DB
{

namespace
{

bool parseMethods(IParser::Pos & pos, Expected & expected, std::vector<String> & methods)
{
    if (!ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected))
        return false;

    static const std::unordered_set<String> allowed_methods = {"GET", "POST", "PUT", "DELETE"};

    while (true)
    {
        ASTPtr method_ast;
        ParserIdentifier method_p;
        if (!method_p.parse(pos, method_ast, expected))
            return false;

        String method = Poco::toUpper(getIdentifierName(method_ast));
        if (!allowed_methods.contains(method))
            return false;

        methods.push_back(method);

        if (ParserToken(TokenType::Comma).ignore(pos, expected))
            continue;
        break;
    }

    return ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected);
}

}

bool ParserCreateHandlerQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_alter(Keyword::ALTER);
    ParserKeyword s_handler(Keyword::HANDLER);
    ParserKeyword s_if_not_exists(Keyword::IF_NOT_EXISTS);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_protocol(Keyword::PROTOCOL);
    ParserKeyword s_url(Keyword::URL);
    ParserKeyword s_prefix(Keyword::PREFIX);
    ParserKeyword s_regexp(Keyword::REGEXP);
    ParserKeyword s_methods(Keyword::METHODS);
    ParserKeyword s_type(Keyword::TYPE);
    ParserKeyword s_as(Keyword::AS);

    ParserIdentifier name_p;
    ParserStringLiteral string_literal_p;

    bool is_alter = false;
    if (s_create.ignore(pos, expected))
        is_alter = false;
    else if (s_alter.ignore(pos, expected))
        is_alter = true;
    else
        return false;

    if (!s_handler.ignore(pos, expected))
        return false;

    auto query = make_intrusive<ASTCreateHandlerQuery>();
    query->is_alter = is_alter;

    if (!is_alter && s_if_not_exists.ignore(pos, expected))
        query->if_not_exists = true;

    ASTPtr name_ast;
    if (!name_p.parse(pos, name_ast, expected))
        return false;
    query->handler_name = getIdentifierName(name_ast);

    if (s_on.ignore(pos, expected))
    {
        String cluster_str;
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;
        query->cluster = std::move(cluster_str);
    }

    if (s_protocol.ignore(pos, expected))
    {
        ASTPtr protocol_ast;
        if (!name_p.parse(pos, protocol_ast, expected))
            return false;
        query->protocol = getIdentifierName(protocol_ast);
    }

    if (s_url.ignore(pos, expected))
    {
        if (s_prefix.ignore(pos, expected))
            query->url_match_type = ASTCreateHandlerQuery::URLMatchType::Prefix;
        else if (s_regexp.ignore(pos, expected))
            query->url_match_type = ASTCreateHandlerQuery::URLMatchType::Regexp;
        else
            query->url_match_type = ASTCreateHandlerQuery::URLMatchType::Exact;

        ASTPtr url_ast;
        if (!string_literal_p.parse(pos, url_ast, expected))
            return false;
        query->url = url_ast->as<ASTLiteral &>().value.safeGet<String>();
        query->has_url = true;
    }

    if (s_methods.ignore(pos, expected))
    {
        std::vector<String> methods;
        if (!parseMethods(pos, expected, methods))
            return false;
        query->methods = std::move(methods);
    }

    if (s_type.ignore(pos, expected))
    {
        ASTPtr type_ast;
        if (!name_p.parse(pos, type_ast, expected))
            return false;
        query->handler_type = Poco::toLower(getIdentifierName(type_ast));
    }

    if (s_as.ignore(pos, expected))
    {
        bool in_parens = ParserToken(TokenType::OpeningRoundBracket).ignore(pos, expected);

        ParserQuery query_p(end);
        ASTPtr inner_query;
        if (!query_p.parse(pos, inner_query, expected))
            return false;

        if (in_parens && !ParserToken(TokenType::ClosingRoundBracket).ignore(pos, expected))
            return false;

        query->query = inner_query;
        query->children.push_back(inner_query);
    }

    /// URL is mandatory for CREATE.
    if (!is_alter && !query->has_url)
        return false;

    node = query;
    return true;
}

}
