#include "HTTPQuery.h"

#include <format>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/IParser.h>
#include <Parsers/TokenIterator.h>

namespace DB
{

namespace
{

static constexpr auto kColumns = "columns";
static constexpr auto kSelect = "select";
static constexpr auto kWhere = "where";
static constexpr auto kOrder = "order";

static const NameSet reserved_param_names{"compress",      "decompress",
                                          "user",          "password",
                                          "quota_key",     "query_id",
                                          "stacktrace",    "role",
                                          "buffer_size",   "wait_end_of_query",
                                          "session_id",    "session_timeout",
                                          "session_check", "client_protocol_version",
                                          "close_session", "execute",
                                          "where",         "columns",
                                          "select",        "order"};

template <typename T>
ASTPtr parseExpression(const std::string & expression, const std::optional<ParserKeyword> & keyword = std::nullopt)
{
    ASTPtr ast;
    Tokens tokens(expression.c_str(), expression.c_str() + expression.size());
    IParser::Pos pos(tokens, 0, 0);
    Expected expected;

    ParserKeyword s_select(Keyword::SELECT);

    if (keyword.has_value())
        s_select.ignore(pos, expected);

    T(false).parse(pos, ast, expected);
    return ast;
}

ASTPtr parseColumns(const std::string & columns)
{
    ASTPtr result;
    Tokens tokens(columns.c_str(), columns.c_str() + columns.size());
    IParser::Pos pos(tokens, 0, 0);
    Expected expected;

    ParserNotEmptyExpressionList(false).parse(pos, result, expected);
    return result;
}

ASTPtr parseWhere(const std::string & where)
{
    ASTPtr result;
    Tokens tokens(where.c_str(), where.c_str() + where.size());
    IParser::Pos pos(tokens, 0, 0);
    Expected expected;

    ParserExpressionWithOptionalAlias(false).parse(pos, result, expected);
    return result;
}

ASTPtr parseOrder(const std::string & order)
{
    ASTPtr result;
    Tokens tokens(order.c_str(), order.c_str() + order.size());
    IParser::Pos pos(tokens, 0, 0);
    Expected expected;

    ParserOrderByExpressionList().parse(pos, result, expected);
    return result;
}

}

template <typename T>
HTTPQueryAST getHTTPQueryAST(const T & params)
{
    HTTPQueryAST result;

    for (const auto & [key, value] : params)
        if (key == kColumns)
            result.select_expressions.push_back(parseColumns(value));
        else if (key == kSelect)
            result.select_expressions.push_back(parseColumns(value));
        else if (key == kWhere)
            result.where_expressions.push_back(parseWhere(value));
        else if (key == kOrder)
            result.order_expressions.push_back(parseOrder(value));
        else if (!reserved_param_names.contains(key))
            result.where_expressions.push_back(parseWhere(std::format("{}={}", key, value)));

    return result;
}

template HTTPQueryAST getHTTPQueryAST(const std::map<std::string, std::string> & params);
template HTTPQueryAST getHTTPQueryAST(const HTMLForm & params);

}
