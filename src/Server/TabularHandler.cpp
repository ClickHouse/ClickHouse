#include "TabularHandler.h"
#include "Parsers/ASTAsterisk.h"
#include "Parsers/ASTExpressionList.h"
#include "Parsers/ASTIdentifier.h"
#include "Parsers/ASTTablesInSelectQuery.h"
#include "Interpreters/executeQuery.h"

#include <Parsers/ASTSelectQuery.h>
#include "Parsers/ExpressionListParsers.h"
#include "Parsers/formatAST.h"

#include <optional>
#include <string>
#include <vector>

#include <Interpreters/Context.h>
#include <Poco/URI.h>

namespace DB
{

static const std::unordered_set<std::string> kQueryParameters = {"where", "columns", "select", "order", "format", "query"};
static constexpr auto kWhere = "where";

TabularHandler::TabularHandler(IServer & server_, const std::optional<String> & content_type_override_)
    : HTTPHandler(server_, "TabularHandler", content_type_override_), log(getLogger("TabularHandler"))
{
}

std::string TabularHandler::getQuery(HTTPServerRequest & request, HTMLForm & /*params*/, ContextMutablePtr context)
{
    auto uri = Poco::URI(request.getURI());

    std::vector<std::string> path_segments;
    uri.getPathSegments(path_segments);

    const auto database = path_segments[1];
    const auto table_with_format = path_segments[2];

    auto pos = table_with_format.rfind('.');
    std::string table = table_with_format.substr(0, pos);
    std::string format = table_with_format.substr(pos + 1);

    auto select_query = std::make_shared<ASTSelectQuery>();

    auto select_expression_list = std::make_shared<ASTExpressionList>();
    select_expression_list->children.push_back(std::make_shared<ASTAsterisk>());
    select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_expression_list);

    auto table_expression = std::make_shared<ASTTableExpression>();
    table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>(database, table);
    auto tables_in_select_query = std::make_shared<ASTTablesInSelectQuery>();
    auto tables_in_select_element = std::make_shared<ASTTablesInSelectQueryElement>();
    tables_in_select_element->table_expression = table_expression;
    tables_in_select_query->children.push_back(tables_in_select_element);
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables_in_select_query);

    const auto & query_parameters = context->getQueryParameters();

    if (query_parameters.contains(kWhere))
    {
        const auto & where_raw = query_parameters.at(kWhere);
        ASTPtr where_expression;
        Tokens tokens(where_raw.c_str(), where_raw.c_str() + where_raw.size());
        IParser::Pos new_pos(tokens, 0, 0);
        Expected expected;

        ParserExpressionWithOptionalAlias(false).parse(new_pos, where_expression, expected);
        select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_expression));
    }

    // Convert AST to query string
    WriteBufferFromOwnString query_buffer;
    formatAST(*select_query, query_buffer, false);
    std::string query_str = query_buffer.str();

    // Append FORMAT clause
    query_str += " FORMAT " + format;

    LOG_INFO(log, "TabularHandler LOG {}", query_str);

    return query_str;
    // LOG_INFO(log, "TabularHandler LOG {}", request.getURI());
}


bool TabularHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (kQueryParameters.contains(key) && !context->getQueryParameters().contains(key))
    {
        context->setQueryParameter(key, value);
        return true;
    }

    return false;
}

} // namespace DB
