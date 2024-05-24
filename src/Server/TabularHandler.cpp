#include "TabularHandler.h"

#include <string>
#include <unordered_set>
#include <vector>

#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/formatAST.h>

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPQueryAST.h>
#include <Poco/URI.h>

namespace DB
{

static constexpr auto kFormat = "format";

TabularHandler::TabularHandler(IServer & server_, const std::optional<String> & content_type_override_)
    : HTTPHandler(server_, "TabularHandler", content_type_override_)
{
}

std::shared_ptr<QueryData> TabularHandler::getQueryAST(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context)
{
    auto uri = Poco::URI(request.getURI());

    std::vector<std::string> path_segments;
    uri.getPathSegments(path_segments);

    std::string database = "default";
    std::string table_with_format;

    if (path_segments.size() == 3)
    {
        database = path_segments[1];
        table_with_format = path_segments[2];
    }
    else
    {
        table_with_format = path_segments[1];
    }

    std::string format = "";
    std::string table;

    auto pos = table_with_format.find('.');
    if (pos != std::string::npos)
    {
        table = table_with_format.substr(0, pos);
        format = table_with_format.substr(pos + 1);
    }
    else
    {
        table = table_with_format;
    }

    auto select_query = std::make_shared<ASTSelectQuery>();

    const auto & query_parameters = context->getQueryParameters();

    if (query_parameters.contains(kFormat))
        format = query_parameters.at(kFormat);

    auto http_query_ast = getHTTPQueryAST(params);
    if (http_query_ast.select_expressions.empty())
    {
        auto select_expression_list = std::make_shared<ASTExpressionList>();
        select_expression_list->children.push_back(std::make_shared<ASTAsterisk>());
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, std::move(select_expression_list));
    }

    auto tables_in_select_query = std::make_shared<ASTTablesInSelectQuery>();
    auto tables_in_select_element = std::make_shared<ASTTablesInSelectQueryElement>();

    auto table_expression = std::make_shared<ASTTableExpression>();
    table_expression->database_and_table_name = std::make_shared<ASTTableIdentifier>(database, table);

    tables_in_select_element->table_expression = std::move(table_expression);
    tables_in_select_query->children.push_back(std::move(tables_in_select_element));
    select_query->setExpression(ASTSelectQuery::Expression::TABLES, std::move(tables_in_select_query));

    context->setDefaultFormat(format);

    return std::make_shared<QueryData>(select_query);
}


bool TabularHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (key == kFormat && !context->getQueryParameters().contains(key))
    {
        context->setQueryParameter(key, value);
        return true;
    }

    return false;
}

} // namespace DB
