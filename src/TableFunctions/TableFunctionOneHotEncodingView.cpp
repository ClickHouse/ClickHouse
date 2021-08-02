#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionOneHotEncodingView.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/executeQuery.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

Strings TableFunctionOneHotEncodingView::getColumnDistinctValues(const String & base_query_str, const String & column_name, ContextPtr context)
{
    Strings values;
    String value;
    String output;

    String distinct_query_str = "SELECT DISTINCT toString(`" + column_name + "`) FROM (" + base_query_str +") ORDER BY `" + column_name + "`";
    ReadBufferFromString istr(distinct_query_str);
    WriteBufferFromString ostr(output);

    auto query_context = Context::createCopy(context);
    query_context->makeQueryContext();
    query_context->getClientInfo().query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    query_context->setCurrentDatabase(context->getCurrentDatabase());
    query_context->setCurrentQueryId(""); // generate random query_id

    executeQuery(istr, ostr, false, query_context, {});

    /// split output by new line
    std::istringstream output_stream(output); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    while (getline(output_stream, value))
        values.push_back(value);

    /// remove last value as it is empty due to \n at the end of the output
    values.pop_back();

    return values;
}

ASTPtr TableFunctionOneHotEncodingView::wrapQuery(ASTPtr query, const String & column_name, ContextPtr context)
{
    String base_query_str = queryToString(query);
    Strings distinct_values = getColumnDistinctValues(base_query_str, column_name, context);
    Strings encoded_columns;

    String wrapped_query_str = "SELECT * EXCEPT (`__mapping_" + column_name + "`) FROM ";
    wrapped_query_str += "(SELECT *, toLowCardinality(toString(`" + column_name + "`)) AS `__mapping_" + column_name + "`,";

    for (const auto &value : distinct_values)
        wrapped_query_str += "toUInt8(equals(`__mapping_" + column_name + "`,toLowCardinality('" + value + "'))) AS `" + column_name + "." + value + "`,";

    wrapped_query_str.pop_back(); // remove last comma
    wrapped_query_str += "FROM (" + base_query_str + "))";

    ParserSelectWithUnionQuery parser;
    return parseQuery(parser, wrapped_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
}

void TableFunctionOneHotEncodingView::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function)
    {
        if (auto * query = function->tryGetQueryArgument())
        {
            ASTPtr base_query = query->clone();
            if (auto * column_list = function->tryGetColumnListArgument()->as<ASTExpressionList>())
            {
                /// iterate over the list of columns that we will use
                /// for one-hot encoding
                for (const auto & element : column_list->children)
                {
                    /// FIXME: need to check if column_name is in the structure of query
                    auto * column_identifier = element->as<ASTIdentifier>();
                    auto column_name = getIdentifierName(column_identifier);
                    /// add a wrapper around the base query that adds
                    /// a new column with one-hot encoding
                    /// for each unique value in the column
                    base_query = wrapQuery(base_query, column_name, context);
                }
                /// set select query inside the create query for the view
                create.set(create.select, base_query);
                return;
            }
        }
    }
    throw Exception("Table function '" + getName() + "' requires a query argument.", ErrorCodes::BAD_ARGUMENTS);
}

ColumnsDescription TableFunctionOneHotEncodingView::getActualTableStructure(ContextPtr context) const
{
    assert(create.select);
    assert(create.children.size() == 1);
    assert(create.children[0]->as<ASTSelectWithUnionQuery>());
    auto sample = InterpreterSelectWithUnionQuery::getSampleBlock(create.children[0], context);
    return ColumnsDescription(sample.getNamesAndTypesList());
}

StoragePtr TableFunctionOneHotEncodingView::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto res = StorageView::create(StorageID(getDatabaseName(), table_name), create, columns, "");
    res->startup();
    return res;
}

void registerTableFunctionOneHotEncodingView(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionOneHotEncodingView>();
}

}
