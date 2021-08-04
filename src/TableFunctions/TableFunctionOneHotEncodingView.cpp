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
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

std::tuple<const String, const String> TableFunctionOneHotEncodingView::getWithAndSelectForColumn(String base_query_str, const String & column_name)
{
    String encoded_column_name = column_name + ".encoded";
    String mapping = "__ohe_map_" + column_name;
    String mapping_idx = "__ohe_map_idx_" + column_name;
    String with;
    String select;

    with = "(SELECT groupArray(`" + column_name +"`) FROM ";
    with += "(SELECT DISTINCT `" + column_name + "` FROM (" + base_query_str + ") ";
    with += "ORDER BY `" + column_name + "`)) AS `" + mapping + "`, ";
    with += "arrayEnumerate(`" + mapping + "`) AS `" + mapping_idx + "`,";
    select = "arrayMap(i -> `" + mapping + "`[i] = `" + column_name + "`,`" + mapping_idx + "`) AS `" + encoded_column_name + "`,";

    return std::make_tuple(with, select);
}

void TableFunctionOneHotEncodingView::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function)
    {
        if (auto * query = function->tryGetQueryArgument())
        {
            if (auto * column_list = function->tryGetColumnListArgument()->as<ASTExpressionList>())
            {
                String with;
                String select;
                String base_query_str = queryToString(query->clone());
                /// FIXME: need to move this code to building AST to prevent SQL injections
                String ohe_query_str = "WITH ";
                String ohe_select = " SELECT *,";
                /// iterate over the list of columns that we will use for one-hot encoding
                for (const auto & element : column_list->children)
                {
                    auto column_name = getIdentifierName(element->as<ASTIdentifier>());
                    std::tie(with, select) = getWithAndSelectForColumn(base_query_str, column_name);
                    ohe_query_str += with;
                    ohe_select += select;
                }
                ohe_query_str.pop_back(); // remove last comma
                ohe_select.pop_back(); // remove last comma
                ohe_query_str += ohe_select + " FROM (" + base_query_str + ")";

                ParserSelectWithUnionQuery parser;
                ASTPtr ohe_query = parseQuery(parser, ohe_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                /// FIXME: given that we are using arrayMap we
                ///        need to force max_block_size to 2 for optimal performance
                ///        given that the cardinality of column can be high.
                ///        Ideally, this needs to be calculated dynamically but for that we
                ///        would need to know the actual cardinality for each column.
                context->getGlobalContext()->setSetting("max_block_size", 2);
                /// set select query inside the create query for the view
                create.set(create.select, ohe_query);
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
