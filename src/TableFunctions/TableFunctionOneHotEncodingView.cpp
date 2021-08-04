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

///
///  Wraps base query to add one-hot encoded `<column>.encoded` Array(UInt8) column
///  using the following query:
///
///  WITH
///     (
///         SELECT groupArray(toLowCardinality(column) AS mapping
///         FROM
///         (
///             SELECT DISTINCT column
///             FROM (base_query)
///             ORDER BY column ASC
///         )
///     ) AS mapping,
///     arrayResize([0], length(mapping)) AS encoding
/// SELECT *, EXCEPT (lc_column)
/// FROM (
///     SELECT
///         *,
///         toLowCardinality(column) AS lc_column,
///         arrayMap((x, i) -> ((mapping[i]) = lc_column), _encoding, arrayEnumerate(mapping)) AS column.encoded
///     FROM (base_query)
/// )
///
ASTPtr TableFunctionOneHotEncodingView::wrapQuery(ASTPtr query, const String & column_name)
{
    // FIXME: build query as AST
    ASTPtr wrapped_query;
    String base_query_str = queryToString(query);
    String encoded_column_name = column_name + ".encoded";
    String mapping = "__one_hot_encoding_mapping_" + column_name;
    String encoding_array = "__one_hot_encoding_array_" + column_name;
    String lc_column_name = "__one_hot_encoding_low_cardinality_" + column_name;

    String wrapped_query_str = "WITH (";
    wrapped_query_str += "SELECT groupArray(toLowCardinality(`" + column_name +"`)) AS `" + mapping + "` ";
    wrapped_query_str += "FROM (SELECT DISTINCT `" + column_name + "` FROM (" + base_query_str + ") ";
    wrapped_query_str += "ORDER BY `" + column_name + "`)) AS `" + mapping + "`, ";
    wrapped_query_str += "arrayResize([toUInt8(0)], length(`" + mapping + "`)) AS `" + encoding_array + "` ";
    wrapped_query_str += "SELECT * EXCEPT (`" + lc_column_name + "`) FROM ";
    wrapped_query_str += "(SELECT *, toLowCardinality(`" + column_name + "`) AS `" + lc_column_name + "`,";
    wrapped_query_str += "arrayMap((x,i) -> (toUInt8((`" + mapping + "`[i] = `" + lc_column_name + "`))), `" + encoding_array;
    wrapped_query_str += "`, arrayEnumerate(`" + mapping + "`)) AS `" + encoded_column_name + "` ";
    wrapped_query_str += "FROM (" + base_query_str + "))";

    ParserSelectWithUnionQuery parser;
    wrapped_query = parseQuery(parser, wrapped_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    return wrapped_query;
}

void TableFunctionOneHotEncodingView::parseArguments(const ASTPtr & ast_function, ContextPtr /* context */)
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
                    base_query = wrapQuery(base_query, column_name);
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
