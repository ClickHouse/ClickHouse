#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Storages/StorageView.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionOneHotEncodingView.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ColumnsDescription TableFunctionOneHotEncodingView::getActualBaseQueryTableStructure(
    ASTPtr base_query,
    ContextPtr context)
{
    if (base_query)
    {
        auto sample = InterpreterSelectWithUnionQuery::getSampleBlock(base_query, context);
        return ColumnsDescription(sample.getNamesAndTypesList());
    }
    return ColumnsDescription();
}

std::tuple<const String, const String> TableFunctionOneHotEncodingView::getWithAndSelectForColumn(
    String base_query_str,
    const String & column_name,
    const size_t & column_idx)
{
    String column_idx_str = std::to_string(column_idx);
    /// create column name and encoded column name place holders
    /// because we can't use the actual names
    /// due to potential SQL injection attacks and therefore we will
    /// replace these in the final AST tree using visitors
    String ohe_column_name = "__ohe_column_name_" + column_idx_str;
    String encoded_column_name = "__ohe_encoded_column_name_" +  column_idx_str;

    String mapping = "__ohe_map_" + column_idx_str;
    String mapping_idx = "__ohe_map_idx_" + column_idx_str;
    String with;
    String select;

    with = "(SELECT groupArray(`" + ohe_column_name +"`) FROM ";
    with += "(SELECT DISTINCT `" + ohe_column_name + "` FROM (" + base_query_str + ") ";
    with += "ORDER BY `" + ohe_column_name + "`)) AS `" + mapping + "`, ";
    with += "arrayEnumerate(`" + mapping + "`) AS `" + mapping_idx + "`,";
    select = "arrayMap(i -> `" + mapping + "`[i] = `" + ohe_column_name + "`,`" + mapping_idx + "`) AS `" + encoded_column_name + "`,";

    /// add columns and aliases for future replacement in the AST tree visitors
    rename_columns_data.columns_name.push_back(ohe_column_name);
    rename_columns_data.renames_to.push_back(column_name);
    rename_aliases_data.aliases_name.push_back(encoded_column_name);
    rename_aliases_data.renames_to.push_back(column_name + ".encoded");

    return std::make_tuple(with, select);
}

void TableFunctionOneHotEncodingView::parseArguments(
    const ASTPtr & ast_function,
    ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function)
    {
        if (auto * query = function->tryGetQueryArgument())
        {
            if (auto * column_list = function->tryGetColumnListArgument()->as<ASTExpressionList>())
            {
                ASTPtr base_query = query->clone();
                auto base_query_columns = getActualBaseQueryTableStructure(base_query, context);
                String base_query_str = queryToString(base_query);
                String ohe_query_str = "WITH ";
                String ohe_select = " SELECT *,";
                String with;
                String select;
                /// iterate over the list of columns that we will use for one-hot encoding
                size_t column_idx = 0;
                for (const auto & element : column_list->children)
                {
                    auto column_name = getIdentifierName(element->as<ASTIdentifier>());
                    /// check that identifier matches base query table structure
                    if (!base_query_columns.has(column_name))
                        throw Exception("Table function '" + getName() + "' got invalid column argument '" + column_name +"'", ErrorCodes::BAD_ARGUMENTS);
                    std::tie(with, select) = getWithAndSelectForColumn(base_query_str, column_name, column_idx);
                    ohe_query_str += with;
                    ohe_select += select;
                    column_idx++;
                }
                ohe_query_str.pop_back(); // remove last comma
                ohe_select.pop_back(); // remove last comma
                ohe_query_str += ohe_select + " FROM (" + base_query_str + ")";
                /// parse query to AST tree
                ParserSelectWithUnionQuery parser;
                ASTPtr ohe_query = parseQuery(parser, ohe_query_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
                /// visit AST tree and replace column names place holders with the actual column names
                RenameColumnsVisitor rename_columns_visitor(rename_columns_data);
                RenameFunctionAliasesVisitor rename_aliases_visitor(rename_aliases_data);
                rename_columns_visitor.visit(ohe_query);
                rename_aliases_visitor.visit(ohe_query);
                /// set select query inside the create query for the view
                create.set(create.select, ohe_query);
                return;
            }
        }
    }
    throw Exception("Table function '" + getName() + "' requires a query argument", ErrorCodes::BAD_ARGUMENTS);
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
