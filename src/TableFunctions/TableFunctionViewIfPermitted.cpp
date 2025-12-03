#include <Core/Settings.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <base/types.h>
#include <Storages/StorageNull.h>
#include <Storages/StorageView.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ACCESS_DENIED;
}

namespace
{

/* viewIfPermitted(query ELSE null('structure'))
 * Works as "view(query)" if the current user has the permissions required to execute "query"; works as "null('structure')" otherwise.
 */
class TableFunctionViewIfPermitted : public ITableFunction
{
public:
    static constexpr auto name = "viewIfPermitted";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "ViewIfPermitted"; }

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    bool isPermitted(const ContextPtr & context, const ColumnsDescription & else_columns) const;

    ASTCreateQuery create;
    ASTPtr else_ast;
    TableFunctionPtr else_table_function;
};


std::vector<size_t> TableFunctionViewIfPermitted::skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const
{
    return {0};
}

void TableFunctionViewIfPermitted::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (!function || !function->arguments || (function->arguments->children.size() != 2))
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires two arguments: a SELECT query and a table function",
            getName());

    const auto & arguments = function->arguments->children;
    auto * select = arguments[0]->as<ASTSelectWithUnionQuery>();
    if (!select)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires a SELECT query as its first argument", getName());
    create.set(create.select, select->clone());

    else_ast = arguments[1];
    if (!else_ast->as<ASTFunction>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' requires a table function as its second argument", getName());
    else_table_function = TableFunctionFactory::instance().get(else_ast, context);
}

ColumnsDescription TableFunctionViewIfPermitted::getActualTableStructure(ContextPtr context, bool is_insert_query) const
{
    return else_table_function->getActualTableStructure(context, is_insert_query);
}

StoragePtr TableFunctionViewIfPermitted::executeImpl(
    const ASTPtr & /* ast_function */, ContextPtr context, const std::string & table_name, ColumnsDescription /* cached_columns */, bool is_insert_query) const
{
    StoragePtr storage;
    auto columns = getActualTableStructure(context, is_insert_query);

    if (isPermitted(context, columns))
    {
        storage = std::make_shared<StorageView>(StorageID(getDatabaseName(), table_name), create, columns, "");
    }
    else
    {
        storage = else_table_function->execute(else_ast, context, table_name);
    }

    storage->startup();
    return storage;
}

bool TableFunctionViewIfPermitted::isPermitted(const ContextPtr & context, const ColumnsDescription & else_columns) const
{
    Block sample_block;

    try
    {
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
        {
            sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(create.children[0], context);
        }
        else
        {
            /// Will throw ACCESS_DENIED if the current user is not allowed to execute the SELECT query.
            sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(create.children[0], context);
        }
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::ACCESS_DENIED)
            return false;
        throw;
    }

    /// We check that columns match only if permitted (otherwise we could reveal the structure to an user who must not know it).
    ColumnsDescription columns{sample_block.getNamesAndTypesList()};
    if (columns != else_columns)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' requires a SELECT query with the result columns matching a table function after 'ELSE'. "
            "Currently the result columns of the SELECT query are {}, and the table function after 'ELSE' gives {}",
            getName(),
            columns.toString(),
            else_columns.toString());
    }

    return true;
}

}

void registerTableFunctionViewIfPermitted(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionViewIfPermitted>({.documentation = {}, .allow_readonly = true});
}

}
