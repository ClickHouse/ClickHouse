#include <Common/OptimizedRegularExpression.h>
#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Storages/StorageMerge.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsUInt64 merge_table_max_tables_to_look_for_schema_inference;
}

namespace
{

[[noreturn]] void throwNoTablesMatchRegexp(const String & source_database_regexp, const String & source_table_regexp)
{
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Error while executing table function merge. Either there is no database, which matches regular expression `{}`, or there are "
        "no tables in the database matches `{}`, which fit tables expression: {}",
        source_database_regexp,
        source_database_regexp,
        source_table_regexp);
}

/* merge (db_name, tables_regexp) - creates a temporary StorageMerge.
 * The structure of the table is taken from the first table that came up, suitable for regexp.
 * If there is no such table, an exception is thrown.
 */
class TableFunctionMerge : public ITableFunction
{
public:
    static constexpr auto name = "merge";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "Merge"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String source_database_name_or_regexp;
    String source_table_regexp;
    bool database_is_regexp = false;
};

std::vector<size_t> TableFunctionMerge::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    auto & table_function_arguments_nodes = table_function_node.getArguments().getNodes();
    size_t table_function_arguments_size = table_function_arguments_nodes.size();

    std::vector<size_t> result;

    for (size_t i = 0; i < table_function_arguments_size; ++i)
    {
        auto * function_node = table_function_arguments_nodes[i]->as<FunctionNode>();
        if (function_node && function_node->getFunctionName() == "REGEXP")
            result.push_back(i);
    }

    return result;
}

void TableFunctionMerge::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function 'merge' requires from 1 to 2 parameters: "
                        "merge(['db_name',] 'tables_regexp')");

    ASTs & args = args_func.at(0)->children;

    if (args.size() == 1)
    {
        database_is_regexp = false;
        source_database_name_or_regexp = context->getCurrentDatabase();

        args[0] = evaluateConstantExpressionAsLiteral(args[0], context);
        source_table_regexp = checkAndGetLiteralArgument<String>(args[0], "table_name_regexp");
    }
    else if (args.size() == 2)
    {
        auto [is_regexp, database_ast] = StorageMerge::evaluateDatabaseName(args[0], context);

        database_is_regexp = is_regexp;

        if (!is_regexp)
            args[0] = database_ast;
        source_database_name_or_regexp = checkAndGetLiteralArgument<String>(database_ast, "database_name");

        args[1] = evaluateConstantExpressionAsLiteral(args[1], context);
        source_table_regexp = checkAndGetLiteralArgument<String>(args[1], "table_name_regexp");
    }
    else
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function 'merge' requires from 1 to 2 parameters: "
                        "merge(['db_name',] 'tables_regexp')");
    }
}

ColumnsDescription TableFunctionMerge::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    auto res = StorageMerge::getColumnsDescriptionFromSourceTables(
        context,
        source_database_name_or_regexp,
        database_is_regexp,
        source_table_regexp,
        context->getSettingsRef()[Setting::merge_table_max_tables_to_look_for_schema_inference]);
    if (res.empty())
        throwNoTablesMatchRegexp(source_database_name_or_regexp, source_table_regexp);

    return res;
}


StoragePtr TableFunctionMerge::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    auto res = std::make_shared<StorageMerge>(
        StorageID(getDatabaseName(), table_name),
        ColumnsDescription{},
        String{},
        source_database_name_or_regexp,
        database_is_regexp,
        source_table_regexp,
        context);

    res->startup();
    return res;
}

}

void registerTableFunctionMerge(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMerge>(
        {
            .documentation = {
                .description = "Creates a temporary Merge table. The structure will be derived from underlying tables by using a union of their columns and by deriving common types.",
                .examples = {{"merge", "SELECT * FROM merge(db, '^table_.*')", ""}},
                .category = FunctionDocumentation::Category::TableFunction
            },
            .allow_readonly = true,
        }
    );
}

}
