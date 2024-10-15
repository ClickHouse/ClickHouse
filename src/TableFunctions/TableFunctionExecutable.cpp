
#include <Common/Exception.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/parseQuery.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageExecutable.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <boost/algorithm/string.hpp>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/* executable(script_name_optional_arguments, format, structure, input_query) - creates a temporary storage from executable file
 *
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionExecutable : public ITableFunction
{
public:
    static constexpr auto name = "executable";

    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "Executable"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String script_name;
    std::vector<String> arguments;
    String format;
    String structure;
    std::vector<ASTPtr> input_queries;
    ASTPtr settings_query = nullptr;
};


std::vector<size_t> TableFunctionExecutable::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    const auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    const auto & table_function_node_arguments = table_function_node.getArguments().getNodes();
    size_t table_function_node_arguments_size = table_function_node_arguments.size();

    if (table_function_node_arguments_size <= 3)
        return {};

    std::vector<size_t> result_indexes;
    result_indexes.reserve(table_function_node_arguments_size - 3);
    for (size_t i = 3; i < table_function_node_arguments_size; ++i)
        result_indexes.push_back(i);

    return result_indexes;
}

void TableFunctionExecutable::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table function '{}' must have arguments",
            getName());

    auto args = function->arguments->children;

    if (args.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires minimum 3 arguments: script_name, format, structure, [input_query...]",
            getName());

    auto check_argument = [&](size_t i, const std::string & argument_name)
    {
        if (!args[i]->as<ASTIdentifier>() &&
            !args[i]->as<ASTLiteral>() &&
            !args[i]->as<ASTQueryParameter>() &&
            !args[i]->as<ASTSubquery>() &&
            !args[i]->as<ASTFunction>())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of argument '{}' for table function '{}': must be an identifier or string literal, but got: {}",
                argument_name, getName(), args[i]->formatForErrorMessage());
    };

    check_argument(0, "script_name");
    check_argument(1, "format");
    check_argument(2, "structure");

    for (size_t i = 0; i <= 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    auto script_name_with_arguments_value = checkAndGetLiteralArgument<String>(args[0], "script_name_with_arguments_value");

    std::vector<String> script_name_with_arguments;
    boost::split(script_name_with_arguments, script_name_with_arguments_value, [](char c){ return c == ' '; });

    script_name = std::move(script_name_with_arguments[0]);
    script_name_with_arguments.erase(script_name_with_arguments.begin());
    arguments = std::move(script_name_with_arguments);
    format = checkAndGetLiteralArgument<String>(args[1], "format");
    structure = checkAndGetLiteralArgument<String>(args[2], "structure");

    for (size_t i = 3; i < args.size(); ++i)
    {
        if (args[i]->as<ASTSetQuery>())
        {
            settings_query = std::move(args[i]);
        }
        else
        {
            ASTPtr query;
            if (!args[i]->children.empty())
                query = args[i]->children.at(0);

            if (query && query->as<ASTSelectWithUnionQuery>())
            {
                input_queries.emplace_back(std::move(query));
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Table function '{}' argument is invalid {}",
                    getName(),
                    args[i]->formatForErrorMessage());
            }
        }
    }
}

ColumnsDescription TableFunctionExecutable::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionExecutable::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto storage_id = StorageID(getDatabaseName(), table_name);
    auto global_context = context->getGlobalContext();
    ExecutableSettings settings;
    settings.script_name = script_name;
    settings.script_arguments = arguments;
    if (settings_query != nullptr)
        settings.applyChanges(settings_query->as<ASTSetQuery>()->changes);

    auto storage = std::make_shared<StorageExecutable>(
        storage_id,
        format,
        settings,
        input_queries,
        getActualTableStructure(context, is_insert_query),
        ConstraintsDescription{},
        /* comment = */ "");
    storage->startup();
    return storage;
}

}

void registerTableFunctionExecutable(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExecutable>();
}

}
