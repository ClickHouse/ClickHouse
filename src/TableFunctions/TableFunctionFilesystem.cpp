#include <filesystem>
#include <Core/NamesAndAliases.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFilesystem.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFilesystem.h>
#include <Interpreters/Context.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void registerTableFunctionFilesystem(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFilesystem>({
        .documentation = {
            .description = R"(
Provides access to file system to list files and return its metadata and contents. Recursively iterates directories.
This table function provides access to filesystem of a server that runs a query.)",
            .examples
            {
                {"Example", R"(Query:
```
:) SELECT * FROM filesystem('/var/lib/clickhouse/user_files')
```
)", ""
                }
            },
            .category = FunctionDocumentation::Category::TableFunction
        }
    }, TableFunctionFactory::Case::Insensitive);
}

void TableFunctionFilesystem::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Wrong AST structure in table function '{}'.", getName());

    ASTs & args = args_func.at(0)->children;

    /// With no arguments it assumes empty path.
    if (args.empty())
        return;

    if (args.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires path as its only argument.", getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    path = args.front()->as<ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionFilesystem::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    auto bool_type = DataTypeFactory::instance().get("Bool");

    DataTypeEnum8::Values file_type_values
    {
        {"none",        0},
        {"not_found",   1},
        {"regular",     2},
        {"directory",   3},
        {"symlink",     4},
        {"block",       5},
        {"character",   6},
        {"fifo",        7},
        {"socket",      8},
        {"unknown",     9},
    };
    auto file_type_enum = std::make_shared<DataTypeEnum8>(std::move(file_type_values));

    ColumnsDescription structure
    {
        {
            {"path", std::make_shared<DataTypeString>()},
            {"name", std::make_shared<DataTypeString>()},
            {"type", std::move(file_type_enum)},
            {"size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
            {"depth", std::make_shared<DataTypeUInt16>()},
            {"modification_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(6))},
            {"is_symlink", bool_type},
            {"content", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            {"owner_read", bool_type},
            {"owner_write", bool_type},
            {"owner_exec", bool_type},
            {"group_read", bool_type},
            {"group_write", bool_type},
            {"group_exec", bool_type},
            {"others_read", bool_type},
            {"others_write", bool_type},
            {"others_exec", bool_type},
            {"set_gid", bool_type},
            {"set_uid", bool_type},
            {"sticky_bit", bool_type}
        }
    };

    structure.setAliases(NamesAndAliases
    {
        {"file", std::make_shared<DataTypeString>(), "name"},
    });

    return structure;
}

StoragePtr TableFunctionFilesystem::executeImpl(const ASTPtr &, ContextPtr context, const std::string & table_name, ColumnsDescription, bool is_insert_query) const
{
    StoragePtr res = std::make_shared<StorageFilesystem>(
        StorageID(getDatabaseName(), table_name), getActualTableStructure(context, is_insert_query), ConstraintsDescription(), String{},
             context->getApplicationType() == Context::ApplicationType::LOCAL, path, fs::canonical(fs::path(context->getUserFilesPath()).string()));
    res->startup();
    return res;
}

}
