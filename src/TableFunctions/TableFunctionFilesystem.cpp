#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFilesystem.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFilesystem.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static const FunctionDocumentation format_table_function_documentation =
{
    .description = R"(
Provides access to file system to list files and return its metadata and contents. Recursively iterates directories.
This table function provides access to filesystem of a server that runs a query.)",
    .examples
    {
        {"Example", R"(Query:
```
:) select * from filesystem('/var/lib/clickhouse/user_files')
```
)", ""
        }
    },
    .categories{"table-functions"}
};

void registerTableFunctionFilesystem(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFilesystem>({format_table_function_documentation, false}, TableFunctionFactory::CaseInsensitive);
}

void TableFunctionFilesystem::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires at least one argument", getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    path = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires path", getName());
}

ColumnsDescription TableFunctionFilesystem::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    auto bool_type = DataTypeFactory::instance().get("Bool");

    ColumnsDescription structure
    {
        {
            {"type", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            {"is_symlink", bool_type},
            {"path", std::make_shared<DataTypeString>()},
            {"size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>())},
            {"modification_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())},
            {"name", std::make_shared<DataTypeString>()},
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
