#include <TableFunctions/TableFunctionPredict.h>

#include <DataTypes/DataTypeString.h>
#include <Storages/StorageMemory.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{


namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionPredict::parseArguments(const ASTPtr & ast_function, ContextPtr)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());
}

StoragePtr TableFunctionPredict::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    ColumnsDescription columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageMemory>(
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        /* comment */ String{},
        settings);
    res->startup();
    return res;
}

void registerTableFunctionPredict(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPredict>(
        {.documentation
         = {.description = "Perturbs a query string with random variations.",
            .returned_value = "A table object with a single column containing perturbed query strings.",
            .category = FunctionDocumentation::Category::TableFunction
         },
         .allow_readonly = true});
}

}
