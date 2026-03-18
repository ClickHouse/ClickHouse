#include <TableFunctions/TableFunctionFuzzJSON.h>

#if USE_RAPIDJSON || USE_SIMDJSON
#include <DataTypes/DataTypeString.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{

extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;

}

void TableFunctionFuzzJSON::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments", getName());

    auto args = args_func.at(0)->children;
    configuration = StorageFuzzJSON::getConfiguration(args, context);
}

ColumnsDescription TableFunctionFuzzJSON::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return ColumnsDescription{{"json", std::make_shared<DataTypeString>()}};
}

StoragePtr TableFunctionFuzzJSON::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    ColumnsDescription columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageFuzzJSON>(
        StorageID(getDatabaseName(), table_name),
        columns,
        /* comment */ String{},
        configuration);
    res->startup();
    return res;
}

void registerTableFunctionFuzzJSON(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFuzzJSON>(
        {.documentation
         = {.description = "Perturbs a JSON string with random variations.",
            .returned_value = "A table object with a a single column containing perturbed JSON strings."},
         .allow_readonly = true});
}

}
#endif
