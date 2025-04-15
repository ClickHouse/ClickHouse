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
         = {
            .description = "Perturbs a JSON string with random variations.",
            .syntax="fuzzJSON({ named_collection [, option=value [,..]] | json_str[, random_seed] })",
            .arguments={
                {"named_collection", "A [Named Collection](sql-reference/statements/create/named-collection.md)"},
                {"option=value", "Named collection optional parameters and their values."},
                {"json_str (String)", "The source string representing structured data in JSON format."},
                {"random_seed (UInt64)", "Manual random seed for producing stable results."},
                {"reuse_output (boolean)", "Reuse the output from a fuzzing process as input for the next fuzzer."},
                {"malform_output (boolean)", "Generate a string that cannot be parsed as a JSON object."},
                {"max_output_length (UInt64)", "Maximum allowable length of the generated or perturbed JSON string."},
                {"probability (Float64)", "The probability to fuzz a JSON field (a key-value pair). Must be within [0, 1] range."},
                {"max_nesting_level (UInt64)", "The maximum allowed depth of nested structures within the JSON data."},
                {"max_array_size (UInt64)", "The maximum allowed size of a JSON array."},
                {"max_object_size` (UInt64)", "The maximum allowed number of fields on a single level of a JSON object."},
                {"max_string_value_length (UInt64)", "The maximum length of a String value."},
                {"min_key_length (UInt64)", "The minimum key length. Should be at least 1."},
                {"max_key_length (UInt64)", "The maximum key length. Should be greater or equal than the `min_key_length`, if specified."}
            },
            .returned_value = "A table object with a a single column containing perturbed JSON strings.",
            .examples={
                {
                    "Named collection example",
                    R"(
CREATE NAMED COLLECTION json_fuzzer AS json_str='{}';
SELECT * FROM fuzzJSON(json_fuzzer) LIMIT 3;
                    )",
                    R"(
{"52Xz2Zd4vKNcuP2":true}
{"UPbOhOQAdPKIg91":3405264103600403024}
{"X0QUWu8yT":[]}
                    )"
                }
            },
            .category=FunctionDocumentation::Category::TableFunction
        },
         .allow_readonly = true});
}

}
#endif
