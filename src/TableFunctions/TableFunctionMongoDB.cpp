#include "config.h"

#if USE_MONGODB
#include <Storages/StorageMongoDB.h>

#include <Common/assert_cast.h>
#include <Common/Exception.h>

#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <TableFunctions/TableFunctionMongoDB.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class TableFunctionMongoDB : public ITableFunction
{
public:
    static constexpr auto name = "mongodb";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "MongoDB"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::shared_ptr<MongoDBConfiguration> configuration;
    String structure;
};

StoragePtr TableFunctionMongoDB::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto storage = std::make_shared<StorageMongoDB>(
        StorageID(getDatabaseName(), table_name),
        std::move(*configuration),
        columns,
        ConstraintsDescription(),
        String{});
        storage->startup();
    return storage;
}

ColumnsDescription TableFunctionMongoDB::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return parseColumnsListFromString(structure, context);
}

void TableFunctionMongoDB::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'mongodb' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context))
    {
        if (!named_collection->has("structure"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Required key (structure) is not specified.");

        structure = named_collection->get<String>("structure");
        named_collection->remove("structure");
        configuration = std::make_shared<MongoDBConfiguration>(StorageMongoDB::getConfigurationFromCollection(named_collection, context));
        return;
    }

    if ((args.size() < 3 || args.size() > 4) && (args.size() < 6 || args.size() > 8))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect argument count for table function '{}'. Usage: "
                        "mongodb('host:port', database, collection, user, password, structure[, options[, oid_columns]]) or mongodb(uri, collection, structure[, oid_columns]).",
                        getName());

    /// `getConfiguration` reads its arguments positionally, with `options` preceding `oid_columns`
    /// in the `host:port` form. Named arguments may be given in any order and `options` may be
    /// omitted, so collect the named `options`/`oid_columns` and the positional optional arguments
    /// separately, then place everything into the canonical positions afterwards instead of pushing
    /// them in the order encountered. Otherwise a named `oid_columns` without `options` would be
    /// misread as `options` and silently ignored, and a named `options` followed by a positional
    /// `oid_columns` would overwrite the `options` slot and drop both values.
    ASTs main_arguments;
    main_arguments.reserve(args.size() - 1);
    const bool is_uri_form = args.size() <= 4;
    const size_t structure_position = is_uri_form ? 2 : 5;
    ASTPtr options_argument;
    ASTPtr oid_columns_argument;
    ASTs positional_optionals;
    for (size_t i = 0; i < args.size(); ++i)
    {
        if (const auto * ast_func = typeid_cast<const ASTFunction *>(args[i].get()))
        {
            const auto & [arg_name, arg_value] = getKeyValueMongoDBArgument(ast_func);
            if (arg_name == "structure")
                structure = checkAndGetLiteralArgument<String>(arg_value, arg_name);
            else if (arg_name == "options")
                options_argument = arg_value;
            else if (arg_name == "oid_columns")
                oid_columns_argument = arg_value;
        }
        else if (i == structure_position)
            structure = checkAndGetLiteralArgument<String>(args[i], "structure");
        else if (i < structure_position)
            main_arguments.push_back(args[i]);
        else
            positional_optionals.push_back(args[i]);
    }

    /// Bind the positional optional arguments to the canonical slots that named arguments did not
    /// already fill, in declaration order. The URI form has a single optional slot (`oid_columns`);
    /// the `host:port` form has `options` followed by `oid_columns`.
    size_t next_positional = 0;
    auto take_positional = [&]() -> ASTPtr
    {
        return next_positional < positional_optionals.size() ? positional_optionals[next_positional++] : nullptr;
    };
    if (is_uri_form)
    {
        if (!oid_columns_argument)
            oid_columns_argument = take_positional();
    }
    else
    {
        if (!options_argument)
            options_argument = take_positional();
        if (!oid_columns_argument)
            oid_columns_argument = take_positional();
    }
    if (next_positional < positional_optionals.size())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Too many arguments for table function '{}': a positional optional argument was given "
            "together with named 'options'/'oid_columns' that already occupy the optional slots.",
            getName());

    /// `oid_columns` occupies the slot after `options` in the `host:port` form, so insert an empty
    /// `options` placeholder when `oid_columns` is given without `options`. An empty `options`
    /// string is equivalent to omitting it.
    if (!is_uri_form && oid_columns_argument && !options_argument)
        options_argument = make_intrusive<ASTLiteral>(Field(String()));
    if (options_argument)
        main_arguments.push_back(options_argument);
    if (oid_columns_argument)
        main_arguments.push_back(oid_columns_argument);

    configuration = std::make_shared<MongoDBConfiguration>(StorageMongoDB::getConfiguration(main_arguments, context));
}

}

std::pair<String, ASTPtr> getKeyValueMongoDBArgument(const ASTFunction * ast_func)
{
    const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_func->arguments.get());
    const auto & function_args = args_expr->children;
    if (function_args.size() != 2 || ast_func->name != "equals" || !function_args[0]->as<ASTIdentifier>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument, got {}", ast_func->formatForErrorMessage());

    const auto & arg_name = function_args[0]->as<ASTIdentifier>()->name();
    if (arg_name == "structure" || arg_name == "options" || arg_name == "oid_columns")
        return std::make_pair(arg_name, function_args[1]);

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument, got {}", ast_func->formatForErrorMessage());
}

void registerTableFunctionMongoDB(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMongoDB>(
    {
        .description = "Allows get data from MongoDB collection.",
        .examples = {
            {"Fetch collection by URI", "SELECT * FROM mongodb('mongodb://root:clickhouse@localhost:27017/database', 'example_collection', 'key UInt64, data String')", ""},
            {"Fetch collection over TLS", "SELECT * FROM mongodb('localhost:27017', 'database', 'example_collection', 'root', 'clickhouse', 'key UInt64, data String', 'tls=true')", ""},
            {"Fetch collection by named collection configuration with overrides", "SELECT * FROM mongodb(mongodb_creds, collection='example_collection', structure='key UInt64, data String')", ""},
        },
        .category = FunctionDocumentation::Category::TableFunction
    });
}

}
#endif
