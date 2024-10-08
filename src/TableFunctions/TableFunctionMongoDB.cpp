#include "config.h"

#if USE_MONGODB
#include <Storages/StorageMongoDB.h>

#include <Common/Exception.h>

#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/ColumnsDescription.h>


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

    const char * getStorageTypeName() const override { return "MongoDB"; }

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

    if (args.size() == 6 || args.size() == 7)
    {
        ASTs main_arguments(args.begin(), args.begin() + 5);

        for (size_t i = 5; i < args.size(); ++i)
        {
            if (const auto * ast_func = typeid_cast<const ASTFunction *>(args[i].get()))
            {
                const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_func->arguments.get());
                auto function_args = args_expr->children;
                if (function_args.size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

                auto arg_name = function_args[0]->as<ASTIdentifier>()->name();

                if (arg_name == "structure")
                    structure = checkAndGetLiteralArgument<String>(function_args[1], "structure");
                else if (arg_name == "options")
                    main_arguments.push_back(function_args[1]);
            }
            else if (i == 5)
            {
                structure = checkAndGetLiteralArgument<String>(args[i], "structure");
            }
            else if (i == 6)
            {
                main_arguments.push_back(args[i]);
            }
        }

        configuration = std::make_shared<MongoDBConfiguration>(StorageMongoDB::getConfiguration(main_arguments, context));
    }
    else if (args.size() == 3)
    {
        ASTs main_arguments(args.begin(), args.begin() + 2);

        for (size_t i = 2; i < args.size(); ++i)
        {
            if (const auto * ast_func = typeid_cast<const ASTFunction *>(args[i].get()))
            {
                const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_func->arguments.get());
                auto function_args = args_expr->children;
                if (function_args.size() != 2)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

                auto arg_name = function_args[0]->as<ASTIdentifier>()->name();

                if (arg_name == "structure")
                    structure = checkAndGetLiteralArgument<String>(function_args[1], "structure");
            }
            else if (i == 2)
            {
                structure = checkAndGetLiteralArgument<String>(args[i], "structure");
            }
        }

        configuration = std::make_shared<MongoDBConfiguration>(StorageMongoDB::getConfiguration(main_arguments, context));
    }
    else
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function 'mongodb' requires 3 or from 6 to 7 parameters: "
                        "mongodb('host:port', database, collection, user, password, structure, [, options]) or mongodb(uri, collection, structure).");
    }
}

}

void registerTableFunctionMongoDB(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMongoDB>(
    {
            .documentation =
            {
                    .description = "Allows get data from MongoDB collection.",
                    .examples = {
                        {"Fetch collection by URI", "SELECT * FROM mongodb('mongodb://root:clickhouse@localhost:27017/database', 'example_collection', 'key UInt64, data String')", ""},
                        {"Fetch collection over TLS", "SELECT * FROM mongodb('localhost:27017', 'database', 'example_collection', 'root', 'clickhouse', 'key UInt64, data String', 'tls=true')", ""},
                    },
                    .categories = {"Integration"},
            },
    });
}

}
#endif
