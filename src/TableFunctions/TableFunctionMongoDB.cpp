#include <Common/Exception.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <TableFunctions/TableFunctionMongoDB.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
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


StoragePtr TableFunctionMongoDB::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto storage = std::make_shared<StorageMongoDB>(
    StorageID(configuration->database, table_name),
    configuration->host,
    configuration->port,
    configuration->database,
    configuration->table,
    configuration->username,
    configuration->password,
    configuration->options,
    columns,
    ConstraintsDescription(),
    String{});
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionMongoDB::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure, context);
}

void TableFunctionMongoDB::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception("Table function 'mongodb' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    ASTList & args = func_args.arguments->children;

    if (args.size() < 6 || args.size() > 7)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Table function 'mongodb' requires from 6 to 7 parameters: mongodb('host:port', database, collection, 'user', 'password', structure, [, 'options'])");
    }

    auto it = std::next(args.begin(), 5);
    ASTList main_arguments(args.begin(), it);

    for (size_t i = 5; it != args.end(); ++i, ++it)
    {
        if (const auto * ast_func = typeid_cast<const ASTFunction *>(it->get()))
        {
            const auto * args_expr = assert_cast<const ASTExpressionList *>(ast_func->arguments.get());
            auto function_args = args_expr->children;
            if (function_args.size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");

            auto arg_name = function_args.front()->as<ASTIdentifier>()->name();

            if (arg_name == "structure")
                structure = checkAndGetLiteralArgument<String>(function_args.back(), "structure");
            else if (arg_name == "options")
                main_arguments.push_back(function_args.back());
        }
        else if (i == 5)
        {
            structure = checkAndGetLiteralArgument<String>(*it, "structure");
        }
        else if (i == 6)
        {
            main_arguments.push_back(*it);
        }
    }

    configuration = StorageMongoDB::getConfiguration(main_arguments, context);
}


void registerTableFunctionMongoDB(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMongoDB>();
}

}
