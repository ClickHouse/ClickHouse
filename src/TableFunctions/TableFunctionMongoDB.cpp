#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Common/quoteString.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <TableFunctions/TableFunctionMongoDB.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include "registerTableFunctions.h"

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
    StorageID(configuration_->database, table_name), 
    configuration_->host,
    configuration_->port,
    configuration_->database,
    configuration_->table,
    configuration_->username,
    configuration_->password,
    configuration_->options,
    columns,
    ConstraintsDescription(),
    String{});
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionMongoDB::getActualTableStructure(ContextPtr context) const
{
    return parseColumnsListFromString(structure_, context);
}

void TableFunctionMongoDB::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments) 
        throw Exception("Table function 'mongodb' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    ASTs & args = func_args.arguments->children;

    if (args.size() < 6 || args.size() > 7)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
        "Table function 'mongodb' requires from 6 to 7 parameters: mongodb('host:port', database, collection, 'user', 'password', structure, [, 'options'])");
    }

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

            auto arg_value_ast = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
            auto * arg_value_literal = arg_value_ast->as<ASTLiteral>();
            if (arg_value_literal)
            {
                auto arg_value = arg_value_literal->value;
                if (arg_name == "structure")
                    structure_ = arg_value.safeGet<String>();
                else if (arg_name == "options")
                    main_arguments.push_back(function_args[1]);
            }
        } 
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value defined argument");
    }
    configuration_ = StorageMongoDB::getConfiguration(main_arguments, context);
}


void registerTableFunctionMongoDB(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMongoDB>();
}

}
