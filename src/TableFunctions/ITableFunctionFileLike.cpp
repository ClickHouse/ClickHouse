#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>

#include <Storages/StorageFile.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() < 2)
        throw Exception("Table function '" + getName() + "' requires at least 2 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    std::string filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    std::string format = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() == 2 && getName() == "file")
    {
        if (format != "Distributed")
            throw Exception("Table function '" + getName() + "' allows 2 arguments only for Distributed format.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
    else if (args.size() != 3 && args.size() != 4)
        throw Exception("Table function '" + getName() + "' requires 3 or 4 arguments: filename, format, structure and compression method (default auto).",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ColumnsDescription columns;
    std::string compression_method = "auto";

    if (args.size() > 2)
    {
        auto structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
        columns = parseColumnsListFromString(structure, context);
    }

    if (args.size() == 4)
        compression_method = args[3]->as<ASTLiteral &>().value.safeGet<String>();

    /// Create table
    StoragePtr storage = getStorage(filename, format, columns, const_cast<Context &>(context), table_name, compression_method);

    storage->startup();

    return storage;
}

}
