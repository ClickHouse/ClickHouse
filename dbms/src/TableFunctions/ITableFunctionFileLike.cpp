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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & ast_function, const Context & context, const std::string & table_name) const
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 3 && args.size() != 4)
        throw Exception("Table function '" + getName() + "' requires 3 or 4 arguments: filename, format, structure and compression method (default auto).",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < args.size(); ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    std::string filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    std::string format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    std::string structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    std::string compression_method;

    if (args.size() == 4)
    {
        compression_method = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    } else compression_method = "auto";

    ColumnsDescription columns = parseColumnsListFromString(structure, context);

    /// Create table
    StoragePtr storage = getStorage(filename, format, columns, const_cast<Context &>(context), table_name, compression_method);

    storage->startup();

    return storage;
}

}
