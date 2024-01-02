#include <TableFunctions/ITableFunctionFileLike.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Common/Exception.h>

#include <Storages/StorageFile.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

void ITableFunctionFileLike::parseFirstArguments(const ASTPtr & arg, const ContextPtr &)
{
    filename = checkAndGetLiteralArgument<String>(arg, "source");
}

String ITableFunctionFileLike::getFormatFromFirstArgument()
{
    return FormatFactory::instance().getFormatFromFileName(filename, true);
}

bool ITableFunctionFileLike::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format, context);
}

void ITableFunctionFileLike::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires at least 1 argument", getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    parseFirstArguments(args[0], context);

    if (args.size() > 1)
        format = checkAndGetLiteralArgument<String>(args[1], "format");

    if (format == "auto")
        format = getFormatFromFirstArgument();

    if (args.size() <= 2)
        return;

    if (args.size() != 3 && args.size() != 4)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires 1, 2, 3 or 4 arguments: "
            "filename, format (default auto), structure (default auto) and compression method (default auto)",
            getName());

    structure = checkAndGetLiteralArgument<String>(args[2], "structure");

    if (structure.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table structure is empty for table function '{}'. If you want to use automatic schema inference, use 'auto'",
            ast_function->formatForErrorMessage());

    if (args.size() == 4)
        compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
}

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    ColumnsDescription columns;
    if (structure != "auto")
        columns = parseColumnsListFromString(structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = getStorage(filename, format, columns, context, table_name, compression_method);
    storage->startup();
    return storage;
}

}
