#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Common/Exception.h>

#include <Storages/StorageFile.h>
#include <Storages/Distributed/DirectoryMonitor.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Processors/ISource.h>

#include <Formats/FormatFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{
    void checkIfFormatSupportsAutoStructure(const String & name, const String & format)
    {
        if (name == "file" && format == "Distributed")
            return;

        if (FormatFactory::instance().checkIfFormatHasAnySchemaReader(format))
            return;

        throw Exception(
            "Table function '" + name
                + "' allows automatic structure determination only for formats that support schema inference and for Distributed format in table function "
                  "'file'",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

void ITableFunctionFileLike::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception("Table function '" + getName() + "' requires at least 1 argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    filename = args[0]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() > 1)
        format = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    if (format == "auto")
        format = FormatFactory::instance().getFormatFromFileName(filename, true);

    if (args.size() <= 2)
    {
        checkIfFormatSupportsAutoStructure(getName(), format);
        return;
    }

    if (args.size() != 3 && args.size() != 4)
        throw Exception("Table function '" + getName() + "' requires 1, 2, 3 or 4 arguments: filename, format (default auto), structure (default auto) and compression method (default auto)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();
    if (structure == "auto")
        checkIfFormatSupportsAutoStructure(getName(), format);

    if (structure.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table structure is empty for table function '{}'. If you want to use automatic schema inference, use 'auto'",
            ast_function->formatForErrorMessage());

    if (args.size() == 4)
        compression_method = args[3]->as<ASTLiteral &>().value.safeGet<String>();
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
