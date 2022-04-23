#include <TableFunctions/TableFunctionFile.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>

#include "registerTableFunctions.h"
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageFile.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTIdentifier_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

void TableFunctionFile::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception("Table function '" + getName() + "' requires at least 1 argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (auto opt_name = tryGetIdentifierName(args[0])) {
        if (*opt_name == "stdin")
            fd = STDIN_FILENO;
        else if (*opt_name == "stdout")
            fd = STDOUT_FILENO;
        else if (*opt_name == "stderr")
            fd = STDERR_FILENO;
        else
            throw Exception("Unknow identifier '" + *opt_name + "' in first second arguments", ErrorCodes::UNKNOWN_IDENTIFIER);
    }
    else if (const auto * literal = args[0]->as<ASTLiteral>()) {
        auto type = literal->value.getType();
        if (type == Field::Types::Int64)
            fd = static_cast<int>(literal->value.get<Int64>());
        else if (type == Field::Types::UInt64)
            fd = static_cast<int>(literal->value.get<UInt64>());
        else if (type == Field::Types::String) {
            filename = literal->value.get<String>();
            if (filename == "-")
                fd = 0;
        }
        else
            throw Exception("The second argument of table function '" + getName() + "' mush be path or file descriptor", ErrorCodes::BAD_ARGUMENTS);
    }

    if (args.size() > 1) {
        args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(args[1], context);
        format = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (format == "auto") {
        if (fd >= 0)
            format = FormatFactory::instance().getFormatFromFileDescriptor(fd);
        else
            format = FormatFactory::instance().getFormatFromFileName(filename, true);
    }

    if (args.size() <= 2)
        return;

    if (args.size() != 3 && args.size() != 4)
        throw Exception("Table function '" + getName() + "' requires 1, 2, 3 or 4 arguments: filename (or file descriptor), format (default auto), structure (default auto) and compression method (default auto)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    args[2] = evaluateConstantExpressionOrIdentifierAsLiteral(args[2], context);
    structure = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    if (structure.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Table structure is empty for table function '{}'. If you want to use automatic schema inference, use 'auto'",
            ast_function->formatForErrorMessage());

    if (args.size() == 4) {
        args[3] = evaluateConstantExpressionOrIdentifierAsLiteral(args[3], context);
        compression_method = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    }
}

StoragePtr TableFunctionFile::getStorage(const String & source,
    const String & format_, const ColumnsDescription & columns,
    ContextPtr global_context, const std::string & table_name,
    const std::string & compression_method_) const
{
    // For `file` table function, we are going to use format settings from the
    // query context.
    StorageFile::CommonArguments args{
        WithContext(global_context),
        StorageID(getDatabaseName(), table_name),
        format_,
        std::nullopt /*format settings*/,
        compression_method_,
        columns,
        ConstraintsDescription{},
        String{},
    };
    if (fd >= 0)
        return StorageFile::create(fd, args);

    return StorageFile::create(source, global_context->getUserFilesPath(), args);
}

ColumnsDescription TableFunctionFile::getActualTableStructure(ContextPtr context) const
{
    if (structure == "auto")
    {
        size_t total_bytes_to_read = 0;
        Strings paths = StorageFile::getPathsList(filename, context->getUserFilesPath(), context, total_bytes_to_read);
        return StorageFile::getTableStructureFromFile(format, paths, compression_method, std::nullopt, context);
    }


    return parseColumnsListFromString(structure, context);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>();
}

}
