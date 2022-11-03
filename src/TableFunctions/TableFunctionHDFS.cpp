#include "config.h"
#include "registerTableFunctions.h"

#if USE_HDFS
#include <Storages/HDFS/StorageHDFS.h>
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionHDFS.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Access/Common/AccessFlags.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Formats/FormatFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionHDFS::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    if (!object_infos && StorageHDFS::shouldCollectObjectInfos(context))
        object_infos.emplace();

    auto storage = std::make_shared<StorageHDFS>(
        configuration.url,
        StorageID(getDatabaseName(), table_name),
        configuration.format,
        columns,
        ConstraintsDescription{},
        String{},
        context,
        object_infos,
        configuration.compression_method);

    storage->startup();

    return storage;
}

ColumnsDescription TableFunctionHDFS::getActualTableStructure(ContextPtr context) const
{
    if (configuration.structure == "auto")
    {
        context->checkAccess(getSourceAccessType());

        if (!object_infos && StorageHDFS::shouldCollectObjectInfos(context))
            object_infos.emplace();

        return StorageHDFS::getTableStructureFromData(
            configuration.format, configuration.url, configuration.compression_method,
            object_infos ? &*object_infos : nullptr, context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

void TableFunctionHDFS::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    if (args.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function '{}' must have at least 1 argument.", getName());

    if (auto named_collection = getURLBasedDataSourceConfiguration(args, context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "filename")
                configuration.url = std::filesystem::path(configuration.url) / checkAndGetLiteralArgument<String>(arg_value, "filename");
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected key-value argument `{}`", arg_name);
        }
    }
    else
    {
        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        configuration.url = checkAndGetLiteralArgument<String>(args[0], "url");

        if (args.size() > 1)
            configuration.format = checkAndGetLiteralArgument<String>(args[1], "format");

        if (configuration.format == "auto")
            configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url, true);

        if (args.size() <= 2)
            return;

        if (args.size() != 3 && args.size() != 4)
        {
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Table function '{}' requires 1, 2, 3 or 4 arguments: "
                "filename, format (default auto), structure (default auto) and compression method (default auto)",
                getName());
        }

        configuration.structure = checkAndGetLiteralArgument<String>(args[2], "structure");

        if (configuration.structure.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Table structure is empty for table function '{}'. If you want to use automatic schema inference, use 'auto'",
                ast_function->formatForErrorMessage());
        }

        if (args.size() == 4)
            configuration.compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
    }
}

void registerTableFunctionHDFS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionHDFS>();
}

}
#endif
