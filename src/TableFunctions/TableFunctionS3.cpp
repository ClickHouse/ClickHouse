#include "config.h"

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StorageS3.h>
#include <Storages/StorageURL.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Formats/FormatFactory.h>
#include "registerTableFunctions.h"
#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// This is needed to avoid copy-pase. Because s3Cluster arguments only differ in additional argument (first) - cluster name
void TableFunctionS3::parseArgumentsImpl(
    const String & error_message, ASTs & args, ContextPtr context, StorageS3::Configuration & s3_configuration, bool get_format_from_file)
{
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args))
    {
        StorageS3::processNamedCollectionResult(s3_configuration, *named_collection);
    }
    else
    {
        if (args.empty() || args.size() > 6)
            throw Exception::createDeprecated(error_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        auto * header_it = StorageURL::collectHeaders(args, s3_configuration.headers_from_ast, context);
        if (header_it != args.end())
            args.erase(header_it);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        /// Size -> argument indexes
        static std::unordered_map<size_t, std::unordered_map<std::string_view, size_t>> size_to_args
        {
            {1, {{}}},
            {2, {{"format", 1}}},
            {5, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}}},
            {6, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}}}
        };

        std::unordered_map<std::string_view, size_t> args_to_idx;
        /// For 4 arguments we support 2 possible variants:
        /// s3(source, format, structure, compression_method) and s3(source, access_key_id, access_key_id, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        if (args.size() == 4)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id");
            if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};

            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
        }
        /// For 3 arguments we support 2 possible variants:
        /// s3(source, format, structure) and s3(source, access_key_id, access_key_id)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        else if (args.size() == 3)
        {

            auto second_arg = checkAndGetLiteralArgument<String>(args[1], "format/access_key_id");
            if (second_arg == "auto" || FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}};
            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        else
        {
            args_to_idx = size_to_args[args.size()];
        }

        /// This argument is always the first
        s3_configuration.url = S3::URI(checkAndGetLiteralArgument<String>(args[0], "url"));

        if (args_to_idx.contains("format"))
            s3_configuration.format = checkAndGetLiteralArgument<String>(args[args_to_idx["format"]], "format");

        if (args_to_idx.contains("structure"))
            s3_configuration.structure = checkAndGetLiteralArgument<String>(args[args_to_idx["structure"]], "structure");

        if (args_to_idx.contains("compression_method"))
            s3_configuration.compression_method = checkAndGetLiteralArgument<String>(args[args_to_idx["compression_method"]], "compression_method");

        if (args_to_idx.contains("access_key_id"))
            s3_configuration.auth_settings.access_key_id = checkAndGetLiteralArgument<String>(args[args_to_idx["access_key_id"]], "access_key_id");

        if (args_to_idx.contains("secret_access_key"))
            s3_configuration.auth_settings.secret_access_key = checkAndGetLiteralArgument<String>(args[args_to_idx["secret_access_key"]], "secret_access_key");
    }

    /// For DataLake table functions, we should specify default format.
    if (s3_configuration.format == "auto" && get_format_from_file)
        s3_configuration.format = FormatFactory::instance().getFormatFromFileName(s3_configuration.url.uri.getPath(), true);
}

void TableFunctionS3::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    const auto message = fmt::format("The signature of table function '{}' could be the following:\n{}", getName(), signature);

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    auto & args = args_func.at(0)->children;

    parseArgumentsImpl(message, args, context, configuration);
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    if (configuration.structure == "auto")
    {
        context->checkAccess(getSourceAccessType());
        return StorageS3::getTableStructureFromData(configuration, std::nullopt, context);
    }

    return parseColumnsListFromString(configuration.structure, context);
}

bool TableFunctionS3::supportsReadingSubsetOfColumns()
{
    return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(configuration.format);
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    S3::URI s3_uri (configuration.url);

    ColumnsDescription columns;
    if (configuration.structure != "auto")
        columns = parseColumnsListFromString(configuration.structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = std::make_shared<StorageS3>(
        configuration,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        String{},
        context,
        /// No format_settings for table function S3
        std::nullopt);

    storage->startup();

    return storage;
}


void registerTableFunctionS3(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionS3>();
}

void registerTableFunctionCOS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionCOS>();
}

void registerTableFunctionOSS(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionOSS>();
}

}

#endif
