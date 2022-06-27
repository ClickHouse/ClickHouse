#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionS3.h>
#include <TableFunctions/parseColumnsListForTableFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageS3.h>
#include <Formats/FormatFactory.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionS3::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    const auto message = fmt::format(
        "The signature of table function {} could be the following:\n" \
        " - url\n"
        " - url, format\n" \
        " - url, format, structure\n" \
        " - url, access_key_id, secret_access_key\n" \
        " - url, format, structure, compression_method\n" \
        " - url, access_key_id, secret_access_key, format\n"
        " - url, access_key_id, secret_access_key, format, structure\n" \
        " - url, access_key_id, secret_access_key, format, structure, compression_method",
        getName());

    if (args_func.size() != 1)
        throw Exception("Table function '" + getName() + "' must have arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;
    StorageS3Configuration configuration;

    if (auto named_collection = getURLBasedDataSourceConfiguration(args, context))
    {
        auto [common_configuration, storage_specific_args] = named_collection.value();
        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "access_key_id")
                configuration.access_key_id = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            else if (arg_name == "secret_access_key")
                configuration.secret_access_key = arg_value->as<ASTLiteral>()->value.safeGet<String>();
            else
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                                "Unknown key-value argument `{}` for StorageS3, expected: "
                                "url, [access_key_id, secret_access_key], name of used format, structure and [compression_method].",
                                arg_name);
        }
    }
    else
    {
        if (args.empty() || args.size() > 6)
            throw Exception(message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        /// Size -> argument indexes
        static auto size_to_args = std::map<size_t, std::map<String, size_t>>
        {
            {1, {{}}},
            {2, {{"format", 1}}},
            {5, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}}},
            {6, {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}, {"structure", 4}, {"compression_method", 5}}}
        };

        std::map<String, size_t> args_to_idx;
        /// For 4 arguments we support 2 possible variants:
        /// s3(source, format, structure, compression_method) and s3(source, access_key_id, access_key_id, format)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        if (args.size() == 4)
        {
            auto second_arg = args[1]->as<ASTLiteral &>().value.safeGet<String>();
            if (FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}, {"compression_method", 3}};

            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}, {"format", 3}};
        }
        /// For 3 arguments we support 2 possible variants:
        /// s3(source, format, structure) and s3(source, access_key_id, access_key_id)
        /// We can distinguish them by looking at the 2-nd argument: check if it's a format name or not.
        else if (args.size() == 3)
        {
            auto second_arg = args[1]->as<ASTLiteral &>().value.safeGet<String>();
            if (FormatFactory::instance().getAllFormats().contains(second_arg))
                args_to_idx = {{"format", 1}, {"structure", 2}};
            else
                args_to_idx = {{"access_key_id", 1}, {"secret_access_key", 2}};
        }
        else
        {
            args_to_idx = size_to_args[args.size()];
        }

        /// This argument is always the first
        configuration.url = args[0]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("format"))
            configuration.format = args[args_to_idx["format"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("structure"))
            configuration.structure = args[args_to_idx["structure"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("compression_method"))
            configuration.compression_method = args[args_to_idx["compression_method"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("access_key_id"))
            configuration.access_key_id = args[args_to_idx["access_key_id"]]->as<ASTLiteral &>().value.safeGet<String>();

        if (args_to_idx.contains("secret_access_key"))
            configuration.secret_access_key = args[args_to_idx["secret_access_key"]]->as<ASTLiteral &>().value.safeGet<String>();
    }

    if (configuration.format == "auto")
        configuration.format = FormatFactory::instance().getFormatFromFileName(configuration.url, true);

    s3_configuration = std::move(configuration);
}

ColumnsDescription TableFunctionS3::getActualTableStructure(ContextPtr context) const
{
    if (s3_configuration->structure == "auto")
    {
        return StorageS3::getTableStructureFromData(
            s3_configuration->format,
            S3::URI(Poco::URI(s3_configuration->url)),
            s3_configuration->access_key_id,
            s3_configuration->secret_access_key,
            context->getSettingsRef().s3_max_connections,
            context->getSettingsRef().s3_max_single_read_retries,
            s3_configuration->compression_method,
            false,
            std::nullopt,
            context);
    }

    return parseColumnsListFromString(s3_configuration->structure, context);
}

StoragePtr TableFunctionS3::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    Poco::URI uri (s3_configuration->url);
    S3::URI s3_uri (uri);
    UInt64 max_single_read_retries = context->getSettingsRef().s3_max_single_read_retries;
    UInt64 min_upload_part_size = context->getSettingsRef().s3_min_upload_part_size;
    UInt64 upload_part_size_multiply_factor = context->getSettingsRef().s3_upload_part_size_multiply_factor;
    UInt64 upload_part_size_multiply_parts_count_threshold = context->getSettingsRef().s3_upload_part_size_multiply_parts_count_threshold;
    UInt64 max_single_part_upload_size = context->getSettingsRef().s3_max_single_part_upload_size;
    UInt64 max_connections = context->getSettingsRef().s3_max_connections;

    ColumnsDescription columns;
    if (s3_configuration->structure != "auto")
        columns = parseColumnsListFromString(s3_configuration->structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = StorageS3::create(
        s3_uri,
        s3_configuration->access_key_id,
        s3_configuration->secret_access_key,
        StorageID(getDatabaseName(), table_name),
        s3_configuration->format,
        max_single_read_retries,
        min_upload_part_size,
        upload_part_size_multiply_factor,
        upload_part_size_multiply_parts_count_threshold,
        max_single_part_upload_size,
        max_connections,
        columns,
        ConstraintsDescription{},
        String{},
        context,
        /// No format_settings for table function S3
        std::nullopt,
        s3_configuration->compression_method);

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

}

#endif
