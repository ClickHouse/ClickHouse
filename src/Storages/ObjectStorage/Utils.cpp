#include <Core/Settings.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/filesystemHelpers.h>
#include <Core/LogsLevel.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/CachedOnDiskReadBufferFromFile.h>
#include <Disks/IO/getThreadPoolReader.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Parsers/ASTFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Poco/UUIDGenerator.h>
#include <fmt/format.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

std::optional<String> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorageQuerySettings & settings,
    const String & key,
    size_t sequence_number)
{
    if (settings.truncate_on_insert
        || !object_storage.exists(StoredObject(key)))
        return std::nullopt;

    if (settings.create_new_file_on_insert)
    {
        auto pos = key.find_first_of('.');
        String new_key;
        do
        {
            new_key = key.substr(0, pos) + "." + std::to_string(sequence_number) + (pos == std::string::npos ? "" : key.substr(pos));
            ++sequence_number;
        }
        while (object_storage.exists(StoredObject(new_key)));

        return new_key;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Object in bucket {} with key {} already exists. "
        "If you want to overwrite it, enable setting {}_truncate_on_insert, if you "
        "want to create a new file on each insert, enable setting {}_create_new_file_on_insert",
        configuration.getNamespace(), key, configuration.getTypeName(), configuration.getTypeName());
}

void resolveSchemaAndFormat(
    ColumnsDescription & columns,
    std::string & format,
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    std::optional<FormatSettings> format_settings,
    std::string & sample_path,
    const ContextPtr & context)
{
    if (format == "auto")
    {
        if (configuration->isDataLakeConfiguration())
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Format must be already specified for {} storage.",
                configuration->getTypeName());
        }
    }

    if (columns.empty())
    {
        if (configuration->isDataLakeConfiguration())
        {
            auto table_structure = configuration->tryGetTableStructureFromMetadata(context);
            if (table_structure)
                columns = table_structure.value();
        }

        if (columns.empty())
        {
            if (format == "auto")
            {
                std::tie(columns, format) = StorageObjectStorage::resolveSchemaAndFormatFromData(
                    object_storage, configuration, format_settings, sample_path, context);
            }
            else
            {
                chassert(!format.empty());
                columns = StorageObjectStorage::resolveSchemaFromData(object_storage, configuration, format_settings, sample_path, context);
            }
        }
    }
    else if (format == "auto")
    {
        format = StorageObjectStorage::resolveFormatFromData(object_storage, configuration, format_settings, sample_path, context);
    }

    validateColumns(columns, configuration);
}

void validateColumns(
    const ColumnsDescription & columns,
    StorageObjectStorageConfigurationPtr configuration,
    bool validate_schema_with_remote,
    ObjectStoragePtr object_storage,
    const std::optional<FormatSettings> * format_settings,
    const std::string * sample_path,
    ContextPtr context,
    const NamesAndTypesList * hive_partition_columns_to_read_from_file_path,
    const ColumnsDescription * columns_in_table_or_function_definition,
    LoggerPtr log)
{
    if (!columns.hasOnlyOrdinary())
    {
        /// We don't allow special columns.
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Special columns like MATERIALIZED, ALIAS or EPHEMERAL are not supported for {} storage.",
            configuration ? configuration->getTypeName() : "object storage");
    }

    /// If schema validation parameters are not provided, skip schema consistency check.
    /// validateColumns is only called with full arguments from StorageObjectStorage when
    /// resolveSchemaAndFormat was not used (validate_schema_with_remote == true).
    if (!object_storage || !configuration || !format_settings || !sample_path || !context
        || !hive_partition_columns_to_read_from_file_path || !columns_in_table_or_function_definition || !log)
        return;

    /// We don't check csv and tsv formats because they change column names.
    if (!validate_schema_with_remote
        || configuration->format == "CSV"
        || configuration->format == "TSV")
        return;

    /// Verify that explicitly specified columns exist in the schema inferred from data.
    String sample_path_schema = *sample_path;
    std::optional<ColumnsDescription> schema_file;
    try
    {
        schema_file = StorageObjectStorage::resolveSchemaFromData(
            object_storage,
            configuration,
            *format_settings,
            sample_path_schema,
            context);
    }
    catch (...)
    {
        tryLogCurrentException(log, "while verifying schema consistency", LogsLevel::debug);
    }
    if (schema_file)
    {
        auto hive_partitioning_columns = hive_partition_columns_to_read_from_file_path->getNameSet();
        for (const auto & column : *columns_in_table_or_function_definition)
            if (!schema_file->tryGet(column.name) && !hive_partitioning_columns.contains(column.name))
            {
                String hive_columns;
                for (const auto & hive_column : hive_partitioning_columns)
                    hive_columns += hive_column + ";";
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find column {} in schema {}, hive columns {}", column.name, schema_file->toString(false), hive_columns);
            }
    }
}

ASTs::iterator getFirstKeyValueArgument(ASTs & args)
{
    ASTs::iterator first_key_value_arg_it = args.end();
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const auto * function_ast = (*it)->as<ASTFunction>();
        if (function_ast && function_ast->name == "equals")
        {
             if (first_key_value_arg_it == args.end())
                first_key_value_arg_it = it;
        }
        else if (first_key_value_arg_it != args.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Expected positional arguments to go before key-value arguments");
        }
    }
    return first_key_value_arg_it;
}

std::unordered_map<std::string, Field> parseKeyValueArguments(const ASTs & function_args, ContextPtr context)
{
    std::unordered_map<std::string, Field> key_value_args;
    for (const auto & arg : function_args)
    {
        const auto * function_ast = arg->as<ASTFunction>();
        if (!function_ast || function_ast->name != "equals")
            continue;

        auto * args_expr = assert_cast<ASTExpressionList *>(function_ast->arguments.get());
        auto & children = args_expr->children;
        if (children.size() != 2)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Key value argument is incorrect: expected 2 arguments, got {}",
                children.size());
        }

        auto key_literal = evaluateConstantExpressionOrIdentifierAsLiteral(children[0], context);
        auto value_literal = evaluateConstantExpressionOrIdentifierAsLiteral(children[1], context);

        auto arg_name_value = key_literal->as<ASTLiteral>()->value;
        if (arg_name_value.getType() != Field::Types::Which::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected string as credential name");

        auto arg_name = arg_name_value.safeGet<String>();
        auto arg_value = value_literal->as<ASTLiteral>()->value;

        auto inserted = key_value_args.emplace(arg_name, arg_value).second;
        if (!inserted)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Duplicate key value argument: {}", arg_name);
    }
    return key_value_args;
}

ParseFromDiskResult parseFromDisk(ASTs args, bool with_structure, ContextPtr context, const fs::path & prefix)
{
    if (args.size() > 3 + with_structure)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Storage requires {} arguments maximum.",
            2 + with_structure);

    std::unordered_map<std::string_view, size_t> engine_args_to_idx;

    if (with_structure)
    {
        if (args.size() > 3)
            engine_args_to_idx = {{"path", 0}, {"format", 1}, {"structure", 2}, {"compression_method", 3}};
        else if (args.size() > 2)
            engine_args_to_idx = {{"path", 0}, {"format", 1}, {"structure", 2}};
        else if (args.size() > 1)
            engine_args_to_idx = {{"path", 0}, {"structure", 1}};
        else if (!args.empty())
            engine_args_to_idx = {{"path", 0}};
    }
    else if (args.size() > 2)
        engine_args_to_idx = {{"path", 0}, {"format", 1}, {"compression_method", 2}};
    else if (args.size() > 1)
        engine_args_to_idx = {{"path", 0}, {"format", 1}};
    else if (!args.empty())
        engine_args_to_idx = {{"path", 0}};

    ASTs key_value_asts;
    if (auto first_key_value_arg_it = getFirstKeyValueArgument(args);
        first_key_value_arg_it != args.end())
    {
        key_value_asts = ASTs(first_key_value_arg_it, args.end());
    }

    auto key_value_args = parseKeyValueArguments(key_value_asts, context);

    ParseFromDiskResult result;
    if (auto path_value = getFromPositionOrKeyValue<String>("path", args, engine_args_to_idx, key_value_args);
        path_value.has_value())
    {
        result.path_suffix = path_value.value();
        if (result.path_suffix.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty path is not allowed");

        if (result.path_suffix.starts_with('/') || !pathStartsWith(prefix / fs::path(result.path_suffix), prefix))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path suffixes starting with '.' or '..' and absolute paths are not allowed. Please specify relative path");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path should be specified as first argument or via `path = <path/to/data>` key value argument");

    if (auto format_value = getFromPositionOrKeyValue<String>("format", args, engine_args_to_idx, key_value_args);
        format_value.has_value())
    {
        result.format = format_value.value();
    }

    if (auto structure_value = getFromPositionOrKeyValue<String>("structure", args, engine_args_to_idx, key_value_args);
        structure_value.has_value())
    {
        result.structure = structure_value.value();
    }

    if (auto compression_method_value = getFromPositionOrKeyValue<String>("compression_method", args, engine_args_to_idx, key_value_args);
        compression_method_value.has_value())
    {
        result.compression_method = compression_method_value.value();
    }
    return result;
}

namespace Setting
{
extern const SettingsUInt64 max_download_buffer_size;
extern const SettingsBool use_cache_for_count_from_files;
extern const SettingsString filesystem_cache_name;
extern const SettingsUInt64 filesystem_cache_boundary_alignment;
}
}
