#pragma once
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ColumnsDescription.h>
#include <Parsers/IAST_fwd.h>
#include <Formats/FormatSettings.h>
#include <filesystem>
namespace fs = std::filesystem;

namespace DB
{

class IObjectStorage;
class ReadBufferIterator;
class SchemaCache;

std::optional<std::string> checkAndGetNewFileOnInsertIfNeeded(
    const IObjectStorage & object_storage,
    const StorageObjectStorageConfiguration & configuration,
    const StorageObjectStorageQuerySettings & settings,
    const std::string & key,
    size_t sequence_number);

void resolveSchemaAndFormat(
    ColumnsDescription & columns,
    std::string & format,
    ObjectStoragePtr object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    std::optional<FormatSettings> format_settings,
    std::string & sample_path,
    const ContextPtr & context);

void validateSupportedColumns(
    ColumnsDescription & columns,
    const StorageObjectStorageConfiguration & configuration);

std::unique_ptr<ReadBufferFromFileBase> createReadBuffer(
    RelativePathWithMetadata & object_info,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context_,
    const LoggerPtr & log,
    const std::optional<ReadSettings> & read_settings = std::nullopt);

ASTs::iterator getFirstKeyValueArgument(ASTs & args);
std::unordered_map<std::string, Field> parseKeyValueArguments(const ASTs & function_args, ContextPtr context);

template <typename T>
std::optional<T> getFromPositionOrKeyValue(
    const std::string & key,
    const ASTs & args,
    const std::unordered_map<std::string_view, size_t> & engine_args_to_idx,
    const std::unordered_map<std::string, Field> & key_value_args)
{
    if (auto arg_it = key_value_args.find(key); arg_it != key_value_args.end())
        return arg_it->second.safeGet<T>();

    if (auto arg_it = engine_args_to_idx.find(key); arg_it != engine_args_to_idx.end())
        return checkAndGetLiteralArgument<T>(args[arg_it->second], key);

    return std::nullopt;
};

struct ParseFromDiskResult
{
    String path_suffix;
    std::optional<String> format;
    std::optional<String> structure;
    std::optional<String> compression_method;
};

ParseFromDiskResult parseFromDisk(ASTs args, bool with_structure, ContextPtr context, const fs::path & prefix);

SchemaCache & getSchemaCache(const ContextPtr & context, const std::string & storage_engine_name);

ColumnsDescription resolveSchemaFromData(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    std::string & sample_path,
    const ContextPtr & context);

std::string resolveFormatFromData(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    std::string & sample_path,
    const ContextPtr & context);

std::pair<ColumnsDescription, std::string> resolveSchemaAndFormatFromData(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    std::string & sample_path,
    const ContextPtr & context);

std::unique_ptr<ReadBufferIterator> createReadBufferIterator(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::optional<FormatSettings> & format_settings,
    ObjectInfos & read_keys,
    const ContextPtr & context);

}
