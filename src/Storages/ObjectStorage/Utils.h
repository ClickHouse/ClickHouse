#pragma once
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/StorageFactory.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class IObjectStorage;

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

/// An empty column name has no identifier to render it with, so it cannot survive analysis.
void validateLakeSchemaColumnNames(const NamesAndTypesList & schema, std::string_view lake_name);

std::unique_ptr<ReadBufferFromFileBase> createReadBuffer(
    RelativePathWithMetadata & object_info,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context_,
    const LoggerPtr & log,
    const std::optional<ReadSettings> & read_settings = std::nullopt,
    bool allow_page_cache = true);

/// Joins an object's path under a storage prefix (a namespace, or a data source description).
/// A leading separator is dropped only when there is a prefix to join under, since `fs::path`
/// would otherwise treat the path as absolute and discard the prefix. An empty prefix leaves the
/// path as written: on a filesystem-backed storage that separator is what makes a path absolute.
std::string joinPathUnderPrefix(const std::string & prefix, const std::string & path);

/// Inverse of `joinPathUnderPrefix` under the same prefix. An empty prefix again needs care, for
/// the opposite reason: `fs::relative` of an absolute path against an empty base is the empty
/// path, which would lose the value rather than leave it.
std::string relativizePathUnderPrefix(const std::string & prefix, const std::string & path);

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

void expandPaimonKeeperMacrosIfNeeded(
    const StorageFactory::Arguments & args,
    const DataLakeStorageSettingsPtr & storage_settings);


}
