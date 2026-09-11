#pragma once
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/StorageFactory.h>
#include <Parsers/IAST_fwd.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <mutex>
#include <map>

namespace DB
{

class IObjectStorage;

#if USE_AVRO
/// Thread-safe wrapper for secondary object storages map
/// (now only used for Iceberg)
struct SecondaryStorages
{
    mutable std::mutex mutex;
    std::map<std::string, ObjectStoragePtr> storages;
};
#endif

// A URI split into components
//  s3://bucket/a/b -> scheme="s3", authority="bucket", path="/a/b"
//  file:///var/x    -> scheme="file", authority="",     path="/var/x"
//  /abs/p           -> scheme="",     authority="",     path="/abs/p"
struct SchemeAuthorityKey
{
    explicit SchemeAuthorityKey(const std::string & uri);

    std::string scheme;
    std::string authority;
    std::string key;
};

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

std::string formatObjectPath(
    const StorageObjectStorageConfiguration & configuration, const std::string & path, bool include_connection_info);
/// `joinPathUnderPrefix` is not injective under a non-empty prefix: a key with a leading separator
/// and the same key without it render to the same `_path` value, so `relativizePathUnderPrefix`
/// alone cannot tell which of them produced a given value. Returns every key that could have, so
/// that a caller which needs the original key can see when the answer is not unique.
Strings candidateKeysUnderPrefix(const std::string & prefix, const std::string & path);

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

#if USE_AVRO
namespace Iceberg { class IcebergPathResolver; }

/// Resolve an absolute metadata path directly to its (object storage, key) by parsing the URI.
/// The storage may be `base_storage` or a secondary one. Returns std::nullopt for paths that must
/// instead go through `path_resolver`: relative paths and scheme-less absolute base-storage paths.
std::optional<std::pair<DB::ObjectStoragePtr, std::string>> tryResolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    SecondaryStorages & secondary_storages,
    const DB::ContextPtr & context);

/// Resolve a metadata path to (object storage, key) for reading. Absolute paths resolve directly via
/// `tryResolveObjectStorageForPath`; relative paths are mapped via `path_resolver`.
std::pair<DB::ObjectStoragePtr, std::string> resolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const DB::ObjectStoragePtr & base_storage,
    SecondaryStorages & secondary_storages,
    const DB::ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver);
#endif

void expandPaimonKeeperMacrosIfNeeded(
    const StorageFactory::Arguments & args,
    const DataLakeStorageSettingsPtr & storage_settings);


}
