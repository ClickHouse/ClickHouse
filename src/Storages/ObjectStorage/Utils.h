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

std::unique_ptr<ReadBufferFromFileBase> createReadBuffer(
    RelativePathWithMetadata & object_info,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & context_,
    const LoggerPtr & log,
    const std::optional<ReadSettings> & read_settings = std::nullopt,
    bool allow_page_cache = true);

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

/// Stored tables (not table functions) must interpret their persisted path the same way in
/// every session: configuration parsing (including the archive `::` split), schema/format
/// inference, hive-partition sample-path resolution and reads must not reclassify the path
/// with the per-query `use_glob_ast_parser` setting, otherwise a table created or first
/// resolved under one setting would read a different object set (e.g. the literal key
/// `data_{x}.json` vs the expansion `data_x.json`) under another. Returns a copy of the
/// context with the legacy glob parser pinned (or the context itself when already legacy).
ContextPtr contextWithPinnedGlobParser(const ContextPtr & context);

}
