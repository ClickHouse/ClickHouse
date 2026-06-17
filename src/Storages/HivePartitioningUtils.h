#pragma once

#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

class Chunk;

namespace HivePartitioningUtils
{
using HivePartitioningKeysAndValues = std::map<std::string_view, std::string_view>;

HivePartitioningKeysAndValues parseHivePartitioningKeysAndValues(const std::string & path);

void addPartitionColumnsToChunk(
    Chunk & chunk,
    const NamesAndTypesList & hive_partition_columns_to_read_from_file_path,
    const std::string & path);

/// Hive partition columns and file columns (Note that file columns might not contain the hive partition columns)
using HivePartitionColumnsWithFileColumnsPair = std::pair<NamesAndTypesList, NamesAndTypesList>;

HivePartitionColumnsWithFileColumnsPair setupHivePartitioningForObjectStorage(
    ColumnsDescription & columns,
    const StorageObjectStorageConfigurationPtr & configuration,
    const std::string & sample_path,
    bool inferred_schema,
    std::optional<FormatSettings> format_settings,
    ContextPtr context);

HivePartitionColumnsWithFileColumnsPair setupHivePartitioningForFileURLLikeStorage(
    ColumnsDescription & columns,
    const std::string & sample_path,
    bool inferred_schema,
    std::optional<FormatSettings> format_settings,
    ContextPtr context);

}

}
