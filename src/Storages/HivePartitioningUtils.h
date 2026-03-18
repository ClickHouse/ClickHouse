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

/**
 * Before 25.8 hive partition columns were "virtual"
 * and were supported only for reads if the path is globbed.
 * After 25.8 we support both reads and writes and:
 * - reads no longer require globs in url to make hive partitioning work
 * - hive partition columns are no longer virtual, but physical
 * - required to be included in table schema, in TODO to be fixed.
 * There is a new storage level setting - partition_strategy (= "none" by default).
 * If partition_strategy = 'hive', we use the new support for hive partitioning.
 * If partition_strategy = 'none' and use_hive_partitioning (old query level setting) = 1
 * and there are hive partitioned paths,
 * then for compatibility we use old hive partitioning support with hive partition columns as virtual.
 */
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

NamesAndTypesList extractHivePartitionColumnsFromPath(
    const ColumnsDescription & storage_columns,
    const std::string & sample_path,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context);
}

}
