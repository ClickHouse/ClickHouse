#pragma once

#include <absl/container/flat_hash_map.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

class Chunk;

namespace HivePartitioningUtils
{
using HivePartitioningKeysAndValues = absl::flat_hash_map<std::string_view, std::string_view>;

HivePartitioningKeysAndValues parseHivePartitioningKeysAndValues(const std::string & path);

void addPartitionColumnsToChunk(
    Chunk & chunk,
    const NamesAndTypesList & hive_partition_columns_to_read_from_file_path,
    const std::string & path);

void extractPartitionColumnsFromPathAndEnrichStorageColumns(
    ColumnsDescription & storage_columns,
    NamesAndTypesList & hive_partition_columns_to_read_from_file_path,
    const std::string & path,
    bool inferred_schema,
    std::optional<FormatSettings> format_settings,
    ContextPtr context);

}

}
