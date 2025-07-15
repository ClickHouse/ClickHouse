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

NamesAndTypesList extractHivePartitionColumnsFromPath(
    const ColumnsDescription & storage_columns,
    const std::string & sample_path,
    const std::optional<FormatSettings> & format_settings,
    const ContextPtr & context);

void addPartitionColumnsToChunk(
    Chunk & chunk,
    const NamesAndTypesList & hive_partition_columns_to_read_from_file_path,
    const std::string & path);

}

}
