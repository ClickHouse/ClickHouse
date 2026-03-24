#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Field.h>
#include <Core/Range.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include <boost/noncopyable.hpp>

namespace DB::Iceberg
{

class AvroForIcebergDeserializer;

enum class ManifestEntryStatus : uint8_t
{
    EXISTING = 0,
    ADDED = 1,
    DELETED = 2,
};

enum class FileContentType : uint8_t
{
    DATA = 0,
    POSITION_DELETE = 1,
    EQUALITY_DELETE = 2
};


enum class ManifestFileContentType
{
    DATA = 0,
    DELETE = 1
};

String FileContentTypeToString(FileContentType type);

struct ColumnInfo
{
    std::optional<Int64> rows_count;
    std::optional<Int64> bytes_size;
    std::optional<Int64> nulls_count;
};

struct PartitionSpecsEntry
{
    Int32 source_id;
    String transform_name;
    String partition_name;
};
using PartitionSpecification = std::vector<PartitionSpecsEntry>;

struct ManifestFileCacheableInfo
{
    std::shared_ptr<AvroForIcebergDeserializer> deserializer;
    size_t file_bytes_size;
};

/// Description of Data file in manifest file
struct ParsedManifestFileEntry : boost::noncopyable
{
    FileContentType content_type;
    /// The original path as stored in the Iceberg metadata.
    /// Must be resolved through IcebergPathResolver before use in storage operations.
    IcebergPathFromMetadata file_path_key;
    Int64 row_number;

    ManifestEntryStatus status;
    std::optional<Int64> parsed_sequence_number;
    std::optional<Int64> parsed_snapshot_id;

    DB::Row partition_key_value;
    std::unordered_map<Int32, ColumnInfo> columns_infos;
    std::unordered_map<Int32, std::pair<Field, Field>> value_bounds;

    String file_format;
    std::optional<IcebergPathFromMetadata> lower_reference_data_file_path; // For position delete files only.
    std::optional<IcebergPathFromMetadata> upper_reference_data_file_path; // For position delete files only.
    std::optional<std::vector<Int32>> equality_ids;

    /// Data file is sorted with this sort_order_id (can be read from metadata.json)
    std::optional<Int32> sort_order_id;

    ParsedManifestFileEntry(
        FileContentType content_type_,
        IcebergPathFromMetadata file_path_key_,
        Int64 row_number_,
        ManifestEntryStatus status_,
        std::optional<Int64> written_sequence_number_,
        std::optional<Int64> written_snapshot_id_,
        DB::Row partition_key_value_,
        std::unordered_map<Int32, ColumnInfo> columns_infos_,
        std::unordered_map<Int32, std::pair<Field, Field>> value_bounds_,
        String file_format_,
        std::optional<IcebergPathFromMetadata> lower_reference_data_file_path_,
        std::optional<IcebergPathFromMetadata> upper_reference_data_file_path_,
        std::optional<std::vector<Int32>> equality_ids_,
        std::optional<Int32> sort_order_id_)
        : content_type(content_type_)
        , file_path_key(std::move(file_path_key_))
        , row_number(row_number_)
        , status(status_)
        , parsed_sequence_number(written_sequence_number_)
        , parsed_snapshot_id(written_snapshot_id_)
        , partition_key_value(std::move(partition_key_value_))
        , columns_infos(std::move(columns_infos_))
        , value_bounds(std::move(value_bounds_))
        , file_format(std::move(file_format_))
        , lower_reference_data_file_path(std::move(lower_reference_data_file_path_))
        , upper_reference_data_file_path(std::move(upper_reference_data_file_path_))
        , equality_ids(std::move(equality_ids_))
        , sort_order_id(sort_order_id_)
    {
    }
};

struct ProcessedManifestFileEntry
{
    std::shared_ptr<const ParsedManifestFileEntry> parsed_entry;
    std::shared_ptr<const PartitionSpecification> common_partition_specification;

    // Always zero in case of format version 1
    Int64 sequence_number;
    Int32 resolved_schema_id;

    String dumpDeletesMatchingInfo() const;
};

using ProcessedManifestFileEntryPtr = std::shared_ptr<const ProcessedManifestFileEntry>;

bool operator<(const PartitionSpecification & lhs, const PartitionSpecification & rhs);
bool operator<(const DB::Row & lhs, const DB::Row & rhs);

std::weak_ordering operator<=>(const ProcessedManifestFileEntryPtr & lhs, const ProcessedManifestFileEntryPtr & rhs);

}

#endif
