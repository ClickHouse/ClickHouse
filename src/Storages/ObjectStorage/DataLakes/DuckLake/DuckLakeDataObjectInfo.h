#pragma once

#include <Formats/FormatFilterInfo.h>
#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePartitionConstantsTransform.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

namespace DB
{

/// One data file of a DuckLake table, with the positional delete files bound to it
/// (DuckLake binds delete files 1:1 via ducklake_delete_file.data_file_id).
struct DuckLakeDataObjectInfo : public ObjectInfo
{
    struct PositionalDeleteFile
    {
        /// Path relative to the object storage root (the table data path).
        String path;
        Int64 delete_count;
    };

    explicit DuckLakeDataObjectInfo(
        const String & path_,
        std::vector<PositionalDeleteFile> positional_delete_files_,
        std::optional<Int64> record_count_,
        std::optional<Int64> file_size_bytes_,
        std::vector<UInt64> inlined_deleted_positions_ = {})
        : ObjectInfo(path_)
        , positional_delete_files(std::move(positional_delete_files_))
        , record_count(record_count_)
        , file_size_bytes(file_size_bytes_)
        , inlined_deleted_positions(std::move(inlined_deleted_positions_))
    {
    }

    std::optional<size_t> getFileSizeHint() const override
    {
        if (file_size_bytes.has_value())
            return static_cast<size_t>(*file_size_bytes);
        return std::nullopt;
    }

    std::vector<PositionalDeleteFile> positional_delete_files;
    std::optional<Int64> record_count;
    std::optional<Int64> file_size_bytes;
    /// File-relative positions deleted via the catalog's inlined deletion table.
    std::vector<UInt64> inlined_deleted_positions;
    /// Set for files added via ducklake_add_data_files: the parquet columns are matched
    /// by name (ducklake_name_mapping) instead of by field id, so the file needs its own
    /// ColumnMapper instead of the table-wide one.
    ColumnMapperPtr column_mapper;
    /// Hive partition columns whose values come from the catalog rather than the parquet
    /// content (is_partition name mappings).
    std::vector<DuckLakePartitionConstantsTransform::ConstantColumn> partition_constants;
};

using DuckLakeDataObjectInfoPtr = std::shared_ptr<DuckLakeDataObjectInfo>;

}
