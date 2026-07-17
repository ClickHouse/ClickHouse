#pragma once

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
        std::optional<Int64> file_size_bytes_)
        : ObjectInfo(path_)
        , positional_delete_files(std::move(positional_delete_files_))
        , record_count(record_count_)
        , file_size_bytes(file_size_bytes_)
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
};

using DuckLakeDataObjectInfoPtr = std::shared_ptr<DuckLakeDataObjectInfo>;

}
