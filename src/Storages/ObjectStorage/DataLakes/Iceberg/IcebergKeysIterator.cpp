

#pragma once
#include "config.h"

#if USE_AVRO
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergKeysIterator.h>

namespace DB
{

IcebergKeysIterator::IcebergKeysIterator(
    const IcebergMetadata & iceberg_metadata_,
    std::vector<Iceberg::ManifestFileEntry> && data_files_,
    std::vector<Iceberg::ManifestFileEntry> && position_deletes_files_,
    ObjectStoragePtr object_storage_,
    IDataLakeMetadata::FileProgressCallback callback_)
    : iceberg_metadata(iceberg_metadata_)
    , data_files(data_files_)
    , position_deletes_files(position_deletes_files_)
    , object_storage(object_storage_)
    , callback(callback_)
{
}

ObjectInfoPtr IcebergKeysIterator::next(size_t)
{
    while (true)
    {
        size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
        if (current_index >= data_files.size())
            return nullptr;

        auto key = data_files[current_index].file_path;
        auto object_metadata = object_storage->getObjectMetadata(key);

        if (callback)
            callback(FileProgress(0, object_metadata.size_bytes));

        return std::make_shared<IcebergDataObjectInfo>(
            iceberg_metadata, data_files[current_index], std::move(object_metadata), position_deletes_files);
    }
}

IcebergDataObjectInfo::IcebergDataObjectInfo(
    const IcebergMetadata & iceberg_metadata,
    Iceberg::ManifestFileEntry data_object_,
    std::optional<ObjectMetadata> metadata_,
    const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_)
    : RelativePathWithMetadata(data_object_.file_path, std::move(metadata_))
    , data_object(data_object_)
{
    ///Object in position_deletes_objects_ are sorted by common_partition_specification, partition_key_value and added_sequence_number.
    /// It is done to have an invariant that position deletes objects which corresponds
    /// to the data object form a subsegment in a position_deletes_objects_ vector.
    /// We need to take all position deletes objects which has the same partition schema and value and has added_sequence_number
    /// greater than or equal to the data object added_sequence_number (https://iceberg.apache.org/spec/#scan-planning)
    /// ManifestFileEntry has comparator by default which helps to do that.
    auto beg_it = std::lower_bound(position_deletes_objects_.begin(), position_deletes_objects_.end(), data_object_);
    auto end_it = std::upper_bound(
        position_deletes_objects_.begin(),
        position_deletes_objects_.end(),
        data_object_,
        [](const Iceberg::ManifestFileEntry & lhs, const Iceberg::ManifestFileEntry & rhs)
        {
            return std::tie(lhs.common_partition_specification, lhs.partition_key_value)
                < std::tie(rhs.common_partition_specification, rhs.partition_key_value);
        });
    if (beg_it - position_deletes_objects_.begin() > end_it - position_deletes_objects_.begin())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Position deletes objects are not sorted by common_partition_specification and partition_key_value, "
            "beginning: {}, end: {}, position_deletes_objects size: {}",
            beg_it - position_deletes_objects_.begin(),
            end_it - position_deletes_objects_.begin(),
            position_deletes_objects_.size());
    }
    position_deletes_objects = std::span<const Iceberg::ManifestFileEntry>{beg_it, end_it};
    if (!position_deletes_objects.empty() && iceberg_metadata.configuration.lock()->format != "Parquet")
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            iceberg_metadata.configuration.lock()->format);
    }
}

}
#endif
