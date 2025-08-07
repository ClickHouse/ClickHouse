#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergObjectInfo.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Storages/ObjectStorage/Iterators/ObjectInfo.h>




namespace DB {

IcebergDataObjectInfo::IcebergDataObjectInfo(std::optional<ObjectMetadata> metadata_, ParsedDataFileInfo parsed_data_file_info_)
    : ObjectInfoBase(parsed_data_file_info_.data_object_file_path, std::move(metadata_))
    , parsed_data_file_info(std::move(parsed_data_file_info_))
{
}

ParsedDataFileInfo::ParsedDataFileInfo(
    StorageObjectStorageConfigurationPtr configuration_,
    Iceberg::ManifestFileEntry data_object_,
    const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_)
    : data_object_file_path_key(data_object_.file_path_key)
    , data_object_file_path(data_object_.file_path)
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
    if (!position_deletes_objects.empty() && configuration_->format != "Parquet")
    {
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            configuration_->format);
    }
}

}
