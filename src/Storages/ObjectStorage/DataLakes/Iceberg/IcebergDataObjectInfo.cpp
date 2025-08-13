#include "config.h"

#if USE_AVRO

#    include <Core/Settings.h>
#    include <Interpreters/Context.h>
#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

#    include <Core/Types.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

#    include <optional>
#    include <tuple>
#    include <base/defines.h>
#    include <Common/SharedMutex.h>

#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>

#    include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#    include <Common/Exception.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNSUPPORTED_METHOD;
}

namespace {

using namespace DB::Iceberg;


std::pair<size_t, size_t> definePositionDeletesSpan(
    ManifestFileEntry data_object_, const std::vector<ManifestFileEntry> & position_deletes_objects_, const String & format)
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
        [](const ManifestFileEntry & lhs, const ManifestFileEntry & rhs)
        {
            return std::tie(lhs.common_partition_specification, lhs.partition_key_value)
                < std::tie(rhs.common_partition_specification, rhs.partition_key_value);
        });
    if (beg_it - position_deletes_objects_.begin() > end_it - position_deletes_objects_.begin())
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Position deletes objects are not sorted by common_partition_specification and partition_key_value, "
            "beginning: {}, end: {}, position_deletes_objects size: {}",
            beg_it - position_deletes_objects_.begin(),
            end_it - position_deletes_objects_.begin(),
            position_deletes_objects_.size());
    }
    if ((beg_it != end_it) && format != "Parquet")
    {
        throw DB::Exception(
            DB::ErrorCodes::UNSUPPORTED_METHOD,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            format);
    }
    return {beg_it - position_deletes_objects_.begin(), end_it - position_deletes_objects_.begin()};
}

}

namespace DB {

using namespace Iceberg;

IcebergDataObjectInfo::IcebergDataObjectInfo(
    Iceberg::ManifestFileEntry data_manifest_file_entry_,
    const std::vector<Iceberg::ManifestFileEntry> & position_deletes_,
    const String & format)
    : RelativePathWithMetadata(data_manifest_file_entry_.file_path)
    , data_object_file_path_key(data_manifest_file_entry_.file_path_key)
    , read_schema_id(data_manifest_file_entry_.schema_id)
    , position_deletes_objects_range(definePositionDeletesSpan(data_manifest_file_entry_, position_deletes_, format))
{}

IcebergDataObjectInfo::IcebergDataObjectInfo(
    String data_object_file_path_,
    String data_object_file_path_key_,
    Int32 read_schema_id_,
    std::pair<size_t, size_t> position_deletes_objects_)
    : RelativePathWithMetadata(std::move(data_object_file_path_))
    , data_object_file_path_key(std::move(data_object_file_path_key_))
    , read_schema_id(read_schema_id_)
    , position_deletes_objects_range(position_deletes_objects_)
{
}
}


#endif
