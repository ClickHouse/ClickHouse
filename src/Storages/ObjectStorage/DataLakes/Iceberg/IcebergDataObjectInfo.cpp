#include "config.h"

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <base/defines.h>
#include <Common/SharedMutex.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

#if USE_AVRO
namespace
{

using namespace DB::Iceberg;


std::vector<ManifestFileEntry>
definePositionDeletesSpan(ManifestFileEntry data_object_, const std::vector<ManifestFileEntry> & position_deletes_objects_)
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
    return {beg_it, end_it};
}

}

#endif

namespace DB
{

namespace Setting
{
extern const SettingsBool use_roaring_bitmap_iceberg_positional_deletes;
};

#if USE_AVRO

IcebergDataObjectInfo::IcebergDataObjectInfo(
    Iceberg::ManifestFileEntry data_manifest_file_entry_,
    const std::vector<Iceberg::ManifestFileEntry> & all_position_delete_entries_,
    const std::vector<Iceberg::ManifestFileEntry> & all_equality_delete_entries_,
    String format_)
    : RelativePathWithMetadata(data_manifest_file_entry_.file_path)
    , data_object_file_path_key(data_manifest_file_entry_.file_path_key)
    , underlying_format_read_schema_id(data_manifest_file_entry_.schema_id)
    , position_deletes_objects(definePositionDeletesSpan(data_manifest_file_entry_, all_position_delete_entries_))
    , equality_deletes_objects(definePositionDeletesSpan(data_manifest_file_entry_, all_equality_delete_entries_))
{
    auto toupper = [](String & str)
    {
        std::transform(str.begin(), str.end(), str.begin(), ::toupper);
        return str;
    };
    if (!position_deletes_objects.empty() && toupper(format_) != "PARQUET")
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            format_);
    }
}

IcebergDataObjectInfo::IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_)
    : RelativePathWithMetadata(data_manifest_file_entry_.file_path)
    , data_object_file_path_key(data_manifest_file_entry_.file_path_key)
    , underlying_format_read_schema_id(data_manifest_file_entry_.schema_id)
    , position_deletes_objects({})
{}

std::shared_ptr<ISimpleTransform> IcebergDataObjectInfo::getPositionDeleteTransformer(
    ObjectStoragePtr object_storage,
    const SharedHeader & header,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context_)
{
    IcebergDataObjectInfoPtr self = shared_from_this();
    if (!context_->getSettingsRef()[Setting::use_roaring_bitmap_iceberg_positional_deletes].value)
        return std::make_shared<IcebergStreamingPositionDeleteTransform>(header, self, object_storage, format_settings, context_);
    else
        return std::make_shared<IcebergBitmapPositionDeleteTransform>(header, self, object_storage, format_settings, context_);
}

#endif
}
