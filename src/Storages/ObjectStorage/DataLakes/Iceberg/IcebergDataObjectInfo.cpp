#include "config.h"

#if USE_AVRO
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
extern const int NOT_IMPLEMENTED;
}


using namespace DB::Iceberg;

namespace DB
{

namespace Setting
{
extern const SettingsBool use_roaring_bitmap_iceberg_positional_deletes;
};

IcebergDataObjectInfo::IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_)
    : RelativePathWithMetadata(data_manifest_file_entry_.file_path)
    , data_object_file_path_key(data_manifest_file_entry_.file_path_key)
    , underlying_format_read_schema_id(data_manifest_file_entry_.schema_id)
    , sequence_number(data_manifest_file_entry_.added_sequence_number)
    , file_format(data_manifest_file_entry_.file_format)
{
    auto toupper = [](String & str)
    {
        std::transform(str.begin(), str.end(), str.begin(), ::toupper);
        return str;
    };
    if (!position_deletes_objects.empty() && toupper(data_manifest_file_entry_.file_format) != "PARQUET")
    {
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Position deletes are only supported for data files of Parquet format in Iceberg, but got {}",
            data_manifest_file_entry_.file_format);
    }
}

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
}

#endif
