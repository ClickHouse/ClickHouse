#pragma once
#include "config.h"

#if USE_AVRO
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>
#include <base/defines.h>


namespace DB
{
struct IcebergDataObjectInfo : public RelativePathWithMetadata, std::enable_shared_from_this<IcebergDataObjectInfo>
{
    using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

    /// Full path to the data object file
    /// It is used to filter position deletes objects by data file path.
    /// It is also used to create a filter for the data object in the position delete transform.
    explicit IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_);

    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        ObjectStoragePtr object_storage,
        const SharedHeader & header,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context_);

    std::optional<String> getFileFormat() const override { return file_format; }

    void addPositionDeleteObject(Iceberg::ManifestFileEntry position_delete_object)
    {
        position_deletes_objects.emplace_back(
            position_delete_object.file_path, position_delete_object.file_format, position_delete_object.reference_data_file_path);
    }

    void addEqualityDeleteObject(const Iceberg::ManifestFileEntry & equality_delete_object)
    {
        equality_deletes_objects.emplace_back(equality_delete_object);
    }

    String data_object_file_path_key; // Full path to the data object file
    Int32 underlying_format_read_schema_id;
    std::vector<Iceberg::PositionDeleteObject> position_deletes_objects;
    std::vector<Iceberg::ManifestFileEntry> equality_deletes_objects;
    Int64 sequence_number;
    String file_format;
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;
}

#endif
