#pragma once
#include "config.h"

#if USE_AVRO
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <base/defines.h>


namespace DB
{
struct IcebergDataObjectInfo : public RelativePathWithMetadata, std::enable_shared_from_this<IcebergDataObjectInfo>
{
    using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

    /// Full path to the data object file
    /// It is used to filter position deletes objects by data file path.
    /// It is also used to create a filter for the data object in the position delete transform.
    explicit IcebergDataObjectInfo(
        Iceberg::ManifestFileEntry data_manifest_file_entry_,
        const std::vector<Iceberg::ManifestFileEntry> & all_position_delete_entries_,
        String format);

    explicit IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_);

    String data_object_file_path_key; // Full path to the data object file
    Int32 underlying_format_read_schema_id;
    std::vector<Iceberg::ManifestFileEntry> position_deletes_objects;
    Int64 sequence_number;

    bool hasPositionDeleteTransformer() const override { return !position_deletes_objects.empty(); }

    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        ObjectStoragePtr object_storage,
        const SharedHeader & header,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context_) override;
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;
}

#endif
