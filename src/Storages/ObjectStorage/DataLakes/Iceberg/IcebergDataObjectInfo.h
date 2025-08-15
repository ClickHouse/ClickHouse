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
struct IcebergDataObjectInfo : public RelativePathWithMetadata
{
    explicit IcebergDataObjectInfo(
        Iceberg::ManifestFileEntry data_manifest_file_entry_,
        const std::vector<Iceberg::ManifestFileEntry> & position_deletes_,
        const String & format);

private:
    String data_object_file_path_key; // Full path to the data object file
    Int32 underlying_format_read_schema_id;
    std::vector<Iceberg::ManifestFileEntry> position_deletes_objects;
    Int64 sequence_number;

    bool hasPositionDeleteTransformer() const override { return !position_deletes_objects.empty(); }
};
#endif

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

}
