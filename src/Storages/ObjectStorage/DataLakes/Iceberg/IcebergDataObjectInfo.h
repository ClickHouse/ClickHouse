#pragma once
#include "config.h"

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/EqualityDeleteObject.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>


namespace DB::Iceberg
{
struct IcebergObjectSerializableInfo
{
    String data_object_file_path_key;
    Int32 underlying_format_read_schema_id;
    Int32 schema_id_relevant_to_iterator;
    Int64 sequence_number;
    String file_format;
    std::vector<Iceberg::PositionDeleteObject> position_deletes_objects;
    std::vector<Iceberg::EqualityDeleteObject> equality_deletes_objects;

    void serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const;
    void deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version);

private:
    void checkVersion(size_t protocol_version) const;
};

}

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <base/defines.h>


namespace DB
{
struct IcebergDataObjectInfo : public ObjectInfo, std::enable_shared_from_this<IcebergDataObjectInfo>
{
    using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

    /// Full path to the data object file
    /// It is used to filter position deletes objects by data file path.
    /// It is also used to create a filter for the data object in the position delete transform.
    explicit IcebergDataObjectInfo(Iceberg::ManifestFileEntry data_manifest_file_entry_, Int32 schema_id_relevant_to_iterator_);

    explicit IcebergDataObjectInfo(const RelativePathWithMetadata & path_);

    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        ObjectStoragePtr object_storage,
        const SharedHeader & header,
        const std::optional<FormatSettings> & format_settings,
        ContextPtr context_);

    std::optional<String> getFileFormat() const override { return info.file_format; }

    void addPositionDeleteObject(Iceberg::ManifestFileEntry position_delete_object);

    void addEqualityDeleteObject(const Iceberg::ManifestFileEntry & equality_delete_object);
    Iceberg::IcebergObjectSerializableInfo info;
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;
}

#endif
