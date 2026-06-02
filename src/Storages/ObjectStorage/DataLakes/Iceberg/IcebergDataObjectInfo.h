#pragma once
#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/EqualityDeleteObject.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>

#include <Core/Field.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>


namespace DB::Iceberg
{

String computePartitionId(const Row & partition_key_value);


struct IcebergObjectSerializableInfo
{
    IcebergPathFromMetadata data_object_file_path_key;
    /// Raw path string as written in the Iceberg manifest, preserved as-is (may be a full URI like
    /// `s3://bucket/...` or a relative path). Used for the `_path` virtual column and as a stable
    /// task identifier. Not a canonicalised storage key — see `IcebergPathResolver::resolve` for that.
    String data_object_file_metadata_path;
    Int32 underlying_format_read_schema_id{};
    Int32 schema_id_relevant_to_iterator{};
    Int64 sequence_number{};
    String file_format;
    String manifest_file;
    String partition_id;
    std::vector<Iceberg::PositionDeleteObject> position_deletes_objects;
    std::vector<Iceberg::EqualityDeleteObject> equality_deletes_objects;
    std::optional<Int64> record_count;
    std::optional<Int64> file_size_in_bytes;

    /// Set to true by the coordinator when the file is outside of the table location
    bool requires_external_storage = false;

    void serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const;
    void deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version);

private:
    void checkVersion(size_t protocol_version) const;
};

}

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/Utils.h>
#include <base/defines.h>


namespace DB
{

struct FormatParserSharedResources;
using FormatParserSharedResourcesPtr = std::shared_ptr<FormatParserSharedResources>;

struct IcebergDataObjectInfo : public ObjectInfo, std::enable_shared_from_this<IcebergDataObjectInfo>
{
    using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

    /// Full path to the data object file
    /// It is used to filter position deletes objects by data file path.
    /// It is also used to create a filter for the data object in the position delete transform.
    /// If resolved_storage_ and resolved_key_ are provided, the file may be located
    /// outside the table location (possibly in a different storage).
    explicit IcebergDataObjectInfo(
        Iceberg::ProcessedManifestFileEntryPtr data_manifest_file_entry_,
        const String & metadata_path_,
        Int32 schema_id_relevant_to_iterator_,
        ObjectStoragePtr resolved_storage_ = nullptr,
        const String & resolved_key_ = "");

    explicit IcebergDataObjectInfo(const RelativePathWithMetadata & path_);
    explicit IcebergDataObjectInfo(const RelativePathWithMetadata & path_, const Iceberg::IcebergObjectSerializableInfo & info_);

    std::shared_ptr<ISimpleTransform> getPositionDeleteTransformer(
        ObjectStoragePtr object_storage,
        const SharedHeader & header,
        const std::optional<FormatSettings> & format_settings,
        FormatParserSharedResourcesPtr parser_shared_resources,
        ContextPtr context_,
        const Iceberg::IcebergPathResolver & path_resolver,
        std::shared_ptr<SecondaryStorages> secondary_storages);

    std::optional<String> getFileFormat() const override { return info.file_format; }

    void addPositionDeleteObject(Iceberg::ProcessedManifestFileEntryPtr position_delete_object, const String & resolved_storage_path);

    std::optional<String> getMetadataPath() const
    {
        if (info.data_object_file_metadata_path.empty())
            return std::nullopt;
        return info.data_object_file_metadata_path;
    }

    ObjectStoragePtr getResolvedStorage() const { return resolved_storage; }

    void setResolvedStorage(ObjectStoragePtr storage) { resolved_storage = std::move(storage); }

    void addEqualityDeleteObject(const Iceberg::ProcessedManifestFileEntryPtr & equality_delete_object, const String & resolved_storage_path);
    Iceberg::IcebergObjectSerializableInfo info;

private:
    /// For files located in a different storage than the table's main storage
    ObjectStoragePtr resolved_storage;
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;
}

#endif
