#pragma once
#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/DeletionVectorObject.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/EqualityDeleteObject.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteObject.h>

#include <Core/Field.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <base/types.h>

namespace DB::Iceberg
{

String computePartitionId(const Row & partition_key_value);

struct IcebergObjectSerializableInfo
{
    /// Preserve the manifest spelling for `_path` and task identity; it may differ from the resolved storage key.
    IcebergPathFromMetadata data_object_file_path_key;
    Int32 underlying_format_read_schema_id{};
    Int32 schema_id_relevant_to_iterator{};
    Int64 sequence_number{};
    String file_format;
    String manifest_file;
    String partition_id;
    std::vector<Iceberg::PositionDeleteObject> position_deletes_objects;
    /// A deletion vector replaces all position delete files, so when present `position_deletes_objects` is empty.
    std::optional<Iceberg::DeletionVectorObject> deletion_vector;
    std::vector<Iceberg::EqualityDeleteObject> equality_deletes_objects;
    std::optional<Int64> record_count;
    std::optional<Int64> file_size_in_bytes;
    std::optional<UInt64> first_row_id;
    std::vector<std::pair<String, Field>> identity_partition_columns;

    bool requires_external_storage = false;

    bool hasPositionDeletes() const { return deletion_vector.has_value() || !position_deletes_objects.empty(); }

    void serializeForClusterFunctionProtocol(WriteBuffer & out, size_t protocol_version) const;
    void deserializeForClusterFunctionProtocol(ReadBuffer & in, size_t protocol_version);

private:
    void checkVersion(size_t protocol_version) const;
};

}

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ExternalPathResolver.h>
#include <base/defines.h>

namespace DB
{

class ISimpleTransform;

struct FormatParserSharedResources;
using FormatParserSharedResourcesPtr = std::shared_ptr<FormatParserSharedResources>;

struct IcebergDataObjectInfo : public ObjectInfo, std::enable_shared_from_this<IcebergDataObjectInfo>
{
    using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

    /// Position deletes match the data path exactly as written in the manifest.
    explicit IcebergDataObjectInfo(
        Iceberg::ProcessedManifestFileEntryPtr data_manifest_file_entry_,
        const String & metadata_path_,
        Int32 schema_id_relevant_to_iterator_,
        std::vector<std::pair<String, Field>> identity_partition_columns_,
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
        std::shared_ptr<ExternalStorageCache> external_storages);

    std::optional<String> getFileFormat() const override { return info.file_format; }

    std::optional<size_t> getFileSizeHint() const override
    {
        if (info.file_size_in_bytes.has_value())
            return static_cast<size_t>(*info.file_size_in_bytes);
        return std::nullopt;
    }

    /// Attach a V2 position delete file (Parquet).
    void addPositionDeleteFile(const Iceberg::ProcessedManifestFileEntryPtr & position_delete_file, const String & resolved_storage_path);

    /// Attach a V3 deletion vector (a blob inside a Puffin file).
    void addDeletionVector(const Iceberg::ProcessedManifestFileEntryPtr & deletion_vector, const String & resolved_storage_path);

    std::optional<String> getPathInDataLakeMetadata() const override
    {
        if (info.data_object_file_path_key.empty())
            return std::nullopt;
        return info.data_object_file_path_key.serialize();
    }

    std::optional<String> getExternalLocalPath() const override;

    std::shared_ptr<ObjectInfo> clone() const override { return std::make_shared<IcebergDataObjectInfo>(*this); }

    ObjectStoragePtr getResolvedStorage(const ObjectStoragePtr & default_storage) const override
    {
        return resolved_storage ? resolved_storage : default_storage;
    }

    ObjectStoragePtr tryGetResolvedStorage() const { return resolved_storage; }

    void setResolvedStorage(ObjectStoragePtr storage) { resolved_storage = std::move(storage); }

    void addEqualityDeleteObject(const Iceberg::ProcessedManifestFileEntryPtr & equality_delete_object, const String & resolved_storage_path);
    Iceberg::IcebergObjectSerializableInfo info;

private:
    ObjectStoragePtr resolved_storage;
};

using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;
}

#endif
