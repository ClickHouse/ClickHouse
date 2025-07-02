


#pragma once
#include "config.h"

#if USE_AVRO


#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include "Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h"

namespace DB {

class IcebergMetadata;

struct IcebergDataObjectInfo : public RelativePathWithMetadata
{
    explicit IcebergDataObjectInfo(
        const IcebergMetadata & iceberg_metadata,
        Iceberg::ManifestFileEntry data_object_,
        std::optional<ObjectMetadata> metadata_ = std::nullopt,
        const std::vector<Iceberg::ManifestFileEntry> & position_deletes_objects_ = {});

    const Iceberg::ManifestFileEntry data_object;
    std::span<const Iceberg::ManifestFileEntry> position_deletes_objects;

    // Return the path in the Iceberg metadata
    std::string getIcebergDataPath() const { return data_object.file_path_key; }
};
using IcebergDataObjectInfoPtr = std::shared_ptr<IcebergDataObjectInfo>;

class IcebergKeysIterator : public IObjectIterator
{
public:
    IcebergKeysIterator(
        const IcebergMetadata & iceberg_metadata_,
        std::vector<Iceberg::ManifestFileEntry>&& position_deletes_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_);

    size_t estimatedKeysCount() override
    {
        return data_files.size();
    }

    ObjectInfoPtr next(size_t) override;

private:
    const IcebergMetadata & iceberg_metadata;
    std::optional<Iceberg::IcebergSnapshot> relevant_snapshot;
    ObjectStoragePtr object_storage;
    IDataLakeMetadata::FileProgressCallback callback;

    //Unfortunately, for now we need to calculate position deletes files beforehand and store their list in RAM.
    std::vector<Iceberg::ManifestFileEntry> position_deletes_files;
};

ObjectIterator IcebergMetadata::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t /* list_batch_size */,
    ContextPtr local_context) const
{
    return std::make_shared<IcebergKeysIterator>(*this, getPositionalDeleteFiles(filter_dag, local_context), object_storage, callback);
}

}

#endif
