#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace
{

class KeysIterator : public IObjectIterator
{
public:
    KeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_,
        std::optional<UInt64> snapshot_version_ = std::nullopt,
        std::vector<std::shared_ptr<ActionsDAG>> && schema_transforms_ = {})
        : data_files(data_files_)
        , schema_transforms(std::move(schema_transforms_))
        , object_storage(object_storage_)
        , callback(callback_)
        , snapshot_version(snapshot_version_)
    {
        chassert(schema_transforms.empty() || schema_transforms.size() == data_files.size());
    }

    size_t estimatedKeysCount() override
    {
        return data_files.size();
    }

    std::optional<UInt64> getSnapshotVersion() const override
    {
        return snapshot_version;
    }

    ObjectInfoPtr next(size_t) override
    {
        while (true)
        {
            size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
            if (current_index >= data_files.size())
                return nullptr;

            auto key = data_files[current_index];
            auto object_metadata = getObjectMetadataForRead(*object_storage, RelativePathWithMetadata(key), /*with_tags=*/ false);

            if (callback)
                callback(FileProgress(0, object_metadata.size_bytes));

            auto object_info = std::make_shared<ObjectInfo>(RelativePathWithMetadata{key, std::move(object_metadata)});
            if (!schema_transforms.empty() && schema_transforms[current_index])
                object_info->data_lake_metadata.emplace().schema_transform = schema_transforms[current_index];
            return object_info;
        }
    }

private:
    Strings data_files;
    std::vector<std::shared_ptr<ActionsDAG>> schema_transforms;
    ObjectStoragePtr object_storage;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;
    std::optional<UInt64> snapshot_version;
};

}

ObjectIterator IDataLakeMetadata::createKeysIterator(
    Strings && data_files_,
    ObjectStoragePtr object_storage_,
    IDataLakeMetadata::FileProgressCallback callback_) const
{
    return std::make_shared<KeysIterator>(std::move(data_files_), object_storage_, callback_);
}

ObjectIterator IDataLakeMetadata::createKeysIterator(
    Strings && data_files_,
    ObjectStoragePtr object_storage_,
    IDataLakeMetadata::FileProgressCallback callback_,
    UInt64 snapshot_version_) const
{
    return std::make_shared<KeysIterator>(std::move(data_files_), object_storage_, callback_, snapshot_version_);
}

ObjectIterator IDataLakeMetadata::createKeysIterator(
    Strings && data_files_,
    std::vector<std::shared_ptr<ActionsDAG>> && schema_transforms_,
    ObjectStoragePtr object_storage_,
    IDataLakeMetadata::FileProgressCallback callback_) const
{
    return std::make_shared<KeysIterator>(
        std::move(data_files_), object_storage_, callback_, std::nullopt, std::move(schema_transforms_));
}

void IDataLakeMetadata::checkInsertIsPossible(ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Writes are not supported for engine");
}

ReadFromFormatInfo IDataLakeMetadata::prepareReadingFromFormat(
    const Strings & requested_columns,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns,
    bool supports_tuple_elements)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns, supports_tuple_elements);
}

}
