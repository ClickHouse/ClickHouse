#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB
{

namespace
{

class KeysIterator : public IObjectIterator
{
public:
    KeysIterator(
        Strings && data_files_,
        ObjectStoragePtr object_storage_,
        IDataLakeMetadata::FileProgressCallback callback_,
        std::optional<UInt64> snapshot_version_ = std::nullopt)
        : data_files(data_files_)
        , object_storage(object_storage_)
        , callback(callback_)
        , snapshot_version(snapshot_version_)
    {
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
            auto object_metadata = object_storage->getObjectMetadata(key, /*with_tags=*/ false);

            if (callback)
                callback(FileProgress(0, object_metadata.size_bytes));

            return std::make_shared<ObjectInfo>(RelativePathWithMetadata{key, std::move(object_metadata)});
        }
    }

private:
    Strings data_files;
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
