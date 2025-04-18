#include "IDataLakeMetadata.h"
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
        IDataLakeMetadata::FileProgressCallback callback_)
        : data_files(data_files_)
        , object_storage(object_storage_)
        , callback(callback_)
    {
    }

    size_t estimatedKeysCount() override
    {
        return data_files.size();
    }

    ObjectInfoPtr next(size_t) override
    {
        while (true)
        {
            size_t current_index = index.fetch_add(1, std::memory_order_relaxed);
            if (current_index >= data_files.size())
                return nullptr;

            auto key = data_files[current_index];

            if (callback)
            {
                /// Too expencive to load size for metadata always
                /// because it requires API call to external storage.
                /// In many cases only keys are needed.
                callback(FileProgress(0, 1));
            }

            return std::make_shared<ObjectInfo>(key, std::nullopt);
        }
    }

private:
    Strings data_files;
    ObjectStoragePtr object_storage;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;
};

}

ObjectIterator IDataLakeMetadata::createKeysIterator(
    Strings && data_files_,
    ObjectStoragePtr object_storage_,
    IDataLakeMetadata::FileProgressCallback callback_) const
{
    return std::make_shared<KeysIterator>(std::move(data_files_), object_storage_, callback_);
}

DB::ReadFromFormatInfo IDataLakeMetadata::prepareReadingFromFormat(
    const Strings & requested_columns,
    const DB::StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    bool supports_subset_of_columns)
{
    return DB::prepareReadingFromFormat(requested_columns, storage_snapshot, context, supports_subset_of_columns);
}

}
