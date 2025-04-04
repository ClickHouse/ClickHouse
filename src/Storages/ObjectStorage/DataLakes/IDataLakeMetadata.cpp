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
        IDataLakeMetadata::FileProgressCallback callback_)
        : data_files(data_files_)
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
            ObjectMetadata object_metadata;
            // auto object_metadata = object_storage->getObjectMetadata(key);

            // if (callback)
            //     callback(FileProgress(0, object_metadata.size_bytes));

            return std::make_shared<ObjectInfo>(key, std::move(object_metadata));
        }
    }

private:
    Strings data_files;
    std::atomic<size_t> index = 0;
    IDataLakeMetadata::FileProgressCallback callback;
};

}

ObjectIterator IDataLakeMetadata::iterate(
    const ActionsDAG * filter_dag,
    FileProgressCallback callback,
    size_t /* list_batch_size */) const
{
    return std::make_shared<KeysIterator>(getDataFiles(filter_dag), callback);
}

Strings IDataLakeMetadata::getDataFiles(const ActionsDAG * /* filter_dag */) const
{
    throwNotImplemented("getDataFiles");
}

}
