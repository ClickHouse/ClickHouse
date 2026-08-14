#include <Storages/ObjectStorage/DataLakes/DataLakeRefreshCursorStore.h>

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <base/hex.h>

namespace DB
{

String refreshCursorToStorage(const String & serialized_cursor)
{
    return hexString(serialized_cursor.data(), serialized_cursor.size());
}

String refreshCursorFromStorage(const String & stored)
{
    String out;
    out.reserve(stored.size() / 2);
    for (size_t i = 0; i + 1 < stored.size(); i += 2)
        out.push_back(static_cast<char>(unhex2(stored.data() + i)));
    return out;
}

DataLakeRefreshCursorStore::DataLakeRefreshCursorStore(std::shared_ptr<StorageObjectStorage> storage_)
    : storage(std::move(storage_))
{
}

String DataLakeRefreshCursorStore::load(ContextPtr context)
{
    auto * metadata = storage->getExternalMetadata(context);
    if (!metadata)
        return {};
    auto stored = metadata->getRefreshCursor(context);
    if (!stored || stored->empty())
        return {};
    return refreshCursorFromStorage(*stored);
}

}
