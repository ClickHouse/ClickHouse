#include <Storages/ObjectStorage/DataLakes/DataLakeRefreshCursorStore.h>

#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

#include <Core/Streaming/CursorTree.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <base/hex.h>

namespace DB
{

String serializeCursorTree(const CursorTreeNodePtr & cursor)
{
    if (!cursor)
        return {};
    WriteBufferFromOwnString buf;
    writeFieldBinary(Field(cursorTreeToMap(cursor)), buf);
    return buf.str();
}

CursorTreeNodePtr deserializeCursorTree(const String & serialized)
{
    if (serialized.empty())
        return nullptr;
    ReadBufferFromString buf(serialized);
    return buildCursorTree(readFieldBinary(buf).safeGet<Map>());
}

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

CursorTreeNodePtr DataLakeRefreshCursorStore::load(ContextPtr context)
{
    auto metadata = storage->getExternalMetadata(context);
    if (!metadata)
        return nullptr;
    auto stored = metadata->getRefreshCursor(context);
    if (!stored || stored->empty())
        return nullptr;
    return deserializeCursorTree(refreshCursorFromStorage(*stored));
}

}
