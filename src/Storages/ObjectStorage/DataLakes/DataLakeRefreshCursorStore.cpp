#include <Storages/ObjectStorage/DataLakes/DataLakeRefreshCursorStore.h>

#include <Core/Streaming/CursorTree.h>
#include <Core/Field.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Base64.h>

namespace DB
{

String refreshCursorToStorage(const CursorTreeNodePtr & cursor)
{
    if (!cursor)
        return {};
    WriteBufferFromOwnString buf;
    writeFieldBinary(Field(cursorTreeToMap(cursor)), buf);
    return base64Encode(buf.str());
}

CursorTreeNodePtr refreshCursorFromStorage(const String & stored)
{
    if (stored.empty())
        return nullptr;
    String decoded = base64Decode(stored);
    ReadBufferFromString buf(decoded);
    return buildCursorTree(readFieldBinary(buf).safeGet<Map>());
}

}
