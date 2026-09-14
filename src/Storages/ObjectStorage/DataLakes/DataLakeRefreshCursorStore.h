#pragma once

#include <base/types.h>

#include <memory>

namespace DB
{

class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;

/// Serialize/deserialize the cursor tree to the opaque binary form used by the Keeper coordination znode.
String serializeCursorTree(const CursorTreeNodePtr & cursor);
CursorTreeNodePtr deserializeCursorTree(const String & serialized);

/// Encode/decode that binary form to a text form safe to store in a JSON string field (an Iceberg
/// snapshot summary value); `to` for the write path, `from` for the read. Hex keeps the round-trip byte-exact.
String refreshCursorToStorage(const String & serialized_cursor);
String refreshCursorFromStorage(const String & stored);

}
