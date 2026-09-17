#pragma once

#include <base/types.h>

#include <memory>

namespace DB
{

class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;

/// Encode/decode the incremental refreshable-MV cursor tree to/from a base64 text form safe to store in a catalog (an Iceberg snapshot summary value is a JSON string).
String refreshCursorToStorage(const CursorTreeNodePtr & cursor);
CursorTreeNodePtr refreshCursorFromStorage(const String & stored);

}
