#pragma once

#include <Core/Field.h>

#include <memory>

namespace DB
{

class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;

Map cursorTreeToMap(const CursorTreeNodePtr & ptr);
CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree);

}
