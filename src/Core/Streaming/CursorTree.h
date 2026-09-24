#pragma once

#include <Core/Field.h>

#include <base/types.h>
#include <Common/MapWithMemoryTracking.h>

#include <memory>
#include <variant>

namespace DB
{

class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;

/// Single node of cursor tree, which represents logical entry of cursor.
/// Example: partition/shard etc.
/// Every walk of the tree recurses once per level, and so does its implicit destructor: `data` owns
/// the children through `shared_ptr`, so releasing the root unwinds the whole chain.
class CursorTreeNode
{
    using Data = MapWithMemoryTracking<String, std::variant<Int64, CursorTreeNodePtr>>;

public:
    bool hasSubtree(const String & key) const;
    const CursorTreeNodePtr & getSubtree(const String & key) const;
    CursorTreeNodePtr & setSubtree(const String & key, CursorTreeNodePtr tree);
    CursorTreeNodePtr & getSubtreeOrCreate(const String & key);

    bool hasValue(const String & key) const;
    Int64 getValue(const String & key) const;
    Int64 getValue(const String & key, Int64 default_value) const;
    Int64 & setValue(const String & key, Int64 value);

    CursorTreeNodePtr clone() const;

    Data::iterator begin();
    Data::iterator end();

    Data::const_iterator begin() const;
    Data::const_iterator end() const;

private:
    Data data;
};

/// Maximum number of levels of a cursor tree, i.e. of dot-separated components in one cursor key.
/// Deliberately not derived from `max_parser_depth`: cursors are also read back from ZooKeeper and
/// from object storage, where there is no settings context.
static constexpr size_t MAX_CURSOR_TREE_DEPTH = 1000;

Map cursorTreeToMap(const CursorTreeNodePtr & ptr);
CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree);

void mergeCursors(const CursorTreeNodePtr & into, const CursorTreeNodePtr & from);

}
