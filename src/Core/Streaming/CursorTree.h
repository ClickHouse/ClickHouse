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
class CursorTreeNode
{
    using Data = MapWithMemoryTracking<String, std::variant<Int64, CursorTreeNodePtr>>;

public:
    [[nodiscard]] bool hasSubtree(const String & key) const;
    [[nodiscard]] const CursorTreeNodePtr & getSubtree(const String & key) const;
    CursorTreeNodePtr & setSubtree(const String & key, CursorTreeNodePtr tree);
    CursorTreeNodePtr & getSubtreeOrCreate(const String & key);

    [[nodiscard]] bool hasValue(const String & key) const;
    [[nodiscard]] Int64 getValue(const String & key) const;
    [[nodiscard]] Int64 getValue(const String & key, Int64 default_value) const;
    Int64 & setValue(const String & key, Int64 value);

    [[nodiscard]] CursorTreeNodePtr clone() const;

    Data::iterator begin();
    Data::iterator end();

    [[nodiscard]] Data::const_iterator begin() const;
    [[nodiscard]] Data::const_iterator end() const;

private:
    Data data;
};

Map cursorTreeToMap(const CursorTreeNodePtr & ptr);
CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree);

void mergeCursors(const CursorTreeNodePtr & into, const CursorTreeNodePtr & from);

}
