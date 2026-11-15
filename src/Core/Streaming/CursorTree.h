#pragma once

#include <Core/Field.h>
#include <Core/Names.h>

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

/// The collapsed form of a cursor tree is a map from dotted paths of keys to values.
/// A dot and a backslash inside a key are escaped by a backslash, so that a key may contain dots (e.g. a file name).
String escapeCursorKey(const String & key);

Map cursorTreeToMap(const CursorTreeNodePtr & ptr);
CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree);

void mergeCursors(const CursorTreeNodePtr & into, const CursorTreeNodePtr & from);

/// The subtree of `cursor` at `path`, or nullptr if `cursor` is nullptr or has no such subtree.
/// Throws if `path` leads to a value instead of a subtree.
CursorTreeNodePtr getCursorSubtree(const CursorTreeNodePtr & cursor, const Strings & path);

}
