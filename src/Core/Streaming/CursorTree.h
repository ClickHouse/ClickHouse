#pragma once

#include <Core/Field.h>
#include <Core/Streaming/CursorTree_fwd.h>

#include <base/types.h>

#include <variant>

namespace DB
{

/// Single node of cursor tree, which represents logical entry of cursor.
/// Example: partition/shard etc.
class CursorTreeNode
{
    using Data = std::map<String, std::variant<Int64, CursorTreeNodePtr>>;

public:
    bool hasSubtree(const String & key) const;
    const CursorTreeNodePtr & getSubtree(const String & key) const;
    CursorTreeNodePtr & setSubtree(const String & key, CursorTreeNodePtr tree);
    CursorTreeNodePtr & next(const String & key);

    bool hasValue(const String & key) const;
    Int64 getValue(const String & key) const;
    Int64 getValue(const String & key, Int64 default_value) const;
    Int64 & setValue(const String & key, Int64 value);

    Data::iterator begin();
    Data::iterator end();

    Data::const_iterator begin() const;
    Data::const_iterator end() const;

private:
    Data data;
};

Map cursorTreeToMap(const CursorTreeNodePtr & ptr);
CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree);

}
