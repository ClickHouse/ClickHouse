#pragma once

#include <memory>
#include <variant>

#include <Poco/JSON/Object.h>

#include <base/types.h>

#include <Core/Field.h>
#include <Core/Streaming/CursorTree_fwd.h>

namespace DB
{

class Context;
using ContextPtr = std::shared_ptr<const Context>;

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
    const Int64 & getValue(const String & key) const;
    Int64 & setValue(const String & key, Int64 value);

    Data::iterator begin();
    Data::iterator end();

    Data::const_iterator begin() const;
    Data::const_iterator end() const;

private:
    Data data;
};

Map cursorTreeToMap(const CursorTreeNodePtr & ptr);
String cursorTreeToString(const CursorTreeNodePtr & ptr);
Poco::JSON::Object cursorTreeToJson(const CursorTreeNodePtr & ptr);

CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree);
CursorTreeNodePtr buildCursorTree(const String & serialized_tree);
CursorTreeNodePtr buildCursorTree(const Poco::JSON::Object::Ptr & json);
CursorTreeNodePtr buildCursorTree(const ContextPtr & context, const std::optional<String> & keeper_key, const std::optional<Map> & collapsed_tree);

void mergeCursors(CursorTreeNodePtr into, CursorTreeNodePtr from);

}
