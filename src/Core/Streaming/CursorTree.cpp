#include <Core/Streaming/CursorTree_fwd.h>
#include <Core/Streaming/CursorTree.h>

#include <Common/Exception.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>

#include <variant>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CURSOR_LOOKUP;
}

namespace
{

void collapseTreeImpl(std::map<String, Int64> & collapsed_tree, std::vector<String> & path, CursorTreeNode * node)
{
    for (const auto & [k, v] : *node)
    {
        path.push_back(k);

        if (std::holds_alternative<Int64>(v))
            collapsed_tree[boost::algorithm::join(path, ".")] = std::get<Int64>(v);
        else
            collapseTreeImpl(collapsed_tree, path, std::get<CursorTreeNodePtr>(v).get());

        path.pop_back();
    }
}

std::map<String, Int64> collapseTree(CursorTreeNode * node)
{
    std::map<String, Int64> collapsed_tree;
    std::vector<String> path;

    collapseTreeImpl(collapsed_tree, path, node);

    return collapsed_tree;
}

}

bool CursorTreeNode::hasSubtree(const String & key) const
{
    auto it = data.find(key);
    if (it == data.end())
        return false;

    return std::holds_alternative<CursorTreeNodePtr>(it->second);
}

const CursorTreeNodePtr & CursorTreeNode::getSubtree(const String & key) const
{
    auto it = data.find(key);

    if (it == data.end())
        throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP, "Trying to extract subtree by key: '{}'", key);

    if (!std::holds_alternative<CursorTreeNodePtr>(it->second))
        throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP, "Trying to extract subtree by key: '{}'", key);

    return std::get<CursorTreeNodePtr>(it->second);
}

CursorTreeNodePtr & CursorTreeNode::setSubtree(const String & key, CursorTreeNodePtr tree)
{
    data[key] = std::move(tree);
    return std::get<CursorTreeNodePtr>(data[key]);
}

CursorTreeNodePtr & CursorTreeNode::getSubtreeOrCreate(const String & key)
{
    auto it = data.find(key);

    if (it == data.end())
        return setSubtree(key, std::make_shared<CursorTreeNode>());

    if (std::holds_alternative<Int64>(it->second))
        throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP, "Trying to extract value by key: '{}'", key);

    return std::get<CursorTreeNodePtr>(it->second);
}

bool CursorTreeNode::hasValue(const String & key) const
{
    auto it = data.find(key);

    if (it == data.end())
        return false;

    return std::holds_alternative<Int64>(it->second);
}

Int64 CursorTreeNode::getValue(const String & key) const
{
    auto it = data.find(key);

    if (it == data.end())
        throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP, "Trying to extract value by key: '{}'", key);

    if (!std::holds_alternative<Int64>(it->second))
        throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP, "Trying to extract subtree by key: '{}'", key);

    return std::get<Int64>(it->second);
}

Int64 CursorTreeNode::getValue(const String & key, Int64 default_value) const
{
    auto it = data.find(key);

    if (it == data.end())
        return default_value;

    if (!std::holds_alternative<Int64>(it->second))
        throw Exception(ErrorCodes::INVALID_CURSOR_LOOKUP, "Trying to extract subtree by key: '{}'", key);

    return std::get<Int64>(it->second);
}

Int64 & CursorTreeNode::setValue(const String & key, Int64 value)
{
    auto & cell = data[key] = std::move(value);
    return std::get<Int64>(cell);
}

CursorTreeNode::Data::iterator CursorTreeNode::begin()
{
    return data.begin();
}

CursorTreeNode::Data::iterator CursorTreeNode::end()
{
    return data.end();
}

CursorTreeNode::Data::const_iterator CursorTreeNode::begin() const
{
    return data.begin();
}

CursorTreeNode::Data::const_iterator CursorTreeNode::end() const
{
    return data.end();
}

///////////////////////////////////////////////////////////////////////////////////

Map cursorTreeToMap(const CursorTreeNodePtr & ptr)
{
    chassert(ptr != nullptr);
    std::map<String, Int64> collapsed_tree = collapseTree(ptr.get());
    Map result;

    for (const auto & [k, v] : collapsed_tree)
        result.push_back(Tuple{k, v});

    return result;
}

CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree)
{
    CursorTreeNodePtr root = std::make_shared<CursorTreeNode>();

    for (const auto & leaf : collapsed_tree)
    {
        const auto & tuple = leaf.safeGet<Tuple>();
        const auto & dotted_path = tuple.at(0).safeGet<String>();
        const auto & value = tuple.at(1).safeGet<Int64>();

        std::vector<String> path;
        boost::split(path, dotted_path, boost::is_any_of("."));

        CursorTreeNode * node = root.get();
        for (size_t i = 0; i + 1 < path.size(); ++i)
            node = node->getSubtreeOrCreate(path[i]).get();

        node->setValue(path.back(), value);
    }

    return root;
}

}
