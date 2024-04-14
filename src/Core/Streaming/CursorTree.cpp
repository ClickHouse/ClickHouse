#include <vector>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Core/Streaming/CursorTree.h>


namespace DB
{

static void collapseTreeImpl(std::map<String, Int64> & collapsed_tree, std::vector<String> & path, CursorTreeNode * node)
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

static std::map<String, Int64> collapseTree(CursorTreeNode * node)
{
    std::map<String, Int64> collapsed_tree;
    std::vector<String> path;

    collapseTreeImpl(collapsed_tree, path, node);

    return collapsed_tree;
}

const CursorTreeNodePtr & CursorTreeNode::getSubtree(const String & key) const
{
    auto it = data.find(key);
    chassert(it != data.end());
    return std::get<CursorTreeNodePtr>(it->second);
}

CursorTreeNodePtr & CursorTreeNode::setSubtree(const String & key, CursorTreeNodePtr tree)
{
    auto & cell = data[key] = std::move(tree);
    return std::get<CursorTreeNodePtr>(cell);
}

CursorTreeNodePtr & CursorTreeNode::next(const String & key)
{
    auto it = data.find(key);

    if (it == data.end())
        return setSubtree(key, std::make_shared<CursorTreeNode>());

    return std::get<CursorTreeNodePtr>(it->second);
}

const Int64 & CursorTreeNode::getValue(const String & key) const
{
    auto it = data.find(key);
    chassert(it != data.end());
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
    std::map<String, Int64> collapsed_tree = collapseTree(ptr.get());
    Map result;

    for (const auto & [k, v] : collapsed_tree)
        result.push_back(Tuple{k, v});

    return result;
}

String cursorTreeToString(const CursorTreeNodePtr & ptr)
{
    std::map<String, Int64> collapsed_tree = collapseTree(ptr.get());
    Poco::JSON::Object json;

    for (const auto & [k, v] : collapsed_tree)
        json.set(k, v);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

    return oss.str();
}

CursorTreeNodePtr buildCursorTree(const Map & collapsed_tree)
{
    CursorTreeNodePtr root = std::make_shared<CursorTreeNode>();

    for (const auto & leaf : collapsed_tree)
    {
        const auto & tuple = leaf.safeGet<const Tuple &>();
        const auto & dotted_path = tuple.at(0).safeGet<String>();
        const auto & value = tuple.at(1).get<Int64>();

        std::vector<String> path;
        boost::split(path, dotted_path, boost::is_any_of("."));

        CursorTreeNode * node = root.get();
        for (size_t i = 0; i + 1 < path.size(); ++i)
            node = node->next(path[i]).get();

        node->setValue(path.back(), value);
    }

    return root;
}

CursorTreeNodePtr buildCursorTree(const String & serialized_tree)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(serialized_tree).extract<Poco::JSON::Object::Ptr>();

    Map inter_repr;

    for (const auto & [k, v] : *json)
        inter_repr.push_back(Tuple{k, v.convert<Int64>()});

    return buildCursorTree(inter_repr);
}

}
