#include <cstddef>
#include <memory>
#include <variant>
#include <vector>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Core/Streaming/ICursor.h>

namespace DB
{

static void collapseImpl(Map & collapsed_tree, std::vector<String> & path, CursorTreeNode * node)
{
    if (node->isFork())
    {
        for (const auto & branch : node->fork())
        {
            path.push_back(branch.first);
            collapseImpl(collapsed_tree, path, branch.second.get());
            path.pop_back();
        }
    }
    else
        collapsed_tree.push_back(Tuple{boost::algorithm::join(path, "."), node->leaf()});
}

CursorTreeNode::CursorTreeNode(Data data_) : data{std::move(data_)}
{
}

bool CursorTreeNode::isFork() const
{
    return std::holds_alternative<Fork>(data);
}

const CursorTreeNode::Fork & CursorTreeNode::fork() const
{
    chassert(isFork());
    return std::get<Fork>(data);
}

CursorTreeNode::Fork & CursorTreeNode::fork()
{
    chassert(isFork());
    return std::get<Fork>(data);
}

bool CursorTreeNode::isLeaf() const
{
    return std::holds_alternative<Leaf>(data);
}

const CursorTreeNode::Leaf & CursorTreeNode::leaf() const
{
    chassert(isLeaf());
    return std::get<Leaf>(data);
}

CursorTreeNode::Leaf & CursorTreeNode::leaf()
{
    chassert(isLeaf());
    return std::get<Leaf>(data);
}

///

CursorTree::CursorTree(CursorTreeNodePtr root_) : root{std::move(root_)}
{
    chassert(root->isFork());
}

/// example of collapsed_tree
///
/// {
///     "shard_1.part_1.block_number": 10,
///     "shard_1.part_1.block_offset": 42,
/// }
CursorTree CursorTree::fromMap(Map collapsed_tree)
{
    CursorTree tree(std::make_shared<CursorTreeNode>());

    for (const auto & leaf : collapsed_tree)
    {
        const auto & tuple = leaf.safeGet<const Tuple &>();
        const auto & dotted_path = tuple.at(0).safeGet<String>();
        const auto & value = tuple.at(1).get<UInt64>();

        std::vector<String> path;
        boost::split(path, dotted_path, boost::is_any_of("."));
        tree.updateTree(path, value);
    }

    return tree;
}


Map CursorTree::collapse() const
{
    Map collapsed_tree;
    std::vector<String> path;

    collapseImpl(collapsed_tree, path, root.get());

    return collapsed_tree;
}

UInt64 CursorTree::getValue(const std::vector<String> & path) const
{
    auto [parent, depth] = getNearestParent(path);
    chassert(depth + 1 == path.size());

    return parent->fork().at(path.back())->leaf();
}

CursorTree CursorTree::getSubtree(const std::vector<String> & path) const
{
    auto [parent, depth] = getNearestParent(path);
    chassert(depth + 1 == path.size());

    return CursorTree(parent->fork().at(path.back()));
}

void CursorTree::updateTree(const std::vector<String> & path, UInt64 value)
{
    CursorTreeNode * parent = retrieveParent(path);

    auto & cursor_fork = parent->fork();
    auto it = cursor_fork.find(path.back());

    if (it == cursor_fork.end())
        cursor_fork.emplace(path.back(), std::make_shared<CursorTreeNode>(value));
    else
        it->second->leaf() = value;
}

void CursorTree::updateTree(const std::vector<String> & path, CursorTree subtree)
{
    CursorTreeNode * parent = retrieveParent(path);

    auto & cursor_fork = parent->fork();
    auto it = cursor_fork.find(path.back());

    if (it == cursor_fork.end())
        cursor_fork.emplace(path.back(), std::move(subtree.root));
    else
        it->second = std::move(subtree.root);
}

std::pair<CursorTreeNode *, UInt8> CursorTree::getNearestParent(const std::vector<String> & path) const
{
    CursorTreeNode * cur_node = root.get();
    UInt8 cur_depth = 0;

    while (cur_depth + 1 < path.size())
    {
        const auto & fork = cur_node->fork();

        if (!fork.contains(path[cur_depth]))
            break;

        cur_node = fork.at(path[cur_depth]).get();
        cur_depth += 1;
    }

    return {cur_node, cur_depth};
}

CursorTreeNode * CursorTree::retrieveParent(const std::vector<String> & path)
{
    auto [parent, depth] = getNearestParent(path);

    for (size_t i = depth; i + 1 < path.size(); ++i)
    {
        auto [it, _] = parent->fork().emplace(path[i], std::make_shared<CursorTreeNode>());
        parent = it->second.get();
    }

    return parent;
}

///

ICursor::ICursor(CursorTree tree_) : tree{std::move(tree_)}
{
}


}
