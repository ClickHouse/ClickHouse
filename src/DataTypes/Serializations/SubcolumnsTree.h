#pragma once

#include <DataTypes/Serializations/PathInData.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/HashMap.h>

namespace DB
{

/// Tree that represents paths in document
/// with additional data in nodes.
template <typename NodeData>
class SubcolumnsTree
{
public:
    struct Node
    {
        enum Kind
        {
            TUPLE,
            NESTED,
            SCALAR,
        };

        explicit Node(Kind kind_) : kind(kind_) {}
        Node(Kind kind_, const NodeData & data_) : kind(kind_), data(data_) {}
        Node(Kind kind_, const NodeData & data_, const PathInData & path_)
            : kind(kind_), data(data_), path(path_) {}

        Kind kind = TUPLE;
        const Node * parent = nullptr;

        Arena strings_pool;
        HashMapWithStackMemory<StringRef, std::shared_ptr<Node>, StringRefHash, 4> children;

        NodeData data;
        PathInData path;

        bool isNested() const { return kind == NESTED; }
        bool isScalar() const { return kind == SCALAR; }

        void addChild(std::string_view key, std::shared_ptr<Node> next_node)
        {
            next_node->parent = this;
            StringRef key_ref{strings_pool.insert(key.data(), key.length()), key.length()};
            children[key_ref] = std::move(next_node);
        }
    };

    using NodeKind = typename Node::Kind;
    using NodePtr = std::shared_ptr<Node>;

    /// Add a leaf without any data in other nodes.
    bool add(const PathInData & path, const NodeData & leaf_data)
    {
        return add(path, [&](NodeKind kind, bool exists) -> NodePtr
        {
            if (exists)
                return nullptr;

            if (kind == Node::SCALAR)
                return std::make_shared<Node>(kind, leaf_data, path);

            return std::make_shared<Node>(kind);
        });
    }

    /// Callback for creation of node. Receives kind of node and
    /// flag, which is true if node already exists.
    using NodeCreator = std::function<NodePtr(NodeKind, bool)>;

    bool add(const PathInData & path, const NodeCreator & node_creator)
    {
        const auto & parts = path.getParts();

        if (parts.empty())
            return false;

        if (!root)
            root = std::make_shared<Node>(Node::TUPLE);

        Node * current_node = root.get();
        for (size_t i = 0; i < parts.size() - 1; ++i)
        {
            assert(current_node->kind != Node::SCALAR);

            auto it = current_node->children.find(StringRef{parts[i].key});
            if (it != current_node->children.end())
            {
                current_node = it->getMapped().get();
                node_creator(current_node->kind, true);

                if (current_node->isNested() != parts[i].is_nested)
                    return false;
            }
            else
            {
                auto next_kind = parts[i].is_nested ? Node::NESTED : Node::TUPLE;
                auto next_node = node_creator(next_kind, false);
                current_node->addChild(String(parts[i].key), next_node);
                current_node = next_node.get();
            }
        }

        auto it = current_node->children.find(StringRef{parts.back().key});
        if (it != current_node->children.end())
            return false;

        auto next_node = node_creator(Node::SCALAR, false);
        current_node->addChild(String(parts.back().key), next_node);
        leaves.push_back(std::move(next_node));

        return true;
    }

    /// Find node that matches the path the best.
    const Node * findBestMatch(const PathInData & path) const
    {
        return findImpl(path, false);
    }

    /// Find node that matches the path exactly.
    const Node * findExact(const PathInData & path) const
    {
        return findImpl(path, true);
    }

    /// Find leaf by path.
    const Node * findLeaf(const PathInData & path) const
    {
        const auto * candidate = findExact(path);
        if (!candidate || !candidate->isScalar())
            return nullptr;
        return candidate;
    }

    using NodePredicate = std::function<bool(const Node &)>;

    /// Finds leaf that satisfies the predicate.
    const Node * findLeaf(const NodePredicate & predicate)
    {
        return findLeaf(root.get(), predicate);
    }

    static const Node * findLeaf(const Node * node, const NodePredicate & predicate)
    {
        if (!node)
            return nullptr;

        if (node->isScalar())
            return predicate(*node) ? node : nullptr;

        for (const auto & [_, child] : node->children)
            if (const auto * leaf = findLeaf(child.get(), predicate))
                return leaf;

        return nullptr;
    }

    /// Find first parent node that satisfies the predicate.
    static const Node * findParent(const Node * node, const NodePredicate & predicate)
    {
        while (node && !predicate(*node))
            node = node->parent;
        return node;
    }

    bool empty() const { return root == nullptr; }
    size_t size() const { return leaves.size(); }

    using Nodes = std::vector<NodePtr>;

    const Nodes & getLeaves() const { return leaves; }
    const Node * getRoot() const { return root.get(); }

    using iterator = typename Nodes::iterator;
    using const_iterator = typename Nodes::const_iterator;

    iterator begin() { return leaves.begin(); }
    iterator end() { return leaves.end(); }

    const_iterator begin() const { return leaves.begin(); }
    const_iterator end() const { return leaves.end(); }

private:
    const Node * findImpl(const PathInData & path, bool find_exact) const
    {
        if (!root)
            return nullptr;

        const auto & parts = path.getParts();
        const Node * current_node = root.get();

        for (const auto & part : parts)
        {
            auto it = current_node->children.find(StringRef{part.key});
            if (it == current_node->children.end())
                return find_exact ? nullptr : current_node;

            current_node = it->getMapped().get();
        }

        return current_node;
    }

    NodePtr root;
    Nodes leaves;
};

}
