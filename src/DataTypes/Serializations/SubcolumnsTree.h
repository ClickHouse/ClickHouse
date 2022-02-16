#pragma once

#include <DataTypes/Serializations/PathInData.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <unordered_map>

namespace DB
{

struct EmptyNodeData {};

template <typename LeafData, typename NodeData = EmptyNodeData>
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

        Kind kind = TUPLE;
        const Node * parent = nullptr;

        std::map<String, std::shared_ptr<Node>, std::less<>> children;
        NodeData data;

        bool isNested() const { return kind == NESTED; }

        void addChild(const String & key, std::shared_ptr<Node> next_node)
        {
            next_node->parent = this;
            children[key] = std::move(next_node);
        }

        virtual ~Node() = default;
    };

    struct Leaf : public Node
    {
        Leaf(const PathInData & path_, const LeafData & data_)
            : Node(Node::SCALAR), path(path_), data(data_)
        {
        }

        PathInData path;
        LeafData data;
    };

    using NodeKind = typename Node::Kind;
    using NodePtr = std::shared_ptr<Node>;
    using LeafPtr = std::shared_ptr<Leaf>;

    bool add(const PathInData & path, const LeafData & leaf_data)
    {
        return add(path, [&](NodeKind kind, bool exists) -> NodePtr
        {
            if (exists)
                return nullptr;

            if (kind == Node::SCALAR)
                return std::make_shared<Leaf>(path, leaf_data);

            return std::make_shared<Node>(kind);
        });
    }

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

            auto it = current_node->children.find(parts[i].key);
            if (it != current_node->children.end())
            {
                current_node = it->second.get();
                node_creator(current_node->kind, true);
                bool current_node_is_nested = current_node->kind == Node::NESTED;

                if (current_node_is_nested != parts[i].is_nested)
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

        auto it = current_node->children.find(parts.back().key);
        if (it != current_node->children.end())
            return false;

        auto next_node = node_creator(Node::SCALAR, false);
        current_node->addChild(String(parts.back().key), next_node);

        auto leaf = std::dynamic_pointer_cast<Leaf>(next_node);
        assert(leaf);
        leaves.push_back(std::move(leaf));

        return true;
    }

    const Node * findBestMatch(const PathInData & path) const
    {
        return findImpl(path, false);
    }

    const Node * findExact(const PathInData & path) const
    {
        return findImpl(path, true);
    }

    const Leaf * findLeaf(const PathInData & path) const
    {
        return typeid_cast<const Leaf *>(findExact(path));
    }

    using LeafPredicate = std::function<bool(const Leaf &)>;

    const Leaf * findLeaf(const LeafPredicate & predicate)
    {
        return findLeaf(root.get(), predicate);
    }

    static const Leaf * findLeaf(const Node * node, const LeafPredicate & predicate)
    {
        if (const auto * leaf = typeid_cast<const Leaf *>(node))
            return predicate(*leaf) ? leaf : nullptr;

        for (const auto & [_, child] : node->children)
            if (const auto * leaf = findLeaf(child.get(), predicate))
                return leaf;

        return nullptr;
    }

    using NodePredicate = std::function<bool(const Node &)>;

    static const Node * findParent(const Node * node, const NodePredicate & predicate)
    {
        while (node && !predicate(*node))
            node = node->parent;
        return node;
    }

    bool empty() const { return root == nullptr; }
    size_t size() const { return leaves.size(); }

    using Leaves = std::vector<LeafPtr>;
    const Leaves & getLeaves() const { return leaves; }
    const Node * getRoot() const { return root.get(); }

    using iterator = typename Leaves::iterator;
    using const_iterator = typename Leaves::const_iterator;

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
            auto it = current_node->children.find(part.key);
            if (it == current_node->children.end())
                return find_exact ? nullptr : current_node;

            current_node = it->second.get();
        }

        return current_node;

    }

    NodePtr root;
    Leaves leaves;
};

}
