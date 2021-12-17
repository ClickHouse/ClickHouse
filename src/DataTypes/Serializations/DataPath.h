#pragma once

#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Columns/IColumn.h>
#include <unordered_map>
#include <Common/typeid_cast.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

class Path
{
public:
    /// TODO: use dynamic bitset
    using BitSet = std::bitset<64>;

    Path() = default;
    explicit Path(std::string_view path_);

    void append(const Path & other);
    void append(std::string_view key);

    const String & getPath() const { return path; }
    bool isNested(size_t i) const { return is_nested.test(i); }
    bool hasNested() const { return is_nested.any(); }
    BitSet getIsNestedBitSet() const { return is_nested; }

    size_t getNumParts() const { return num_parts; }
    bool empty() const { return path.empty(); }

    Strings getParts() const;

    static Path getNext(const Path & current_path, const Path & key, bool make_nested = false);

    void writeBinary(WriteBuffer & out) const;
    void readBinary(ReadBuffer & in);

    bool operator==(const Path & other) const { return path == other.path; }
    bool operator!=(const Path & other) const { return !(*this == other); }
    struct Hash { size_t operator()(const Path & value) const; };

private:
    String path;
    size_t num_parts = 0;
    BitSet is_nested;
};

using Paths = std::vector<Path>;

template <typename ColumnHolder>
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

        Kind kind = TUPLE;
        const Node * parent = nullptr;
        std::unordered_map<String, std::shared_ptr<Node>> children;

        bool isNested() const { return kind == NESTED; }

        std::shared_ptr<Node> addChild(const String & key_, Kind kind_)
        {
            auto next_node = kind_ == SCALAR ? std::make_shared<Leaf>() : std::make_shared<Node>();
            next_node->kind = kind_;
            next_node->parent = this;
            children[key_] = next_node;
            return next_node;
        }

        virtual ~Node() = default;
    };

    struct Leaf : public Node
    {
        Path path;
        ColumnHolder column;
    };

    using NodePtr = std::shared_ptr<Node>;
    using LeafPtr = std::shared_ptr<Leaf>;

    bool add(const Path & path, const ColumnHolder & column)
    {
        auto parts = path.getParts();
        auto is_nested = path.getIsNestedBitSet();

        if (parts.empty())
            return false;

        if (!root)
        {
            root = std::make_shared<Node>();
            root->kind = Node::TUPLE;
        }

        Node * current_node = root.get();
        for (size_t i = 0; i < parts.size() - 1; ++i)
        {
            assert(current_node->kind != Node::SCALAR);

            auto it = current_node->children.find(parts[i]);
            if (it != current_node->children.end())
            {
                current_node = it->second.get();
                bool current_node_is_nested = current_node->kind == Node::NESTED;

                if (current_node_is_nested != is_nested.test(i))
                    return false;
            }
            else
            {
                auto next_kind = is_nested.test(i) ? Node::NESTED : Node::TUPLE;
                current_node = current_node->addChild(parts[i], next_kind).get();
            }
        }

        auto it = current_node->children.find(parts.back());
        if (it != current_node->children.end())
            return false;

        auto node = current_node->addChild(parts.back(), Node::SCALAR);
        auto leaf = std::dynamic_pointer_cast<Leaf>(node);
        assert(leaf);

        leaf->path = path;
        leaf->column = column;
        leaves.push_back(std::move(leaf));

        return true;
    }

    const Node * findBestMatch(const Path & path) const
    {
        return findImpl(path, false);
    }

    const Node * findExact(const Path & path) const
    {
        return findImpl(path, true);
    }

    const Leaf * findLeaf(const Path & path) const
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

    using iterator = typename Leaves::iterator;
    using const_iterator = typename Leaves::const_iterator;

    iterator begin() { return leaves.begin(); }
    iterator end() { return leaves.end(); }

    const_iterator begin() const { return leaves.begin(); }
    const_iterator end() const { return leaves.end(); }

private:
    const Node * findImpl(const Path & path, bool find_exact) const
    {
        if (!root)
            return nullptr;

        auto parts = path.getParts();
        const Node * current_node = root.get();

        for (const auto & part : parts)
        {
            auto it = current_node->children.find(part);
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
