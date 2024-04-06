#pragma once

#include <memory>
#include <variant>
#include <vector>

#include <Core/Field.h>
#include <base/types.h>

namespace DB
{

class CursorTree;
class CursorTreeNode;
using CursorTreeNodePtr = std::shared_ptr<CursorTreeNode>;

/// TODO
class CursorTreeNode
{
    using Leaf = UInt64;
    using Fork = std::map<String, CursorTreeNodePtr>;
    using Data = std::variant<Fork, Leaf>;

public:
    explicit CursorTreeNode(Data data_ = Fork{});

    bool isFork() const;
    const Fork & fork() const;
    Fork & fork();

    bool isLeaf() const;
    const Leaf & leaf() const;
    Leaf & leaf();

private:
    Data data;
};

/// TODO
class CursorTree
{
public:
    static CursorTree fromMap(Map collapsed_tree_);
    Map collapse() const;

    UInt64 getValue(const std::vector<String> & path) const;
    CursorTree getSubtree(const std::vector<String> & path) const;

    void updateTree(const std::vector<String> & path, UInt64 value);
    void updateTree(const std::vector<String> & path, CursorTree subtree);

private:
    explicit CursorTree(CursorTreeNodePtr root_);

    std::pair<CursorTreeNode *, UInt8> getNearestParent(const std::vector<String> & path) const;
    CursorTreeNode * retrieveParent(const std::vector<String> & path);

    CursorTreeNodePtr root;
};

/// TODO
class ICursor
{
public:
    explicit ICursor(CursorTree tree_);
    virtual ~ICursor() = default;

protected:
    CursorTree tree;
};

}
