#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsDirectoryEntries.h>

#include <algorithm>
#include <cstdint>
#include <utility>

namespace DB
{

struct FsDirectoryEntry
{
    std::string name;
    std::shared_ptr<FsNode> child;
    std::shared_ptr<const FsDirectoryEntry> left;
    std::shared_ptr<const FsDirectoryEntry> right;
    int64_t height;
};

namespace
{

using EntryPtr = std::shared_ptr<const FsDirectoryEntry>;
using ChildPtr = std::shared_ptr<FsNode>;

int64_t height(const EntryPtr & entry)
{
    return entry ? entry->height : 0;
}

EntryPtr make(const std::string & name, const ChildPtr & child, EntryPtr left, EntryPtr right)
{
    const int64_t new_height = 1 + std::max(height(left), height(right));
    return std::make_shared<FsDirectoryEntry>(name, child, std::move(left), std::move(right), new_height);
}

EntryPtr rotateRight(const EntryPtr & node)
{
    const auto & pivot = node->left;
    return make(pivot->name, pivot->child, pivot->left, make(node->name, node->child, pivot->right, node->right));
}

EntryPtr rotateLeft(const EntryPtr & node)
{
    const auto & pivot = node->right;
    return make(pivot->name, pivot->child, make(node->name, node->child, node->left, pivot->left), pivot->right);
}

EntryPtr balance(EntryPtr node)
{
    if (height(node->left) > height(node->right) + 1)
    {
        if (height(node->left->left) < height(node->left->right))
            node = make(node->name, node->child, rotateLeft(node->left), node->right);

        return rotateRight(node);
    }

    if (height(node->right) > height(node->left) + 1)
    {
        if (height(node->right->right) < height(node->right->left))
            node = make(node->name, node->child, node->left, rotateRight(node->right));

        return rotateLeft(node);
    }

    return node;
}

EntryPtr insert(const EntryPtr & entry, const std::string & name, const ChildPtr & child)
{
    if (!entry)
        return make(name, child, nullptr, nullptr);

    if (name < entry->name)
        return balance(make(entry->name, entry->child, insert(entry->left, name, child), entry->right));

    if (name > entry->name)
        return balance(make(entry->name, entry->child, entry->left, insert(entry->right, name, child)));

    return make(name, child, entry->left, entry->right);
}

EntryPtr remove(const EntryPtr & entry, const std::string & name)
{
    if (!entry)
        return nullptr;

    if (name < entry->name)
        return balance(make(entry->name, entry->child, remove(entry->left, name), entry->right));

    if (name > entry->name)
        return balance(make(entry->name, entry->child, entry->left, remove(entry->right, name)));

    if (!entry->left)
        return entry->right;

    if (!entry->right)
        return entry->left;

    auto successor = entry->right;
    while (successor->left)
        successor = successor->left;

    return balance(make(successor->name, successor->child, entry->left, remove(entry->right, successor->name)));
}

void visit(const EntryPtr & entry, const std::function<void(const std::string &, const ChildPtr &)> & callback)
{
    if (!entry)
        return;

    visit(entry->left, callback);
    callback(entry->name, entry->child);
    visit(entry->right, callback);
}

}

std::shared_ptr<FsNode> FsDirectoryEntries::findChild(const std::string & name) const
{
    const auto * entry = root.get();
    while (entry)
    {
        if (name == entry->name)
            return entry->child;

        if (name < entry->name)
            entry = entry->left.get();
        else
            entry = entry->right.get();
    }
    return nullptr;
}

void FsDirectoryEntries::putChild(const std::string & name, const std::shared_ptr<FsNode> & child)
{
    root = insert(root, name, child);
}

void FsDirectoryEntries::removeChild(const std::string & name)
{
    root = remove(root, name);
}

bool FsDirectoryEntries::isEmpty() const
{
    return !root;
}

void FsDirectoryEntries::forEachChild(const std::function<void(const std::string & name, const std::shared_ptr<FsNode> & child)> & callback) const
{
    visit(root, callback);
}

}
