#include <Disks/DiskObjectStorage/MetadataStorages/Memory/InMemoryDirectoryTree.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int LOGICAL_ERROR;
}

InMemoryDirectoryTree::NodePtr InMemoryDirectoryTree::resolve(const NormalizedPath & path) const
{
    NodePtr node = root;
    for (const auto & component : path)
    {
        if (node->isFile())
            return nullptr;
        auto it = node->children.find(component.string());
        if (it == node->children.end())
            return nullptr;
        node = it->second;
    }
    return node;
}

std::pair<InMemoryDirectoryTree::Node *, std::string> InMemoryDirectoryTree::resolveParent(const NormalizedPath & path) const
{
    if (path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The root directory has no parent");

    auto parent = resolve(path.parent_path());
    if (!parent || parent->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST,
            "Cannot resolve `{}`: `{}` is not an existing directory", path.string(), path.parent_path().string());

    return {parent.get(), path.filename().string()};
}

bool InMemoryDirectoryTree::existsFile(const NormalizedPath & path) const
{
    auto node = resolve(path);
    return node && node->isFile();
}

bool InMemoryDirectoryTree::existsDirectory(const NormalizedPath & path) const
{
    auto node = resolve(path);
    return node && !node->isFile();
}

bool InMemoryDirectoryTree::existsFileOrDirectory(const NormalizedPath & path) const
{
    return resolve(path) != nullptr;
}

DiskObjectStorageMetadata * InMemoryDirectoryTree::getMetadata(const NormalizedPath & path)
{
    auto node = resolve(path);
    return node && node->isFile() ? &*node->metadata : nullptr;
}

const DiskObjectStorageMetadata * InMemoryDirectoryTree::getMetadata(const NormalizedPath & path) const
{
    auto node = resolve(path);
    return node && node->isFile() ? &*node->metadata : nullptr;
}

std::vector<std::string> InMemoryDirectoryTree::listDirectory(const NormalizedPath & path) const
{
    auto node = resolve(path);
    if (!node || node->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", path.string());

    std::vector<std::string> result;
    result.reserve(node->children.size());
    for (const auto & [name, child] : node->children)
        result.push_back((path / name).string());
    return result;
}

std::optional<DiskObjectStorageMetadata> InMemoryDirectoryTree::putFile(const NormalizedPath & path, DiskObjectStorageMetadata metadata)
{
    auto [parent, leaf] = resolveParent(path);

    std::optional<DiskObjectStorageMetadata> displaced;
    if (auto it = parent->children.find(leaf); it != parent->children.end())
    {
        if (!it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Cannot create file `{}`: a directory with this name exists", path.string());
        displaced.emplace(std::move(*it->second->metadata));
        parent->children.erase(it);
    }

    auto node = std::make_shared<Node>();
    node->metadata.emplace(std::move(metadata));
    parent->children.emplace(std::move(leaf), std::move(node));
    return displaced;
}

DiskObjectStorageMetadata InMemoryDirectoryTree::removeFile(const NormalizedPath & path)
{
    auto [parent, leaf] = resolveParent(path);

    auto it = parent->children.find(leaf);
    if (it == parent->children.end() || !it->second->isFile())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path.string());

    DiskObjectStorageMetadata metadata = std::move(*it->second->metadata);
    parent->children.erase(it);
    return metadata;
}

void InMemoryDirectoryTree::createDirectory(const NormalizedPath & path)
{
    auto [parent, leaf] = resolveParent(path);

    if (auto it = parent->children.find(leaf); it != parent->children.end())
    {
        if (it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Cannot create directory `{}`: a file with this name exists", path.string());
        return;
    }

    parent->children.emplace(std::move(leaf), std::make_shared<Node>());
}

void InMemoryDirectoryTree::createDirectoryRecursive(const NormalizedPath & path)
{
    Node * node = root.get();
    for (const auto & component : path)
    {
        auto it = node->children.find(component.string());
        if (it == node->children.end())
            it = node->children.emplace(component.string(), std::make_shared<Node>()).first;
        else if (it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS,
                "Cannot create directory `{}`: `{}` is a file", path.string(), component.string());
        node = it->second.get();
    }
}

void InMemoryDirectoryTree::removeDirectory(const NormalizedPath & path)
{
    auto [parent, leaf] = resolveParent(path);

    auto it = parent->children.find(leaf);
    if (it == parent->children.end() || it->second->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", path.string());

    if (!it->second->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot remove directory `{}`: `{}` remains under it", path.string(), it->second->children.begin()->first);

    parent->children.erase(it);
}

void InMemoryDirectoryTree::removeSubtree(const NormalizedPath & path, const std::function<void(const std::string &, DiskObjectStorageMetadata &)> & visitor)
{
    auto node = resolve(path);
    if (!node)
        return;

    std::function<void(Node &, const std::filesystem::path &)> visit = [&](Node & current, const std::filesystem::path & relative_path)
    {
        if (current.isFile())
        {
            visitor(relative_path.empty() ? "." : relative_path.string(), *current.metadata);
            return;
        }
        for (auto & [name, child] : current.children)
            visit(*child, relative_path / name);
    };
    visit(*node, {});

    if (path.empty())
    {
        root->children.clear();
        return;
    }
    auto [parent, leaf] = resolveParent(path);
    parent->children.erase(leaf);
}

std::optional<DiskObjectStorageMetadata> InMemoryDirectoryTree::moveFile(const NormalizedPath & from, const NormalizedPath & to, bool replace)
{
    auto [from_parent, from_leaf] = resolveParent(from);
    auto from_it = from_parent->children.find(from_leaf);
    if (from_it == from_parent->children.end() || !from_it->second->isFile())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", from.string());

    auto [to_parent, to_leaf] = resolveParent(to);

    std::optional<DiskObjectStorageMetadata> displaced;
    if (auto to_it = to_parent->children.find(to_leaf); to_it != to_parent->children.end())
    {
        if (!replace || !to_it->second->isFile())
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File `{}` already exists", to.string());
        displaced.emplace(std::move(*to_it->second->metadata));
        to_parent->children.erase(to_it);
    }

    /// `from_it` stays valid: resolving `to` does not mutate the tree.
    auto node = std::move(from_it->second);
    from_parent->children.erase(from_it);
    to_parent->children.emplace(std::move(to_leaf), std::move(node));
    return displaced;
}

void InMemoryDirectoryTree::moveDirectory(const NormalizedPath & from, const NormalizedPath & to)
{
    auto from_node = resolve(from);
    if (!from_node || from_node->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", from.string());

    /// A destination inside the moved directory would create a cycle.
    {
        NodePtr node = root;
        for (const auto & component : to)
        {
            if (node == from_node)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot move directory `{}` under itself (`{}`)", from.string(), to.string());
            if (node->isFile())
                break;
            auto it = node->children.find(component.string());
            if (it == node->children.end())
                break;
            node = it->second;
        }
    }

    auto [to_parent, to_leaf] = resolveParent(to);
    if (to_parent->children.contains(to_leaf))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "File or directory `{}` already exists", to.string());

    auto [from_parent, from_leaf] = resolveParent(from);
    auto node = std::move(from_parent->children.at(from_leaf));
    from_parent->children.erase(from_leaf);
    to_parent->children.emplace(std::move(to_leaf), std::move(node));
}

void InMemoryDirectoryTree::forEachMetadataUnder(const NormalizedPath & path, const std::function<void(const std::string &, DiskObjectStorageMetadata &)> & visitor) const
{
    auto node = resolve(path);
    if (!node || node->isFile())
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", path.string());

    std::function<void(Node &, const std::filesystem::path &)> visit = [&](Node & current, const std::filesystem::path & full_path)
    {
        if (current.isFile())
        {
            visitor(full_path.string(), *current.metadata);
            return;
        }
        for (auto & [name, child] : current.children)
            visit(*child, full_path / name);
    };
    visit(*node, path);
}

}
